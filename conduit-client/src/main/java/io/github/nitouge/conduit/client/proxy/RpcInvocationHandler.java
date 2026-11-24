package io.github.nitouge.conduit.client.proxy;

import io.github.nitouge.conduit.api.RpcContext;
import io.github.nitouge.conduit.api.RpcRequest;
import io.github.nitouge.conduit.api.RpcResponse;
import io.github.nitouge.conduit.api.ServiceMeta;
import io.github.nitouge.conduit.client.RpcClient;
import io.github.nitouge.conduit.cluster.Cluster;
import io.github.nitouge.conduit.cluster.ClusterOptions;
import io.github.nitouge.conduit.cluster.LoadBalancer;
import io.github.nitouge.conduit.cluster.RemoteInvoker;
import io.github.nitouge.conduit.common.exception.RpcException;
import io.github.nitouge.conduit.common.filter.Filter;
import io.github.nitouge.conduit.common.filter.FilterChain;
import io.github.nitouge.conduit.common.filter.FilterChainBuilder;
import io.github.nitouge.conduit.common.filter.FilterLoader;
import io.github.nitouge.conduit.common.filter.Side;
import io.github.nitouge.conduit.common.spi.ExtensionLoader;
import io.github.nitouge.conduit.registry.Registry;
import org.apache.commons.collections4.MapUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * 动态代理处理器：把接口方法调用变成一次RPC。
 *
 * <ul>
 *   <li><b>直连</b>：固定 {@link ServiceMeta}。</li>
 *   <li><b>注册中心</b>：每次 {@code discover} 取列表，交给 {@link Cluster}（容错/重试）+
 *   {@link LoadBalancer}（选址）+ 熔断完成调用。</li>
 *   <li><b>同步/异步统一调用链</b>：{@link #terminal()} 是同步、异步共用的链尾——都经过
 *   Cluster → LoadBalancer → RemoteInvoker → RpcClient，返回同一个
 *   {@code CompletableFuture<RpcResponse>}。同步只是在链外多做一次
 *   {@code get(timeout)} 阻塞展开；异步直接把future返回给调用方。FailOver等容错策略
 *   在future异常完成时决定是否换节点重试，两条路径因此享有完全一致的容错语义。</li>
 *   <li><b>Filter链</b>：同步与异步路径都经消费侧 {@link Filter} 链——链尾终端才是
 *   真正发起调用，熔断/负载均衡/集群重试都位于链尾内层。</li>
 *   <li><b>上下文透传</b>：把 {@link RpcContext} 的attachments拷入 {@link RpcRequest}
 *   后清空当前线程上下文（一次性语义）。</li>
 * </ul>
 */
public class RpcInvocationHandler implements InvocationHandler {

    private static final Logger log = LoggerFactory.getLogger(RpcInvocationHandler.class);

    /** 异步回调线程池：把结果映射与用户后续回调切出Netty I/O线程，避免阻塞事件循环 */
    private static final ExecutorService CALLBACK_POOL = Executors.newFixedThreadPool(
            Math.max(4, Runtime.getRuntime().availableProcessors()),
            r -> {
                Thread t = new Thread(r, "conduit-async-cb");
                t.setDaemon(true);
                return t;
            });

    private final Class<?> interfaceClass;
    private final RpcClient client;
    private final long timeoutMillis;

    /** 直连模式：固定目标；为null表示走注册中心 */
    private final ServiceMeta directTarget;
    /** 注册中心模式：非空表示走发现 + 集群容错 */
    private final Registry registry;
    private final Cluster cluster;
    private final LoadBalancer loadBalancer;

    /** 重试/退避/熔断参数（来自@RpcReference与全局conduit.cluster.*） */
    private final ClusterOptions clusterOptions;

    /** 消费侧filter链（按order排序、已过滤CONSUMER侧）；无filter时为空列表 */
    private final List<Filter> consumerFilters;

    /** M1直连构造（默认集群参数；直连模式不使用Cluster，参数仅保留一致性） */
    public RpcInvocationHandler(Class<?> interfaceClass, ServiceMeta target, RpcClient client, long timeoutMillis) {
        this(interfaceClass, target, client, timeoutMillis, ClusterOptions.defaults());
    }

    /** M1直连构造 + 自定义集群参数 */
    public RpcInvocationHandler(Class<?> interfaceClass, ServiceMeta target, RpcClient client,
                                long timeoutMillis, ClusterOptions clusterOptions) {
        this.interfaceClass = interfaceClass;
        this.directTarget = target;
        this.registry = null;
        this.cluster = null;
        this.loadBalancer = null;
        this.client = client;
        this.timeoutMillis = timeoutMillis;
        this.clusterOptions = (clusterOptions == null) ? ClusterOptions.defaults() : clusterOptions;
        this.consumerFilters = FilterLoader.load(Side.CONSUMER);
    }

    /** 注册中心构造（默认failover + random） */
    public RpcInvocationHandler(Class<?> interfaceClass, Registry registry, RpcClient client, long timeoutMillis) {
        this(interfaceClass, registry, client, timeoutMillis, "failover", "random", ClusterOptions.defaults());
    }

    /** 注册中心构造（指定集群容错策略 + 负载均衡策略） */
    public RpcInvocationHandler(Class<?> interfaceClass, Registry registry,
                                RpcClient client, long timeoutMillis,
                                String clusterName, String loadBalanceName) {
        this(interfaceClass, registry, client, timeoutMillis, clusterName, loadBalanceName,
                ClusterOptions.defaults());
    }

    /** 注册中心构造（指定策略 + 重试/退避/熔断参数） */
    public RpcInvocationHandler(Class<?> interfaceClass, Registry registry,
                                RpcClient client, long timeoutMillis,
                                String clusterName, String loadBalanceName,
                                ClusterOptions clusterOptions) {
        this.interfaceClass = interfaceClass;
        this.directTarget = null;
        this.registry = registry;
        this.client = client;
        this.timeoutMillis = timeoutMillis;
        // 自适应代理：默认用注解/配置的策略名，单次调用可通过RpcContext的
        // cluster / loadbalance attachment临时覆盖
        this.cluster = ExtensionLoader.getExtensionLoader(Cluster.class).getAdaptiveExtension(clusterName);
        this.loadBalancer = ExtensionLoader.getExtensionLoader(LoadBalancer.class).getAdaptiveExtension(loadBalanceName);
        this.clusterOptions = (clusterOptions == null) ? ClusterOptions.defaults() : clusterOptions;
        this.consumerFilters = FilterLoader.load(Side.CONSUMER);
        // 订阅只是感知列表变更的增强，失败不应阻断代理创建；invoke时仍走discover（带缓存兜底）。
        try {
            registry.subscribe(interfaceClass.getName(), services ->
                    log.info("发现服务{}提供者列表变更：{}个实例",
                            interfaceClass.getName(), services == null ? 0 : services.size()));
        } catch (Exception e) {
            log.warn("订阅服务{}失败（不影响调用，invoke时仍走discover兜底）: {}",
                    interfaceClass.getName(), e.toString());
        }
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        // Object的方法按代理自身的身份处理，绝不转发成远程调用，也不能盲目invoke到handler——
        // 否则proxy.equals(proxy)会变成handler.equals(proxy)返回false，破坏equals自反性，
        // 进而污染 把代理放进HashSet/HashMap、去重、容器比较 等场景。
        if (method.getDeclaringClass() == Object.class) {
            return switch (method.getName()) {
                case "equals" -> proxy == (args == null ? null : args[0]);
                case "hashCode" -> System.identityHashCode(proxy);
                case "toString" -> interfaceClass.getName() + "@RpcProxy@"
                        + Integer.toHexString(System.identityHashCode(proxy));
                default -> method.invoke(this, args);
            };
        }

        RpcRequest request = new RpcRequest();
        request.setInterfaceName(interfaceClass.getName());
        request.setMethodName(method.getName());
        request.setParamTypes(method.getParameterTypes());
        request.setArgs(args);
        // 把当前线程RpcContext的attachments拷入请求，随即清空（一次性语义，
        // 防止泄漏到同线程下次调用）。拷贝而非引用，避免请求持有可变上下文。
        Map<String, String> attachments = RpcContext.getContext().getAttachments();
        if (MapUtils.isNotEmpty(attachments)) {
            request.setAttachments(new HashMap<>(attachments));
        }
        RpcContext.removeContext();

        boolean async = method.getReturnType() == CompletableFuture.class;
        log.debug("[RpcInvocationHandler] 发起调用 {}#{}, mode={}, {}={}, attachments={}",
                interfaceClass.getName(), method.getName(),
                async ? "ASYNC" : "SYNC",
                registry != null ? "registry" : "direct",
                registry != null ? "discover" : (directTarget == null ? "null" : directTarget.getHost() + ":" + directTarget.getPort()),
                attachments.keySet());

        // 异步：方法返回CompletableFuture<T> → 不阻塞，直接返回future
        if (async) {
            return invokeAsync(request);
        }
        return invokeSync(request, method);
    }

    /**
     * 同步：经消费侧filter链发起调用；链尾终端与异步路径共享同一个 {@link #terminal()}——
     * 走注册中心时同样经过Cluster（容错/重试）+ LoadBalancer + RemoteInvoker，只是在链外
     * 用 {@code get(timeout)} 把future阻塞展开、统一施加超时上限。
     *
     * <p>把传输异常/超时统一转成RpcException，再按响应体的hasException抛业务异常/返回值。</p>
     */
    private Object invokeSync(RpcRequest request, Method method) throws Throwable {
        FilterChain chain = FilterChainBuilder.build(consumerFilters, terminal());

        RpcResponse response;
        try {
            response = chain.invoke(request).get(timeoutMillis, TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            throw new RpcException("调用超时(" + timeoutMillis + "ms): "
                    + interfaceClass.getName() + "#" + method.getName(), e);
        } catch (ExecutionException e) {
            throw toRpcThrowable(e.getCause() != null ? e.getCause() : e, method);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RpcException("调用被中断: " + interfaceClass.getName() + "#" + method.getName(), e);
        }

        if (response.hasException()) {
            throw response.getException();
        }
        return response.getData();
    }

    /** 把链上抛出的异常规整为可抛出的类型：RpcException/RuntimeException原样，其余包成RpcException。 */
    private Throwable toRpcThrowable(Throwable cause, Method method) {
        if (cause instanceof RpcException) {
            return cause;
        }
        if (cause instanceof RuntimeException) {
            return cause;
        }
        return new RpcException("集群调用失败: " + interfaceClass.getName() + "#" + method.getName(), cause);
    }

    /**
     * 异步：经消费侧filter链，与同步路径共享同一个 {@link #terminal()}——走注册中心时同样经过
     * Cluster（容错/重试）+ LoadBalancer + RemoteInvoker，直接把future返回给调用方，不做
     * 阻塞展开。之前版本这里绕开了Cluster（只做选址+sendAsync，没有容错重试）；统一之后
     * 异步调用也享有与同步一致的FailOver/熔断等集群容错语义。
     *
     * <p>映射用thenApplyAsync切到回调线程池，避免在Netty I/O线程上跑用户后续回调。</p>
     */
    private CompletableFuture<Object> invokeAsync(RpcRequest request) {
        FilterChain chain = FilterChainBuilder.build(consumerFilters, terminal());

        return chain.invoke(request).thenApplyAsync(response -> {
            if (response.hasException()) {
                throw new CompletionException(response.getException());
            }
            return response.getData();
        }, CALLBACK_POOL);
    }

    /**
     * 同步/异步共用的链尾终端：走注册中心discover + {@link Cluster#invoke}（内含负载均衡+容错重试）；
     * 直连模式则直接 {@code sendAsync} 到固定目标（无候选可选，Cluster语义在此不适用）。
     *
     * <p>两条路径都不同步抛异常——统一通过future的正常/异常完成表达结果，保持与
     * {@link FilterChain} 的契约一致。</p>
     */
    private FilterChain terminal() {
        if (registry != null) {
            return req -> {
                List<ServiceMeta> providers = registry.discover(interfaceClass.getName());
                return cluster.invoke(providers, req, loadBalancer, remoteInvoker(), clusterOptions);
            };
        }
        return req -> client.sendAsync(req, directTarget, timeoutMillis);
    }

    /** 给集群层注入的远程调用回调：直接转发到RpcClient的异步发送，传输失败体现为future异常完成。 */
    private RemoteInvoker remoteInvoker() {
        return (target, req) -> client.sendAsync(req, target, timeoutMillis);
    }
}
