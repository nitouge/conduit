package io.github.nitouge.conduit.client.proxy;

import io.github.nitouge.conduit.api.RpcAttachments;
import io.github.nitouge.conduit.api.RpcContext;
import io.github.nitouge.conduit.api.RpcRequest;
import io.github.nitouge.conduit.api.RpcResponse;
import io.github.nitouge.conduit.api.ServiceMeta;
import io.github.nitouge.conduit.client.ReferenceOptions;
import io.github.nitouge.conduit.client.RpcClient;
import io.github.nitouge.conduit.cluster.Cluster;
import io.github.nitouge.conduit.cluster.ClusterOptions;
import io.github.nitouge.conduit.cluster.LoadBalancer;
import io.github.nitouge.conduit.cluster.RemoteInvoker;
import io.github.nitouge.conduit.cluster.router.Router;
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
 *   <li><b>注册中心</b>：每次 {@code discover} 取列表，先经 {@link Router} 按
 *   version/group/tag 收窄候选（同机房优先），再交给 {@link Cluster}（容错/重试）+
 *   {@link LoadBalancer}（选址）+ 熔断完成调用。</li>
 *   <li><b>deadline</b>：把 {@code min(上游deadline, now+本引用超时)} 写进 attachments 下发，
 *   本地等待与每次重试的传输超时都用剩余预算——上游不等了，下游不会再白跑。</li>
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

    /** 该引用的静态路由偏好（version/group/tag），随每次请求下发 */
    private final ReferenceOptions referenceOptions;

    /** 路由：在负载均衡之前把候选实例收窄到本次调用真正该去的那一批 */
    private final Router router;

    /** 消费侧filter链（按order排序、已过滤CONSUMER侧）；无filter时为空列表 */
    private final List<Filter> consumerFilters;

    /** M1直连构造（默认集群参数；直连模式不使用Cluster，参数仅保留一致性） */
    public RpcInvocationHandler(Class<?> interfaceClass, ServiceMeta target, RpcClient client, long timeoutMillis) {
        this(interfaceClass, target, client, timeoutMillis, ClusterOptions.defaults(), ReferenceOptions.empty());
    }

    /** M1直连构造 + 自定义集群参数 */
    public RpcInvocationHandler(Class<?> interfaceClass, ServiceMeta target, RpcClient client,
                                long timeoutMillis, ClusterOptions clusterOptions) {
        this(interfaceClass, target, client, timeoutMillis, clusterOptions, ReferenceOptions.empty());
    }

    /** M1直连构造 + 集群参数 + 路由偏好 */
    public RpcInvocationHandler(Class<?> interfaceClass, ServiceMeta target, RpcClient client,
                                long timeoutMillis, ClusterOptions clusterOptions,
                                ReferenceOptions referenceOptions) {
        this.interfaceClass = interfaceClass;
        this.directTarget = target;
        this.registry = null;
        this.cluster = null;
        this.loadBalancer = null;
        this.client = client;
        this.timeoutMillis = timeoutMillis;
        this.clusterOptions = (clusterOptions == null) ? ClusterOptions.defaults() : clusterOptions;
        this.referenceOptions = (referenceOptions == null) ? ReferenceOptions.empty() : referenceOptions;
        this.router = ExtensionLoader.getExtensionLoader(Router.class).getDefaultExtension();
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
        this(interfaceClass, registry, client, timeoutMillis, clusterName, loadBalanceName,
                clusterOptions, ReferenceOptions.empty());
    }

    /** 注册中心构造（指定策略 + 重试参数 + 路由偏好） */
    public RpcInvocationHandler(Class<?> interfaceClass, Registry registry,
                                RpcClient client, long timeoutMillis,
                                String clusterName, String loadBalanceName,
                                ClusterOptions clusterOptions, ReferenceOptions referenceOptions) {
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
        this.referenceOptions = (referenceOptions == null) ? ReferenceOptions.empty() : referenceOptions;
        this.router = ExtensionLoader.getExtensionLoader(Router.class).getDefaultExtension();
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
        // 路由偏好：单次调用的attachment覆盖@RpcReference的静态声明（灰度切流就是这么做的）
        applyReference(request);
        // deadline：min(上游已带来的deadline, now+本引用超时)，随请求下发；下游据此到点自我中断
        RpcAttachments.applyDeadline(request, timeoutMillis, System.currentTimeMillis());

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

        // 等待上限 = min(本引用超时, deadline - now)：deadline 是整条链路共享的预算，
        // 上游给了更早的截止时间，这里就不该按自己的超时傻等
        long budget = RpcAttachments.budgetMillis(request, timeoutMillis, System.currentTimeMillis());
        RpcResponse response;
        try {
            response = chain.invoke(request).get(budget, TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            throw new RpcException(timeoutMessage(budget, method), e);
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

    /** 超时原因尽量说清「是本地超时还是deadline预算耗尽」——两者的处理方式完全不同 */
    private String timeoutMessage(long budget, Method method) {
        String reason = budget < timeoutMillis ? "deadline预算耗尽" : "本地超时";
        return "调用超时(" + budget + "ms, " + reason + "): " + interfaceClass.getName() + "#" + method.getName();
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
     * 同步/异步共用的链尾终端：discover → {@link Router}（版本/分组/标签/机房）→
     * {@link Cluster#invoke}（内含负载均衡+容错重试）；直连模式则直接 {@code sendAsync}
     * 到固定目标（无候选可选，Cluster/路由语义在此不适用）。
     *
     * <p>链路契约要求终端<b>绝不同步抛出</b>（无filter时chain就是terminal本身），
     * 故路由为空/集群无候选等异常都转成failed future。</p>
     */
    private FilterChain terminal() {
        if (registry != null) {
            return req -> {
                try {
                    List<ServiceMeta> providers = router.route(registry.discover(interfaceClass.getName()), req);
                    return cluster.invoke(providers, req, loadBalancer, remoteInvoker(), clusterOptions);
                } catch (Throwable t) {
                    return failed(t);
                }
            };
        }
        return req -> client.sendAsync(req, directTarget, timeoutFor(req));
    }

    /** 把同步异常转成统一的failed future，保持 FilterChain 契约 */
    private static CompletableFuture<RpcResponse> failed(Throwable cause) {
        CompletableFuture<RpcResponse> failed = new CompletableFuture<>();
        failed.completeExceptionally(cause);
        return failed;
    }

    /** 每次真正发起远程调用的超时预算（重试也会被 deadline 收敛，越到后面预算越小） */
    private long timeoutFor(RpcRequest request) {
        return RpcAttachments.budgetMillis(request, timeoutMillis, System.currentTimeMillis());
    }

    /** 给集群层注入的远程调用回调：直接转发到RpcClient的异步发送，传输失败体现为future异常完成。 */
    private RemoteInvoker remoteInvoker() {
        return (target, req) -> client.sendAsync(req, target, timeoutFor(req));
    }

    /**
     * 把路由偏好写进请求：{@code RpcContext} 的 attachment（单次调用）优先于
     * {@link ReferenceOptions}（{@code @RpcReference} 的静态声明）。
     */
    private void applyReference(RpcRequest request) {
        String version = firstNonBlank(request.getAttachment(RpcAttachments.VERSION), referenceOptions.getVersion());
        if (version != null) {
            request.setVersion(version);
        }
        String group = firstNonBlank(request.getAttachment(RpcAttachments.GROUP), referenceOptions.getGroup());
        if (group != null) {
            request.setGroup(group);
        }
        String tag = firstNonBlank(request.getAttachment(RpcAttachments.TAG), referenceOptions.getTag());
        if (tag != null) {
            request.setAttachment(RpcAttachments.TAG, tag);
        }
    }

    private static String firstNonBlank(String first, String second) {
        if (first != null && !first.isBlank()) {
            return first.trim();
        }
        if (second != null && !second.isBlank()) {
            return second.trim();
        }
        return null;
    }
}
