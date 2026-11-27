package io.github.nitouge.conduit.client.proxy;

import io.github.nitouge.conduit.api.ServiceMeta;
import io.github.nitouge.conduit.client.ReferenceOptions;
import io.github.nitouge.conduit.client.RpcClient;
import io.github.nitouge.conduit.cluster.ClusterOptions;
import io.github.nitouge.conduit.registry.Registry;

import java.lang.reflect.Proxy;

/**
 * 代理工厂：为接口生成JDK动态代理对象，调用它就像调本地方法，实际走完整RPC链路。
 *
 * <p>{@link ClusterOptions} 重载用于传入重试次数/退避/熔断参数；不传时使用默认值
 * （重试2次、不退避、熔断阈值5/5s），保持既有调用方式不变。</p>
 */
public class RpcClientProxy {

    private RpcClientProxy() {
    }

    /**
     * 直连显式给定目标 {@link ServiceMeta}
     */
    public static <T> T create(Class<T> interfaceClass, ServiceMeta target, RpcClient client, long timeoutMillis) {
        return create(interfaceClass, target, client, timeoutMillis, ClusterOptions.defaults());
    }

    /**
     * 直连 + 自定义集群参数
     */
    public static <T> T create(Class<T> interfaceClass, ServiceMeta target, RpcClient client,
                               long timeoutMillis, ClusterOptions clusterOptions) {
        return create(interfaceClass, target, client, timeoutMillis, clusterOptions, ReferenceOptions.empty());
    }

    /**
     * 直连 + 集群参数 + 路由偏好（version/group/tag）
     */
    @SuppressWarnings("unchecked")
    public static <T> T create(Class<T> interfaceClass, ServiceMeta target, RpcClient client,
                               long timeoutMillis, ClusterOptions clusterOptions,
                               ReferenceOptions referenceOptions) {
        return (T) Proxy.newProxyInstance(
                interfaceClass.getClassLoader(),
                new Class<?>[]{interfaceClass},
                new RpcInvocationHandler(interfaceClass, target, client, timeoutMillis,
                        clusterOptions, referenceOptions));
    }

    /**
     * 注册中心：经 {@link Registry} 发现 + 默认集群容错(failover)/负载均衡(random)
     */
    @SuppressWarnings("unchecked")
    public static <T> T create(Class<T> interfaceClass, Registry registry, RpcClient client, long timeoutMillis) {
        return (T) Proxy.newProxyInstance(
                interfaceClass.getClassLoader(),
                new Class<?>[]{interfaceClass},
                new RpcInvocationHandler(interfaceClass, registry, client, timeoutMillis));
    }

    /**
     * 注册中心 + 自定义集群参数
     */
    @SuppressWarnings("unchecked")
    public static <T> T create(Class<T> interfaceClass, Registry registry, RpcClient client,
                               long timeoutMillis, ClusterOptions clusterOptions) {
        return (T) Proxy.newProxyInstance(
                interfaceClass.getClassLoader(),
                new Class<?>[]{interfaceClass},
                new RpcInvocationHandler(interfaceClass, registry, client, timeoutMillis,
                        "failover", "random", clusterOptions));
    }

    /**
     * 注册中心：指定集群容错策略（failover/failfast/failsafe/failback）与负载均衡（random/roundrobin/consistenthash）
     */
    public static <T> T create(Class<T> interfaceClass, Registry registry, RpcClient client, long timeoutMillis,
                               String cluster, String loadBalance) {
        return create(interfaceClass, registry, client, timeoutMillis, cluster, loadBalance,
                ClusterOptions.defaults());
    }

    /**
     * 注册中心：指定策略 + 重试/退避/熔断参数
     */
    public static <T> T create(Class<T> interfaceClass, Registry registry, RpcClient client, long timeoutMillis,
                               String cluster, String loadBalance, ClusterOptions clusterOptions) {
        return create(interfaceClass, registry, client, timeoutMillis, cluster, loadBalance,
                clusterOptions, ReferenceOptions.empty());
    }

    /**
     * 注册中心：指定策略 + 重试参数 + 路由偏好（version/group/tag）。
     *
     * <p>路由偏好决定 discover 之后候选实例的收窄条件，是灰度与多版本共存的基础。</p>
     */
    @SuppressWarnings("unchecked")
    public static <T> T create(Class<T> interfaceClass, Registry registry, RpcClient client, long timeoutMillis,
                               String cluster, String loadBalance, ClusterOptions clusterOptions,
                               ReferenceOptions referenceOptions) {
        return (T) Proxy.newProxyInstance(
                interfaceClass.getClassLoader(),
                new Class<?>[]{interfaceClass},
                new RpcInvocationHandler(interfaceClass, registry, client, timeoutMillis,
                        cluster, loadBalance, clusterOptions, referenceOptions));
    }
}
