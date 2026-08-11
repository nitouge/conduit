package io.github.nitouge.conduit.client.proxy;

import io.github.nitouge.conduit.api.ServiceMeta;
import io.github.nitouge.conduit.client.RpcClient;
import io.github.nitouge.conduit.registry.Registry;

import java.lang.reflect.Proxy;

/**
 * 代理工厂：为接口生成JDK动态代理对象，调用它就像调本地方法，实际走完整RPC链路。
 */
public class RpcClientProxy {

    private RpcClientProxy() {
    }

    /** M1直连：显式给定目标 {@link ServiceMeta} */
    @SuppressWarnings("unchecked")
    public static <T> T create(Class<T> interfaceClass, ServiceMeta target, RpcClient client, long timeoutMillis) {
        return (T) Proxy.newProxyInstance(
                interfaceClass.getClassLoader(),
                new Class<?>[]{interfaceClass},
                new RpcInvocationHandler(interfaceClass, target, client, timeoutMillis));
    }

    /** 注册中心：经 {@link Registry} 发现 + 默认集群容错(failover)/负载均衡(random) */
    @SuppressWarnings("unchecked")
    public static <T> T create(Class<T> interfaceClass, Registry registry, RpcClient client, long timeoutMillis) {
        return (T) Proxy.newProxyInstance(
                interfaceClass.getClassLoader(),
                new Class<?>[]{interfaceClass},
                new RpcInvocationHandler(interfaceClass, registry, client, timeoutMillis));
    }

    /** 注册中心：指定集群容错策略（failover/failfast/failsafe/failback）与负载均衡（random/roundrobin/consistenthash） */
    @SuppressWarnings("unchecked")
    public static <T> T create(Class<T> interfaceClass, Registry registry, RpcClient client, long timeoutMillis, String cluster, String loadBalance) {
        return (T) Proxy.newProxyInstance(
                interfaceClass.getClassLoader(),
                new Class<?>[]{interfaceClass},
                new RpcInvocationHandler(interfaceClass, registry, client, timeoutMillis, cluster, loadBalance));
    }
}
