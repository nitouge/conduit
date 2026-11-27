package io.github.nitouge.conduit.testkit;

import io.github.nitouge.conduit.api.ServiceMeta;
import io.github.nitouge.conduit.client.ReferenceOptions;
import io.github.nitouge.conduit.client.RpcClient;
import io.github.nitouge.conduit.client.proxy.RpcClientProxy;
import io.github.nitouge.conduit.cluster.ClusterOptions;
import io.github.nitouge.conduit.registry.Registry;
import io.github.nitouge.conduit.remoting.ClientOptions;

/**
 * 客户端夹具：封装 {@link RpcClient} 与代理创建，保证测试里不重复写
 * `new RpcClient()` / `RpcClientProxy.create(...) * 4重载` / `finally close()`。
 *
 * <pre>{@code
 * try (TestRpc rpc = TestRpc.create("kryo")) {
 *     EchoService echo = rpc.proxy(EchoService.class, registry, 3000);
 *     assertEquals("ok:hi", echo.echo("hi"));
 * }
 * }</pre>
 *
 * <p>默认单次调用超时 {@value #DEFAULT_TIMEOUT_MILLIS}ms；不同的超时/集群策略直接用对应重载。</p>
 */
public final class TestRpc implements AutoCloseable {

    /** 默认调用超时（毫秒）：足够覆盖本机首连+握手，又能在失败场景快速暴露 */
    public static final long DEFAULT_TIMEOUT_MILLIS = 3_000;

    private final RpcClient client;

    private TestRpc(RpcClient client) {
        this.client = client;
    }

    /** 默认配置（JDK序列化） */
    public static TestRpc create() {
        return new TestRpc(new RpcClient());
    }

    /** 指定序列化（jdk/json/kryo/protostuff） */
    public static TestRpc create(String serializer) {
        return new TestRpc(new RpcClient(serializer));
    }

    /** 指定客户端传输配置（压缩/TLS/心跳/连接池） */
    public static TestRpc create(ClientOptions options) {
        return new TestRpc(new RpcClient(options));
    }

    /** 直连代理：目标地址显式给出 */
    public <T> T proxy(Class<T> interfaceClass, ServiceMeta target) {
        return proxy(interfaceClass, target, DEFAULT_TIMEOUT_MILLIS);
    }

    public <T> T proxy(Class<T> interfaceClass, ServiceMeta target, long timeoutMillis) {
        return RpcClientProxy.create(interfaceClass, target, client, timeoutMillis);
    }

    /** 注册中心代理：默认failover + random */
    public <T> T proxy(Class<T> interfaceClass, Registry registry) {
        return proxy(interfaceClass, registry, DEFAULT_TIMEOUT_MILLIS);
    }

    public <T> T proxy(Class<T> interfaceClass, Registry registry, long timeoutMillis) {
        return RpcClientProxy.create(interfaceClass, registry, client, timeoutMillis);
    }

    /** 注册中心代理：指定容错策略与负载均衡 */
    public <T> T proxy(Class<T> interfaceClass, Registry registry, long timeoutMillis,
                       String cluster, String loadBalance) {
        return proxy(interfaceClass, registry, timeoutMillis, cluster, loadBalance, ClusterOptions.defaults());
    }

    /** 注册中心代理：策略 + 重试/退避/熔断参数 */
    public <T> T proxy(Class<T> interfaceClass, Registry registry, long timeoutMillis,
                       String cluster, String loadBalance, ClusterOptions clusterOptions) {
        return RpcClientProxy.create(interfaceClass, registry, client, timeoutMillis,
                cluster, loadBalance, clusterOptions);
    }

    /** 注册中心代理：带路由偏好（version/group/tag），默认 failover + random */
    public <T> T proxy(Class<T> interfaceClass, Registry registry, ReferenceOptions referenceOptions) {
        return proxy(interfaceClass, registry, DEFAULT_TIMEOUT_MILLIS, referenceOptions);
    }

    /** 注册中心代理：指定超时 + 路由偏好 */
    public <T> T proxy(Class<T> interfaceClass, Registry registry, long timeoutMillis,
                       ReferenceOptions referenceOptions) {
        return RpcClientProxy.create(interfaceClass, registry, client, timeoutMillis,
                "failover", "random", ClusterOptions.defaults(), referenceOptions);
    }

    /** 注册中心代理：策略 + 重试参数 + 路由偏好（多版本/灰度集群的完整形态） */
    public <T> T proxy(Class<T> interfaceClass, Registry registry, long timeoutMillis,
                       String cluster, String loadBalance, ClusterOptions clusterOptions,
                       ReferenceOptions referenceOptions) {
        return RpcClientProxy.create(interfaceClass, registry, client, timeoutMillis,
                cluster, loadBalance, clusterOptions, referenceOptions);
    }

    /** 底层客户端（需要直接断言连接数/心跳计数等指标时用） */
    public RpcClient client() {
        return client;
    }

    @Override
    public void close() {
        client.close();
    }
}
