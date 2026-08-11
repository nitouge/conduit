package io.github.nitouge.conduit.cluster;

import io.github.nitouge.conduit.api.RpcRequest;
import io.github.nitouge.conduit.api.RpcResponse;
import io.github.nitouge.conduit.api.ServiceMeta;
import io.github.nitouge.conduit.cluster.breaker.CircuitBreakerRegistry;
import io.github.nitouge.conduit.cluster.loadbalance.RandomLoadBalancer;
import io.github.nitouge.conduit.cluster.support.FailbackCluster;
import io.github.nitouge.conduit.cluster.support.FailfastCluster;
import io.github.nitouge.conduit.cluster.support.FailoverCluster;
import io.github.nitouge.conduit.cluster.support.FailsafeCluster;
import io.github.nitouge.conduit.common.exception.RpcException;
import io.github.nitouge.conduit.common.spi.ExtensionLoader;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ClusterTest {

    private final ServiceMeta good = new ServiceMeta("svc", "10.0.0.1", 1);
    private final ServiceMeta bad = new ServiceMeta("svc", "10.0.0.9", 2);
    private final LoadBalancer lb = new RandomLoadBalancer();

    private RpcRequest req() {
        RpcRequest r = new RpcRequest();
        r.setInterfaceName("svc");
        r.setMethodName("m");
        return r;
    }

    /** RemoteInvoker现在返回CompletableFuture；测试里用它快速造一个异常完成的future。 */
    private static CompletableFuture<RpcResponse> failedFuture(Throwable t) {
        CompletableFuture<RpcResponse> f = new CompletableFuture<>();
        f.completeExceptionally(t);
        return f;
    }

    @BeforeEach
    void resetBreakers() {
        CircuitBreakerRegistry.clear();
    }

    @Test
    void clustersLoadViaSpi() {
        ExtensionLoader<Cluster> loader = ExtensionLoader.getExtensionLoader(Cluster.class);
        assertInstanceOf(FailoverCluster.class, loader.getDefaultExtension()); // @SPI("failover")
        assertInstanceOf(FailfastCluster.class, loader.getExtension("failfast"));
        assertInstanceOf(FailsafeCluster.class, loader.getExtension("failsafe"));
        assertInstanceOf(FailbackCluster.class, loader.getExtension("failback"));
    }

    @Test
    void failoverRetriesOtherNodeWhenOneFails() throws Exception {
        Cluster failover = new FailoverCluster();
        // bad节点必失败，good节点成功
        RemoteInvoker ri = (node, request) -> {
            if (node.equals(bad)) {
                return failedFuture(new RpcException("connection refused: " + node.getHost()));
            }
            return CompletableFuture.completedFuture(RpcResponse.success("ok-from-" + node.getHost()));
        };
        List<ServiceMeta> list = Arrays.asList(bad, good);

        RpcResponse resp = failover.invoke(list, req(), lb, ri).get(1, TimeUnit.SECONDS);
        // 无论随机先选到谁，最终都会切到good成功
        assertEquals("ok-from-10.0.0.1", resp.getData());
    }

    @Test
    void failfastThrowsImmediately() {
        Cluster failfast = new FailfastCluster();
        RemoteInvoker ri = (node, request) -> failedFuture(new RpcException("down"));
        CompletableFuture<RpcResponse> future = failfast.invoke(Arrays.asList(good), req(), lb, ri);
        ExecutionException ex = assertThrows(ExecutionException.class,
                () -> future.get(1, TimeUnit.SECONDS));
        assertInstanceOf(RpcException.class, ex.getCause());
    }

    @Test
    void failsafeReturnsEmptyOnFailure() throws Exception {
        Cluster failsafe = new FailsafeCluster();
        RemoteInvoker ri = (node, request) -> failedFuture(new RpcException("down"));
        RpcResponse resp = failsafe.invoke(Arrays.asList(good), req(), lb, ri).get(1, TimeUnit.SECONDS);
        assertNull(resp.getData());
        assertNull(resp.getException());
    }

    @Test
    void failbackReturnsEmptyAndSchedulesRetry() throws Exception {
        Cluster failback = new FailbackCluster();
        RemoteInvoker ri = (node, request) -> failedFuture(new RpcException("down"));
        RpcResponse resp = failback.invoke(Arrays.asList(good), req(), lb, ri).get(1, TimeUnit.SECONDS);
        assertNull(resp.getData()); // 立即返回空，后台best-effort重试
    }
}
