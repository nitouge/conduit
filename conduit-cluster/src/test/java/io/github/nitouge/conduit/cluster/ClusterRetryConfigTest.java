package io.github.nitouge.conduit.cluster;

import io.github.nitouge.conduit.api.RpcRequest;
import io.github.nitouge.conduit.api.RpcResponse;
import io.github.nitouge.conduit.api.ServiceMeta;
import io.github.nitouge.conduit.cluster.breaker.CircuitBreakerRegistry;
import io.github.nitouge.conduit.cluster.loadbalance.RandomLoadBalancer;
import io.github.nitouge.conduit.cluster.support.FailoverCluster;
import io.github.nitouge.conduit.common.exception.RpcException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 重试策略配置：重试次数与退避来自 {@link ClusterOptions}，按调用生效。
 */
class ClusterRetryConfigTest {

    private final ServiceMeta n1 = new ServiceMeta("svc", "10.0.0.1", 1);
    private final ServiceMeta n2 = new ServiceMeta("svc", "10.0.0.2", 2);
    private final ServiceMeta n3 = new ServiceMeta("svc", "10.0.0.3", 3);
    private final LoadBalancer lb = new RandomLoadBalancer();

    @BeforeEach
    void resetBreakers() {
        CircuitBreakerRegistry.clear();
    }

    private RpcRequest req() {
        RpcRequest request = new RpcRequest();
        request.setInterfaceName("svc");
        request.setMethodName("m");
        return request;
    }

    private static CompletableFuture<RpcResponse> failed(Throwable t) {
        CompletableFuture<RpcResponse> future = new CompletableFuture<>();
        future.completeExceptionally(t);
        return future;
    }

    @Test
    void retriesFromOptionsLimitAttempts() {
        AtomicInteger calls = new AtomicInteger();
        RemoteInvoker invoker = (node, request) -> {
            calls.incrementAndGet();
            return failed(new RpcException("down"));
        };
        List<ServiceMeta> nodes = Arrays.asList(n1, n2, n3);

        CompletableFuture<RpcResponse> future = new FailoverCluster().invoke(nodes, req(), lb, invoker,
                ClusterOptions.builder().retries(1).build());

        assertThrows(ExecutionException.class, () -> future.get(2, TimeUnit.SECONDS));
        assertEquals(2, calls.get(), "retries=1 表示总尝试2次（首次 + 1次重试）");
    }

    @Test
    void zeroRetriesMeansSingleAttempt() {
        AtomicInteger calls = new AtomicInteger();
        RemoteInvoker invoker = (node, request) -> {
            calls.incrementAndGet();
            return failed(new RpcException("down"));
        };

        CompletableFuture<RpcResponse> future = new FailoverCluster().invoke(
                Arrays.asList(n1, n2, n3), req(), lb, invoker,
                ClusterOptions.builder().retries(0).build());

        assertThrows(ExecutionException.class, () -> future.get(2, TimeUnit.SECONDS));
        assertEquals(1, calls.get());
    }

    @Test
    void retryBackoffDelaysNextAttempt() {
        AtomicInteger calls = new AtomicInteger();
        RemoteInvoker invoker = (node, request) -> {
            calls.incrementAndGet();
            return failed(new RpcException("down"));
        };

        long start = System.nanoTime();
        CompletableFuture<RpcResponse> future = new FailoverCluster().invoke(
                Arrays.asList(n1, n2), req(), lb, invoker,
                ClusterOptions.builder().retries(1).retryBackoffMs(150).build());
        assertThrows(ExecutionException.class, () -> future.get(3, TimeUnit.SECONDS));
        long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);

        assertEquals(2, calls.get());
        assertTrue(elapsedMs >= 120, "重试退避应生效，实际耗时=" + elapsedMs + "ms");
    }

    @Test
    void fourArgInvocationKeepsDefaultTwoRetries() {
        AtomicInteger calls = new AtomicInteger();
        RemoteInvoker invoker = (node, request) -> {
            calls.incrementAndGet();
            return failed(new RpcException("down"));
        };

        CompletableFuture<RpcResponse> future = new FailoverCluster().invoke(
                Arrays.asList(n1, n2, n3), req(), lb, invoker);

        assertThrows(ExecutionException.class, () -> future.get(2, TimeUnit.SECONDS));
        assertEquals(3, calls.get(), "四参调用保持既有行为：默认重试2次");
    }

    @Test
    void optionsRejectInvalidValues() {
        assertThrows(IllegalArgumentException.class, () -> ClusterOptions.builder().retries(-1));
        assertThrows(IllegalArgumentException.class, () -> ClusterOptions.builder().retryBackoffMs(-1));
        assertThrows(IllegalArgumentException.class, () -> CircuitBreakerOptions.builder().failureThreshold(0));
        assertThrows(IllegalArgumentException.class, () -> CircuitBreakerOptions.builder().openDurationMs(-1));
    }
}
