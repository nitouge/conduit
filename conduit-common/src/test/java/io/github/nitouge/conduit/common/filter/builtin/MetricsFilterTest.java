package io.github.nitouge.conduit.common.filter.builtin;

import io.github.nitouge.conduit.api.RpcRequest;
import io.github.nitouge.conduit.api.RpcResponse;
import io.github.nitouge.conduit.common.exception.RpcException;
import io.github.nitouge.conduit.common.filter.FilterChain;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MetricsFilterTest {

    private final MetricsFilter filter = new MetricsFilter();

    @BeforeEach
    void reset() {
        MetricsFilter.reset();
    }

    private static RpcRequest request() {
        RpcRequest request = new RpcRequest();
        request.setInterfaceName("svc");
        request.setMethodName("m");
        return request;
    }

    @Test
    void recordsSuccessFailureLatencyAndDistribution() {
        RpcRequest request = request();
        FilterChain ok = req -> CompletableFuture.completedFuture(RpcResponse.success("ok"));
        FilterChain businessError = req -> CompletableFuture.completedFuture(
                RpcResponse.error(new RpcException("business")));
        FilterChain transportError = req -> {
            CompletableFuture<RpcResponse> failed = new CompletableFuture<>();
            failed.completeExceptionally(new RpcException("transport"));
            return failed;
        };

        filter.invoke(request, ok).join();
        filter.invoke(request, businessError).join();
        filter.invoke(request, transportError).handle((r, e) -> null);

        MetricsFilter.Stats stats = MetricsFilter.snapshot().get("svc#m");
        assertEquals(3, stats.total());
        assertEquals(1, stats.success());
        assertEquals(2, stats.error());
        assertEquals(2.0 / 3.0, stats.errorRate(), 1e-9);
        assertTrue(stats.maxRtMs() >= stats.minRtMs());
        assertTrue(stats.avgRtMs() >= 0);

        long bucketTotal = 0;
        for (long count : stats.bucketCounts()) {
            bucketTotal += count;
        }
        assertEquals(3, bucketTotal, "所有延迟桶计数之和应等于调用数");
        assertTrue(stats.estimatedPercentileMs(0.95) > 0);

        assertEquals(3, MetricsFilter.totalCalls());
        assertEquals(2, MetricsFilter.totalErrors());
    }

    @Test
    void resetClearsAllStats() {
        filter.invoke(request(), req -> CompletableFuture.completedFuture(RpcResponse.success("ok"))).join();
        assertEquals(1, MetricsFilter.totalCalls());

        MetricsFilter.reset();

        assertEquals(0, MetricsFilter.totalCalls());
        assertTrue(MetricsFilter.snapshot().isEmpty());
    }
}
