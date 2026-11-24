package io.github.nitouge.conduit.common.filter.builtin;

import io.github.nitouge.conduit.api.RpcRequest;
import io.github.nitouge.conduit.api.RpcResponse;
import io.github.nitouge.conduit.common.filter.FilterChain;
import io.github.nitouge.conduit.common.trace.TraceContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.slf4j.MDC;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class TraceFilterTest {

    private final TraceFilter filter = new TraceFilter();

    @AfterEach
    void clearMdc() {
        MDC.remove(TraceContext.TRACE_ID);
    }

    private static RpcRequest request() {
        RpcRequest request = new RpcRequest();
        request.setInterfaceName("svc");
        request.setMethodName("m");
        return request;
    }

    @Test
    void generatesTraceIdWhenAbsentAndBindsMdcDuringChain() {
        RpcRequest request = request();
        AtomicReference<String> traceDuringChain = new AtomicReference<>();
        AtomicReference<String> mdcDuringChain = new AtomicReference<>();
        FilterChain chain = req -> {
            traceDuringChain.set(TraceContext.currentTraceId());
            mdcDuringChain.set(MDC.get(TraceContext.TRACE_ID));
            return CompletableFuture.completedFuture(RpcResponse.success("ok"));
        };

        filter.invoke(request, chain).join();

        String traceId = request.getAttachment(TraceContext.TRACE_ID);
        assertNotNull(traceId);
        assertEquals(32, traceId.length(), "生成的traceId应为32位十六进制");
        assertEquals(traceId, traceDuringChain.get());
        assertEquals(traceId, mdcDuringChain.get(), "链路执行期间traceId应绑定到MDC");
        assertNull(MDC.get(TraceContext.TRACE_ID), "调用结束后应恢复MDC");
    }

    @Test
    void preservesExistingTraceId() {
        RpcRequest request = request();
        request.setAttachment(TraceContext.TRACE_ID, "existing-trace");
        AtomicReference<String> seen = new AtomicReference<>();
        FilterChain chain = req -> {
            seen.set(req.getAttachment(TraceContext.TRACE_ID));
            return CompletableFuture.completedFuture(RpcResponse.success("ok"));
        };

        filter.invoke(request, chain).join();

        assertEquals("existing-trace", seen.get());
        assertEquals("existing-trace", request.getAttachment(TraceContext.TRACE_ID));
    }

    @Test
    void restoresPreviousMdcValue() {
        MDC.put(TraceContext.TRACE_ID, "outer-trace");
        RpcRequest request = request();
        FilterChain chain = req -> CompletableFuture.completedFuture(RpcResponse.success("ok"));

        filter.invoke(request, chain).join();

        assertEquals("outer-trace", MDC.get(TraceContext.TRACE_ID));
    }

    @Test
    void restoresMdcEvenWhenChainThrows() {
        RpcRequest request = request();
        FilterChain chain = req -> {
            throw new IllegalStateException("boom");
        };

        assertThrows(IllegalStateException.class, () -> filter.invoke(request, chain));
        assertNull(MDC.get(TraceContext.TRACE_ID));
    }
}
