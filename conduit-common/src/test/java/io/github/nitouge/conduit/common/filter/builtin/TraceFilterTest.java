package io.github.nitouge.conduit.common.filter.builtin;

import io.github.nitouge.conduit.api.RpcContext;
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
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class TraceFilterTest {

    private final TraceFilter filter = new TraceFilter();

    @AfterEach
    void clearMdc() {
        MDC.remove(TraceContext.TRACE_ID);
        MDC.remove(TraceContext.SPAN_ID);
        MDC.remove(TraceContext.PARENT_SPAN_ID);
        System.clearProperty(TraceContext.SAMPLE_RATE_KEY);
        RpcContext.removeContext();
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

    // ------------------------------------------------------------ span与采样

    @Test
    void createsNewSpanWithParentFromServingContext() {
        // 本线程正在服务一个请求（span=parent-1）：发起下游调用时应新建span、父节点指向它
        RpcContext.getContext().setAttachment(TraceContext.SPAN_ID, "parent-1");
        RpcRequest request = request();
        AtomicReference<String> spanDuringChain = new AtomicReference<>();

        filter.invoke(request, req -> {
            spanDuringChain.set(MDC.get(TraceContext.SPAN_ID));
            return CompletableFuture.completedFuture(RpcResponse.success("ok"));
        }).join();

        String spanId = request.getAttachment(TraceContext.SPAN_ID);
        assertNotNull(spanId);
        assertNotEquals("parent-1", spanId, "每次远程调用都应新建span");
        assertEquals("parent-1", request.getAttachment(TraceContext.PARENT_SPAN_ID));
        assertEquals(spanId, spanDuringChain.get(), "调用期间spanId应绑定到MDC");
        assertNull(MDC.get(TraceContext.SPAN_ID), "调用结束后应恢复MDC");
    }

    @Test
    void rootCallDropsStaleParentSpanId() {
        RpcRequest request = request();
        // 上游残留的父节点（例如调用方线程上下文里带来的旧值）不能被根调用继承
        request.setAttachment(TraceContext.PARENT_SPAN_ID, "stale-parent");

        filter.invoke(request, req -> CompletableFuture.completedFuture(RpcResponse.success("ok"))).join();

        assertNull(request.getAttachment(TraceContext.PARENT_SPAN_ID), "根调用不应带父节点");
        assertNotNull(request.getAttachment(TraceContext.SPAN_ID));
    }

    @Test
    void samplingDecisionIsMadeOnceAndKeptForTheWholeTrace() {
        System.setProperty(TraceContext.SAMPLE_RATE_KEY, "0");
        RpcRequest request = request();

        filter.invoke(request, req -> CompletableFuture.completedFuture(RpcResponse.success("ok"))).join();
        assertEquals("0", request.getAttachment(TraceContext.SAMPLED), "采样率为0时应标记不采");

        // 下游即便配了全采，也必须沿用上游的结论（否则会出现半截链路）
        System.setProperty(TraceContext.SAMPLE_RATE_KEY, "1.0");
        filter.invoke(request, req -> CompletableFuture.completedFuture(RpcResponse.success("ok"))).join();
        assertEquals("0", request.getAttachment(TraceContext.SAMPLED));
    }
}
