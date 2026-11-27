package io.github.nitouge.conduit.common.trace;

import io.github.nitouge.conduit.api.RpcAttachments;
import io.github.nitouge.conduit.api.RpcContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.slf4j.MDC;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 追踪上下文工具测试：spanId 生成、确定性采样、MDC 三元组绑定与恢复。
 */
class TraceContextTest {

    @AfterEach
    void clear() {
        System.clearProperty(TraceContext.SAMPLE_RATE_KEY);
        RpcContext.removeContext();
        MDC.remove(RpcAttachments.TRACE_ID);
        MDC.remove(RpcAttachments.SPAN_ID);
        MDC.remove(RpcAttachments.PARENT_SPAN_ID);
    }

    @Test
    void spanIdIsFreshAndWellFormed() {
        String first = TraceContext.nextSpanId();
        String second = TraceContext.nextSpanId();

        assertEquals(16, first.length(), "spanId应为16位十六进制");
        assertTrue(first.matches("[0-9a-f]{16}"), first);
        assertNotEquals(first, second, "每次都应换新的spanId");
    }

    @Test
    void sampleRateBoundaries() {
        System.setProperty(TraceContext.SAMPLE_RATE_KEY, "1.0");
        assertTrue(TraceContext.shouldSample("abc"));

        System.setProperty(TraceContext.SAMPLE_RATE_KEY, "0");
        assertFalse(TraceContext.shouldSample("abc"));
    }

    @Test
    void samplingIsDeterministicForTheSameTraceId() {
        System.setProperty(TraceContext.SAMPLE_RATE_KEY, "0.3");

        // 确定性是"整条链结论一致"的前提：同一个traceId在哪台机器上都必须得到同一个结论
        boolean first = TraceContext.shouldSample("9f86d081884c7d659a2feaa0c55ad015");
        for (int i = 0; i < 20; i++) {
            assertEquals(first, TraceContext.shouldSample("9f86d081884c7d659a2feaa0c55ad015"));
        }
    }

    @Test
    void invalidSampleRateFallsBackToDefault() {
        System.setProperty(TraceContext.SAMPLE_RATE_KEY, "not-a-number");
        assertTrue(TraceContext.shouldSample("abc"), "非法值应回退为默认全采");
    }

    @Test
    void mdcTripleIsBoundAndRestored() {
        MDC.put(RpcAttachments.TRACE_ID, "outer-trace");

        TraceContext.MdcSnapshot previous = TraceContext.bindMdc("t-1", "s-1", null);
        assertEquals("t-1", MDC.get(RpcAttachments.TRACE_ID));
        assertEquals("s-1", MDC.get(RpcAttachments.SPAN_ID));
        assertNull(MDC.get(RpcAttachments.PARENT_SPAN_ID), "空值应移除而不是写入\"null\"");

        TraceContext.restoreMdc(previous);
        assertEquals("outer-trace", MDC.get(RpcAttachments.TRACE_ID));
        assertNull(MDC.get(RpcAttachments.SPAN_ID));
    }

    @Test
    void currentSpanIdReadsFromMdcThenContext() {
        RpcContext.getContext().setAttachment(RpcAttachments.SPAN_ID, "ctx-span");
        assertEquals("ctx-span", TraceContext.currentSpanId(), "没有MDC时应回退到RpcContext");

        MDC.put(RpcAttachments.SPAN_ID, "mdc-span");
        assertEquals("mdc-span", TraceContext.currentSpanId(), "MDC优先");
    }
}
