package io.github.nitouge.conduit.common.filter.builtin;

import io.github.nitouge.conduit.api.RpcRequest;
import io.github.nitouge.conduit.api.RpcResponse;
import io.github.nitouge.conduit.common.exception.RpcException;
import io.github.nitouge.conduit.common.trace.TraceContext;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class StructuredLogFilterTest {

    private static RpcRequest request() {
        RpcRequest request = new RpcRequest();
        request.setInterfaceName("com.example.\"Quoted\"");
        request.setMethodName("m");
        request.setAttachment(TraceContext.TRACE_ID, "trace-1");
        return request;
    }

    @Test
    void formatsJsonWithEscapedValuesAndErrorFields() {
        String json = StructuredLogFilter.format(request(), 12,
                RpcResponse.error(new RpcException("bad\nline")), null);

        assertTrue(json.startsWith("{"));
        assertTrue(json.endsWith("}"));
        assertTrue(json.contains("\"traceId\":\"trace-1\""));
        assertTrue(json.contains("\"durationMs\":12"));
        assertTrue(json.contains("\"success\":false"));
        assertTrue(json.contains("com.example.\\\"Quoted\\\""), json);
        assertTrue(json.contains("\"errorType\":\"io.github.nitouge.conduit.common.exception.RpcException\""));
        assertTrue(json.contains("bad\\nline"), json);
    }

    @Test
    void successOmitsErrorFields() {
        String json = StructuredLogFilter.format(request(), 1,
                RpcResponse.success("ok"), null);

        assertTrue(json.contains("\"success\":true"));
        assertFalse(json.contains("errorType"));
        assertFalse(json.contains("errorMessage"));
    }

    @Test
    void filterCompletesChain() {
        String json = StructuredLogFilter.format(request(), 0,
                CompletableFuture.completedFuture(RpcResponse.success("ok")).join(), null);
        assertTrue(new StructuredLogFilter()
                .invoke(request(), req -> CompletableFuture.completedFuture(RpcResponse.success("ok")))
                .join().getData() != null);
        assertTrue(json.contains("\"success\":true"));
    }
}
