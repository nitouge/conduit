package io.github.nitouge.conduit.common.filter.builtin;

import io.github.nitouge.conduit.api.RpcRequest;
import io.github.nitouge.conduit.api.RpcResponse;
import io.github.nitouge.conduit.common.filter.Filter;
import io.github.nitouge.conduit.common.filter.FilterChain;
import io.github.nitouge.conduit.common.trace.TraceContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * 结构化访问日志Filter：每次调用输出一行JSON（logger = {@code CONDUIT.ACCESS.JSON}），
 * 便于被ELK/Loki/ClickHouse等直接采集。
 *
 * <p>字段：{@code timestamp}、{@code traceId}、{@code interface}、{@code method}、
 * {@code durationMs}、{@code success}，失败时追加 {@code errorType}/{@code errorMessage}。</p>
 *
 * <p><b>默认关闭</b>（与文本版AccessLog二选一，避免重复日志）：
 * {@code -Dconduit.filter.structuredlog.enabled=true} 开启。
 * 不引入logback JSON encoder依赖，按约定字段自行拼接并转义。</p>
 */
public class StructuredLogFilter implements Filter {

    private static final Logger log = LoggerFactory.getLogger("CONDUIT.ACCESS.JSON");

    @Override
    public CompletableFuture<RpcResponse> invoke(RpcRequest request, FilterChain chain) {
        long start = System.nanoTime();
        return chain.invoke(request).whenComplete((resp, err) -> {
            long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
            log.info(format(request, elapsedMs, resp, err));
        });
    }

    @Override
    public int order() {
        return -90; // 在TraceFilter/AccessLog之后，确保拿到traceId
    }

    @Override
    public boolean enabled() {
        return false; // 默认关闭：避免与文本AccessLog重复输出
    }

    @Override
    public String name() {
        return "structuredlog";
    }

    /** 生成一行JSON访问日志（包可见，便于单测） */
    static String format(RpcRequest request, long durationMs, RpcResponse response, Throwable error) {
        Throwable exception = (error != null) ? error
                : (response == null ? null : response.getException());
        boolean success = error == null && (response == null || !response.hasException());

        StringBuilder json = new StringBuilder(256);
        json.append('{')
                .append("\"timestamp\":").append(System.currentTimeMillis()).append(',')
                .append("\"traceId\":\"").append(escape(request.getAttachment(TraceContext.TRACE_ID))).append("\",")
                .append("\"spanId\":\"").append(escape(request.getAttachment(TraceContext.SPAN_ID))).append("\",")
                .append("\"parentSpanId\":\"").append(escape(request.getAttachment(TraceContext.PARENT_SPAN_ID)))
                .append("\",")
                .append("\"interface\":\"").append(escape(request.getInterfaceName())).append("\",")
                .append("\"method\":\"").append(escape(request.getMethodName())).append("\",")
                .append("\"durationMs\":").append(durationMs).append(',')
                .append("\"success\":").append(success);
        if (exception != null) {
            json.append(",\"errorType\":\"").append(escape(exception.getClass().getName())).append('"')
                    .append(",\"errorMessage\":\"").append(escape(exception.getMessage())).append('"');
        }
        return json.append('}').toString();
    }

    /** 最小JSON字符串转义（引号/反斜杠/换行/制表/控制字符） */
    static String escape(String value) {
        if (value == null) {
            return "";
        }
        StringBuilder out = new StringBuilder(value.length() + 8);
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            switch (c) {
                case '"' -> out.append("\\\"");
                case '\\' -> out.append("\\\\");
                case '\n' -> out.append("\\n");
                case '\r' -> out.append("\\r");
                case '\t' -> out.append("\\t");
                default -> {
                    if (c < 0x20) {
                        out.append(String.format("\\u%04x", (int) c));
                    } else {
                        out.append(c);
                    }
                }
            }
        }
        return out.toString();
    }
}
