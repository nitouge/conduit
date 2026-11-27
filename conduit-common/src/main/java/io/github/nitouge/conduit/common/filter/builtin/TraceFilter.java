package io.github.nitouge.conduit.common.filter.builtin;

import io.github.nitouge.conduit.api.RpcRequest;
import io.github.nitouge.conduit.api.RpcResponse;
import io.github.nitouge.conduit.common.filter.Filter;
import io.github.nitouge.conduit.common.filter.FilterChain;
import io.github.nitouge.conduit.common.filter.Side;
import io.github.nitouge.conduit.common.trace.TraceContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * 链路追踪Filter（消费侧）：每次<b>发起远程调用</b>时开一个新的 span，并把它透传给提供侧。
 *
 * <ul>
 *   <li><b>traceId</b>：已有（来自 {@code RpcContext}，即本线程正在服务的请求）则沿用，否则生成——</li>
 *   <li><b>spanId</b>：每次调用都换新的；</li>
 *   <li><b>parentSpanId</b>：取「本线程当前正在服务的 span」。服务A处理请求(span=S0)时再调B，
 *       B 收到的 span 就是 S1、父节点 S0；B 再调 C 就是 S2/S1——父子关系随链路自然延伸；</li>
 *   <li><b>采样</b>：只在根调用上决定一次（按 traceId 确定性哈希 + {@code conduit.trace.sample-rate}），
 *       结论写进 {@code conduit.trace.sampled} 沿链透传，避免半截链路；</li>
 *   <li><b>MDC</b>：调用期间把 traceId/spanId/parentSpanId 绑定到当前线程（日志模板 {@code %X{spanId}} 可直接用）。</li>
 * </ul>
 *
 * <p>提供侧不在Filter里做：{@code RpcRequestHandler} 在请求边界就把这三个字段还原到
 * {@link io.github.nitouge.conduit.api.RpcContext} 与 MDC，业务方法和其它 Filter 都能读到。</p>
 *
 * <p>默认启用；用 {@code -Dconduit.filter.trace.enabled=false} 关闭。
 * 采样只影响 span 日志/上报，traceId 始终透传，不影响调用本身。</p>
 */
public class TraceFilter implements Filter {

    private static final Logger log = LoggerFactory.getLogger(TraceFilter.class);

    @Override
    public CompletableFuture<RpcResponse> invoke(RpcRequest request, FilterChain chain) {
        String traceId = TraceContext.ensureTraceId(request);
        boolean sampled = TraceContext.ensureSampled(request);

        // 本跳 span：父节点取发起这次调用的线程当前正在服务的 span（没有则是根调用）
        String parentSpanId = firstNonBlank(request.getAttachment(TraceContext.SPAN_ID),
                TraceContext.currentSpanId());
        String spanId = TraceContext.nextSpanId();
        request.setAttachment(TraceContext.SPAN_ID, spanId);
        if (parentSpanId != null) {
            request.setAttachment(TraceContext.PARENT_SPAN_ID, parentSpanId);
        } else {
            // 根调用不能继承上游残留的父节点，否则链路里会出现指向不存在父 span 的悬空引用
            request.removeAttachment(TraceContext.PARENT_SPAN_ID);
        }

        TraceContext.MdcSnapshot previous = TraceContext.bindMdc(traceId, spanId, parentSpanId);
        long startNanos = System.nanoTime();
        try {
            CompletableFuture<RpcResponse> future = chain.invoke(request);
            if (sampled) {
                future.whenComplete((response, error) -> logSpan(traceId, spanId, parentSpanId,
                        request, startNanos, response, error));
            }
            return future;
        } finally {
            // 同步前置/终端已执行完；异步回调不依赖MDC（AccessLog/StructuredLog直接读request）
            TraceContext.restoreMdc(previous);
        }
    }

    /** span 结束日志：只在被采样时输出，字段与 MDC 键名保持一致，便于按 spanId 串日志 */
    private static void logSpan(String traceId, String spanId, String parentSpanId, RpcRequest request,
                                long startNanos, RpcResponse response, Throwable error) {
        if (!log.isDebugEnabled()) {
            return;
        }
        long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
        boolean success = error == null && (response == null || !response.hasException());
        log.debug("[Trace] span结束 traceId={} spanId={} parentSpanId={} {}#{} 耗时{}ms {}",
                traceId, spanId, parentSpanId,
                request.getInterfaceName(), request.getMethodName(), elapsedMs, success ? "OK" : "FAIL");
    }

    @Override
    public int order() {
        return -200; // 最外层：先于AccessLog等生成/绑定traceId与spanId
    }

    @Override
    public boolean appliesTo(Side side) {
        // 提供侧的上下文还原在 RpcRequestHandler 的请求边界完成（Filter 链之前），这里只负责发起侧
        return side == Side.CONSUMER;
    }

    @Override
    public String name() {
        return "trace";
    }

    private static String firstNonBlank(String first, String second) {
        if (first != null && !first.isBlank()) {
            return first;
        }
        return second != null && !second.isBlank() ? second : null;
    }
}
