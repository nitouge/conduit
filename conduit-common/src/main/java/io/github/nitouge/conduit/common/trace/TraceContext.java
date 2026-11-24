package io.github.nitouge.conduit.common.trace;

import io.github.nitouge.conduit.api.RpcContext;
import io.github.nitouge.conduit.api.RpcRequest;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.MDC;

import java.util.concurrent.ThreadLocalRandom;

/**
 * 链路追踪上下文工具：traceId的生成、透传、MDC绑定。
 *
 * <p>约定traceId以 attachment形式放在 {@link RpcRequest#getAttachments()} 中随请求透传：
 * 消费侧由 {@code TraceFilter} 生成/沿用，提供侧从请求中取出、写入 {@link RpcContext} 与MDC，
 * 业务方法可通过 {@code RpcContext.getContext().getTraceId()} 读取。</p>
 *
 * <p>不依赖任何追踪SDK；需要接入OpenTelemetry等体系时，把traceId的生成/上报替换掉即可。</p>
 */
public final class TraceContext {

    /** attachment与MDC中使用的traceId key */
    public static final String TRACE_ID = "traceId";

    private TraceContext() {
    }

    /** 当前上下文的traceId：优先MDC（日志/业务线程），其次RpcContext；都没有返回null */
    public static String currentTraceId() {
        String fromMdc = MDC.get(TRACE_ID);
        if (StringUtils.isNotBlank(fromMdc)) {
            return fromMdc;
        }
        return RpcContext.getContext().getAttachment(TRACE_ID);
    }

    /** 确保请求携带traceId：已有则沿用，没有则生成并写入attachments */
    public static String ensureTraceId(RpcRequest request) {
        if (request == null) {
            return generateTraceId();
        }
        String traceId = request.getAttachment(TRACE_ID);
        if (StringUtils.isBlank(traceId)) {
            traceId = generateTraceId();
            request.setAttachment(TRACE_ID, traceId);
        }
        return traceId;
    }

    /** 生成32位十六进制traceId（无外部依赖，足够避免单机/跨机碰撞） */
    public static String generateTraceId() {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        return String.format("%016x%016x", random.nextLong(), random.nextLong());
    }

    /**
     * 把traceId放入当前线程MDC，返回原来的traceId（无则null），供finally恢复。
     *
     * <p>只是线程本地绑定；异步provider的continuation跑在别的线程，需要业务自行拷贝
     * {@code RpcContext}（与既有attachments的异步限制一致）。</p>
     */
    public static String putMdc(String traceId) {
        String previous = MDC.get(TRACE_ID);
        MDC.put(TRACE_ID, traceId);
        return previous;
    }

    /** 恢复MDC中的traceId */
    public static void restoreMdc(String previousTraceId) {
        if (previousTraceId == null) {
            MDC.remove(TRACE_ID);
        } else {
            MDC.put(TRACE_ID, previousTraceId);
        }
    }
}
