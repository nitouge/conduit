package io.github.nitouge.conduit.common.trace;

import io.github.nitouge.conduit.api.RpcAttachments;
import io.github.nitouge.conduit.api.RpcContext;
import io.github.nitouge.conduit.api.RpcRequest;
import io.github.nitouge.conduit.common.config.ConduitConfig;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.MDC;

import java.util.concurrent.ThreadLocalRandom;

/**
 * 链路追踪上下文工具：traceId / spanId / 父子关系 / 采样，以及 MDC 绑定。
 *
 * <p>约定追踪字段以 attachment 形式放在 {@link RpcRequest#getAttachments()} 中随请求透传：</p>
 *
 * <table border="1">
 *   <caption>attachment 字段</caption>
 *   <tr><th>键</th><th>含义</th></tr>
 *   <tr><td>{@code traceId}</td><td>一条调用链的唯一标识，32 位十六进制；跨进程、跨服务不变</td></tr>
 *   <tr><td>{@code spanId}</td><td>本跳 span 的标识，16 位十六进制；每经过一次远程调用换新</td></tr>
 *   <tr><td>{@code parentSpanId}</td><td>上游 span（调用方发起这次调用的那个 span）；根调用没有</td></tr>
 *   <tr><td>{@code conduit.trace.sampled}</td><td>采样结论 {@code "1"}/{@code "0"}：根服务决定一次，整条链沿用</td></tr>
 * </table>
 *
 * <p><b>span 模型（每跳一个 span）</b>：</p>
 * <pre>
 *   服务A 处理请求(span=S0) ──调用──▶ 服务B 处理该调用(span=S1, parent=S0) ──调用──▶ 服务C(span=S2, parent=S1)
 * </pre>
 * <p>消费侧每次发起远程调用都新建 spanId，父节点取「本线程当前正在服务的 span」；提供侧直接采用请求里的
 * spanId 作为本线程当前 span——于是业务方法里再发起的调用天然接在正确的父节点上。</p>
 *
 * <p><b>采样</b>：根调用按 {@code conduit.trace.sample-rate}（默认 1.0，即全采）对 traceId 做确定性哈希
 * 采样，结论写进 {@code sampled} 沿链透传——同一 traceId 在任何服务上都会得到相同结论，不会出现
 * 「上游采、下游不采」的半截链路。采样只影响 span 日志/上报，traceId 本身始终透传。</p>
 *
 * <p>不依赖任何追踪 SDK；需要接入 OpenTelemetry 等体系时，把 span 的生成/上报替换掉即可。</p>
 */
public final class TraceContext {

    /** attachment与MDC中使用的traceId key（与 api 的约定保持同一份常量，避免两处漂移） */
    public static final String TRACE_ID = RpcAttachments.TRACE_ID;

    /** 本跳spanId的attachment/MDC key */
    public static final String SPAN_ID = RpcAttachments.SPAN_ID;

    /** 上游spanId的attachment/MDC key */
    public static final String PARENT_SPAN_ID = RpcAttachments.PARENT_SPAN_ID;

    /** 采样结论attachment key；值 "1" 采、"0" 不采 */
    public static final String SAMPLED = RpcAttachments.SAMPLED;

    /** 采样率配置键 */
    public static final String SAMPLE_RATE_KEY = "conduit.trace.sample-rate";

    /** 默认采样率：全采（保持既有行为） */
    public static final double DEFAULT_SAMPLE_RATE = 1.0;

    /** 采样桶数量：10000 个桶对应 0.01% 的精度 */
    private static final long SAMPLE_BUCKETS = 10_000L;

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

    /** 当前线程正在服务的spanId（本线程作为提供方处理的那一跳）；不在任何请求里时返回null */
    public static String currentSpanId() {
        String fromMdc = MDC.get(SPAN_ID);
        if (StringUtils.isNotBlank(fromMdc)) {
            return fromMdc;
        }
        return RpcContext.getContext().getAttachment(SPAN_ID);
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

    /**
     * 决定（或沿用）本链路的采样结论：请求已带结论则直接沿用，否则按 traceId 确定性哈希 + 采样率判定，
     * 并写回请求。
     *
     * @return {@code true} 表示本链路被采样
     */
    public static boolean ensureSampled(RpcRequest request) {
        String existing = request == null ? null : request.getAttachment(SAMPLED);
        if ("1".equals(existing)) {
            return true;
        }
        if ("0".equals(existing)) {
            return false;
        }
        boolean sampled = shouldSample(request == null ? null : request.getAttachment(TRACE_ID));
        if (request != null) {
            request.setAttachment(SAMPLED, sampled ? "1" : "0");
        }
        return sampled;
    }

    /** 按 traceId 做确定性采样：同一 traceId 在任何服务上结论一致 */
    public static boolean shouldSample(String traceId) {
        double rate = ConduitConfig.getDouble(SAMPLE_RATE_KEY, DEFAULT_SAMPLE_RATE);
        if (rate >= 1.0) {
            return true;
        }
        if (rate <= 0.0) {
            return false;
        }
        long bucket = bucketOf(traceId);
        return bucket < (long) (rate * SAMPLE_BUCKETS);
    }

    /** 生成32位十六进制traceId（无外部依赖，足够避免单机/跨机碰撞） */
    public static String generateTraceId() {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        return String.format("%016x%016x", random.nextLong(), random.nextLong());
    }

    /** 生成16位十六进制spanId */
    public static String nextSpanId() {
        return String.format("%016x", ThreadLocalRandom.current().nextLong());
    }

    /**
     * 把traceId放入当前线程MDC，返回原来的traceId（无则null），供finally恢复。
     *
     * <p>只是线程本地绑定；异步provider的continuation跑在别的线程，需要显式传播
     * （见 {@code ContextSnapshot}）。</p>
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

    /**
     * 把 trace 上下文（traceId/spanId/parentSpanId）绑定到当前线程 MDC，返回之前的快照。
     *
     * <p>空值会被移除而不是写入 "null"，日志里不会出现 {@code spanId=null} 这种噪音。</p>
     */
    public static MdcSnapshot bindMdc(String traceId, String spanId, String parentSpanId) {
        MdcSnapshot previous = new MdcSnapshot(MDC.get(TRACE_ID), MDC.get(SPAN_ID), MDC.get(PARENT_SPAN_ID));
        putOrRemove(TRACE_ID, traceId);
        putOrRemove(SPAN_ID, spanId);
        putOrRemove(PARENT_SPAN_ID, parentSpanId);
        return previous;
    }

    /** 恢复到 {@link #bindMdc} 之前的 MDC 状态 */
    public static void restoreMdc(MdcSnapshot snapshot) {
        MdcSnapshot previous = snapshot == null ? MdcSnapshot.EMPTY : snapshot;
        putOrRemove(TRACE_ID, previous.traceId());
        putOrRemove(SPAN_ID, previous.spanId());
        putOrRemove(PARENT_SPAN_ID, previous.parentSpanId());
    }

    private static void putOrRemove(String key, String value) {
        if (StringUtils.isBlank(value)) {
            MDC.remove(key);
        } else {
            MDC.put(key, value);
        }
    }

    private static long bucketOf(String traceId) {
        long hash = 0;
        if (traceId != null) {
            for (int i = 0; i < traceId.length(); i++) {
                hash = hash * 31 + traceId.charAt(i);
            }
        }
        // 取模后可能是负数：先归一到 [0, buckets)
        return Math.floorMod(hash, SAMPLE_BUCKETS);
    }

    /** MDC 三元组快照（traceId/spanId/parentSpanId） */
    public record MdcSnapshot(String traceId, String spanId, String parentSpanId) {

        /** 空快照：三项都不存在 */
        public static final MdcSnapshot EMPTY = new MdcSnapshot(null, null, null);
    }
}
