package io.github.nitouge.conduit.api;

/**
 * 跨进程透传的 attachment 键约定与少量工具方法。
 *
 * <p>{@link RpcRequest#getAttachments()} 是一条随请求跨进程流动的 K-V 通道。它不污染协议头、
 * 天然兼容所有序列化器，适合承载「横切能力」的参数：截止时间、灰度标签、同机房偏好等。
 * 键散落在各处字符串字面量里最容易拼错、也最难统一改名，故集中在本类。</p>
 *
 * <p>消费侧通常通过 {@link RpcContext} 写入这些键（{@code RpcContext.getContext().setAttachment(...)}），
 * 由 {@code RpcInvocationHandler} 拷进 {@link RpcRequest}；提供侧由 {@code RpcRequestHandler}
 * 还原回 {@link RpcContext}，业务方法与 Filter 都能读到。</p>
 */
public final class RpcAttachments {

    /**
     * 请求截止时间（绝对时间，epoch 毫秒）。
     *
     * <p>语义是「这个请求最晚在什么时刻之前还值得执行」而不是「还能等多久」：绝对时间才能
     * 在跨进程、跨重试、跨节点转发时保持单调——每一跳都用 {@code min(上游deadline, now+本地超时)}
     * 收敛，上游即使不等待了，下游也不会继续白跑。</p>
     */
    public static final String DEADLINE = "conduit.deadline";

    /** 灰度/标签路由：本次调用期望的提供者标签（覆盖 {@code @RpcReference} 的静态默认值） */
    public static final String TAG = "conduit.tag";

    /** 同机房优先：调用方所在机房/可用区，用于「同机房优先、跨机房兜底」的软路由 */
    public static final String ZONE = "conduit.zone";

    /** 版本覆盖：单次调用临时改写路由版本（默认取自 {@code @RpcReference(version=...)}） */
    public static final String VERSION = "conduit.version";

    /** 分组覆盖：单次调用临时改写路由分组 */
    public static final String GROUP = "conduit.group";

    /** traceId（历史键名保持 {@code traceId} 不变，避免破坏已有日志采集规则） */
    public static final String TRACE_ID = "traceId";

    /** 本跳 spanId：每经过一次远程调用换新；同时作为 MDC key 使用 */
    public static final String SPAN_ID = "spanId";

    /** 上游 spanId（发起这次调用的那个 span）；根调用没有；同时作为 MDC key 使用 */
    public static final String PARENT_SPAN_ID = "parentSpanId";

    /** 采样结论：{@code "1"} 采、{@code "0"} 不采；根调用决定一次后沿链透传 */
    public static final String SAMPLED = "conduit.trace.sampled";

    /**
     * 幂等键：同一次<b>逻辑调用</b>共用一个键，服务端据此去重。
     *
     * <p>关键在「逻辑调用」而不是「一次尝试」：failover 重放的是同一个请求对象，键不变；调用方
     * 两次独立的业务操作会各自拿到新键。消费侧默认自动生成（{@code IdempotencyFilter}），也可以
     * 自己指定（如用订单号）来实现业务级幂等。</p>
     */
    public static final String IDEMPOTENCY_KEY = "conduit.idempotency-key";

    private RpcAttachments() {
    }

    /**
     * 读取请求的截止时间。
     *
     * @return epoch 毫秒；未携带或值非法返回 {@code 0}
     */
    public static long deadlineOf(RpcRequest request) {
        if (request == null) {
            return 0L;
        }
        return parsePositiveLong(request.getAttachment(DEADLINE));
    }

    /** 写入截止时间（{@code <=0} 视为「不设置」，避免写出无意义的 0） */
    public static void setDeadline(RpcRequest request, long deadlineEpochMillis) {
        if (request != null && deadlineEpochMillis > 0) {
            request.setAttachment(DEADLINE, Long.toString(deadlineEpochMillis));
        }
    }

    /**
     * 生成本次调用的截止时间并写入请求：{@code min(上游已带deadline, now + 本地超时)}。
     *
     * <p>「取更早的那个」是这条链路的关键：A→B→C 时，B 既不能比 A 等得久（A 已经不等了，
     * B 还在等就是白跑），也不该比自己的超时更久。第一次发起调用时上游没有 deadline，
     * 直接用 {@code now + timeout}。</p>
     *
     * @param timeoutMillis 本地配置的调用超时（{@code <=0} 表示不设本地上限）
     * @param nowMillis     当前时间，便于测试注入
     * @return 生效的截止时间；两处都没给时返回 {@code 0}（不写 attachment）
     */
    public static long applyDeadline(RpcRequest request, long timeoutMillis, long nowMillis) {
        long inherited = deadlineOf(request);
        long own = timeoutMillis > 0 ? saturatedAdd(nowMillis, timeoutMillis) : 0L;
        long deadline;
        if (inherited > 0 && own > 0) {
            deadline = Math.min(inherited, own);
        } else {
            deadline = inherited > 0 ? inherited : own;
        }
        setDeadline(request, deadline);
        return deadline;
    }

    /**
     * 计算本次（或本次重试）可用的超时预算。
     *
     * <p>有 deadline 时取 {@code min(配置超时, deadline-now)}：剩余预算到点就只给 1ms，
     * 让传输层立刻超时并下发 CANCEL，而不是按原配置傻等——这正是「不白跑」在客户端一侧的落地。</p>
     */
    public static long budgetMillis(RpcRequest request, long configuredTimeoutMillis, long nowMillis) {
        long configured = configuredTimeoutMillis > 0 ? configuredTimeoutMillis : 1L;
        long deadline = deadlineOf(request);
        if (deadline <= 0) {
            return configured;
        }
        long remaining = deadline - nowMillis;
        return remaining > 0 ? Math.min(configured, remaining) : 1L;
    }

    private static long saturatedAdd(long a, long b) {
        long sum = a + b;
        return ((a ^ sum) & (b ^ sum)) < 0 ? Long.MAX_VALUE : sum;
    }

    private static long parsePositiveLong(String value) {
        if (value == null || value.isBlank()) {
            return 0L;
        }
        try {
            long parsed = Long.parseLong(value.trim());
            return parsed > 0 ? parsed : 0L;
        } catch (NumberFormatException e) {
            return 0L;
        }
    }
}
