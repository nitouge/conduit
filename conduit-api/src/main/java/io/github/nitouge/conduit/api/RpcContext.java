package io.github.nitouge.conduit.api;

import java.util.HashMap;
import java.util.Map;

/**
 * 调用线程内的隐式上下文：{@link RpcRequest#getAttachments} 的当前线程视图。
 *
 * <p><b>用途：</b>让消费侧在调用前 {@code setAttachment("traceId", x)}，提供侧在业务方法体里
 * {@code RpcContext.getContext().getAttachment("traceId")} 就能读到——无需把traceId作为方法
 * 显式参数串在所有业务接口上。配套 {@code Filter} 链路可在此处做上下文透传/指标埋点。</p>
 *
 * <p><b>线程语义：</b>基于 {@link ThreadLocal}：消费侧在调用线程写，提供侧在业务线程写，
 * 两者不共线程、无冲突，但都需要「调用边界清零」避免泄漏——消费侧在
 * {@code RpcInvocationHandler} 拷进 {@link RpcRequest} 后立即 {@link #removeContext()}，
 * 提供侧在 {@code RpcRequestHandler} 业务反射调用后（{@code finally}）{@link #removeContext()}。</p>
 *
 * <p><b>局限：</b>异步provider方法的continuation跑在另一个线程里，{@code ThreadLocal}
 * 不自动传播——属固有限制，与H12（异步绕过熔断）同类取舍，需要时改用显式 {@code CompletableFuture}
 * 参数透传或专门做 {@code ContextSnapshot}，不在本类职责内。</p>
 */
public final class RpcContext {

    private static final ThreadLocal<RpcContext> CONTEXT = new ThreadLocal<>();

    private final Map<String, String> attachments = new HashMap<>();

    private RpcContext() {
    }

    /** 获取当前线程的上下文；不存在则懒创建一个（不写回ThreadLocal）。 */
    public static RpcContext getContext() {
        RpcContext ctx = CONTEXT.get();
        if (ctx == null) {
            ctx = new RpcContext();
            CONTEXT.set(ctx);
        }
        return ctx;
    }

    /**
     * 取出并清空当前线程的上下文。
     *
     * <p>在调用边界必须调用——否则下一次同线程调用会读到上次残留的attachments。</p>
     */
    public static RpcContext removeContext() {
        RpcContext ctx = CONTEXT.get();
        CONTEXT.remove();
        return ctx;
    }

    /** 写入附加参数 */
    public void setAttachment(String key, String value) {
        if (key != null) {
            attachments.put(key, value);
        }
    }

    /** 读取附加参数；未设置返回 {@code null} */
    public String getAttachment(String key) {
        return attachments.get(key);
    }

    /** 全量attachments（内部Map的实时引用，调用方应只读） */
    public Map<String, String> getAttachments() {
        return attachments;
    }

    /** 用已有attachments覆盖当前（用于提供侧从请求还原到上下文） */
    public void setAttachments(Map<String, String> source) {
        attachments.clear();
        if (source != null) {
            attachments.putAll(source);
        }
    }

    /** 当前调用的traceId（attachment key="traceId"）；未设置返回null */
    public String getTraceId() {
        return attachments.get("traceId");
    }

    /** 设置当前调用的traceId（会随请求透传到提供侧，并被TraceFilter沿用） */
    public void setTraceId(String traceId) {
        setAttachment("traceId", traceId);
    }
}
