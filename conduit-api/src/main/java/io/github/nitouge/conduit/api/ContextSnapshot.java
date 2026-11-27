package io.github.nitouge.conduit.api;

import org.slf4j.MDC;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * 调用上下文的跨线程快照：把 {@link RpcContext} 的 attachments（含 traceId/spanId）与同名 MDC 字段
 * 搬到另一个线程执行时使用。
 *
 * <p>{@link RpcContext} 基于 {@link ThreadLocal}——异步 provider 的 continuation、业务自建的线程池、
 * {@code CompletableFuture} 回调都跑在别的线程，默认读不到发起方写下的上下文。要跨线程保留链路信息，
 * 必须显式传播：</p>
 *
 * <pre>{@code
 * ContextSnapshot snapshot = ContextSnapshot.capture();      // 在发起线程捕获（含traceId/spanId）
 * executor.execute(snapshot.wrap(() -> {
 *     // 这里 RpcContext.getContext().getTraceId() 与 MDC 都能拿到值
 *     orderService.create(...);                              // 下游也能接着这条链路
 * }));
 * }</pre>
 *
 * <p>语义是「<b>在快照上下文下执行</b>」而不是「覆盖全局」：任务结束后恢复执行线程原来的上下文，
 * 所以复用一个线程池不会把上下文泄漏给下一个任务。</p>
 */
public final class ContextSnapshot {

    /** 需要随快照一起传播的 MDC 键：与 attachments 同名，日志模板可直接使用 */
    private static final String[] MDC_KEYS = {
            RpcAttachments.TRACE_ID, RpcAttachments.SPAN_ID, RpcAttachments.PARENT_SPAN_ID
    };

    private final Map<String, String> attachments;
    private final Map<String, String> mdc;

    private ContextSnapshot(Map<String, String> attachments, Map<String, String> mdc) {
        this.attachments = attachments;
        this.mdc = mdc;
    }

    /** 捕获当前线程的上下文（attachments + MDC 中的 trace 字段） */
    public static ContextSnapshot capture() {
        return new ContextSnapshot(new HashMap<>(RpcContext.getContext().getAttachments()), captureMdc());
    }

    /** 用给定 attachments 构造快照（不做 MDC 传播） */
    public static ContextSnapshot of(Map<String, String> attachments) {
        return new ContextSnapshot(attachments == null ? new HashMap<>() : new HashMap<>(attachments), Map.of());
    }

    /** 在快照上下文下执行任务 */
    public void run(Runnable task) {
        inScope(() -> {
            task.run();
            return null;
        });
    }

    /** 在快照上下文下执行带返回值的任务 */
    public <T> T call(Callable<T> task) throws Exception {
        RpcContext previous = RpcContext.removeContext();
        Map<String, String> previousMdc = captureMdc();
        try {
            RpcContext.getContext().setAttachments(attachments);
            applyMdc(mdc);
            return task.call();
        } finally {
            RpcContext.attach(previous);
            applyMdc(previousMdc);
        }
    }

    /** 包装成 Runnable：交给任意线程池执行时自动带上快照上下文 */
    public Runnable wrap(Runnable task) {
        return () -> run(task);
    }

    /** 包装成 Supplier（如 {@code CompletableFuture.supplyAsync}） */
    public <T> Supplier<T> wrap(Supplier<T> task) {
        return () -> inScope(task);
    }

    /** 包装成 Function（如 {@code CompletableFuture.thenApplyAsync} 的映射函数） */
    public <T, R> Function<T, R> wrap(Function<T, R> fn) {
        return value -> inScope(() -> fn.apply(value));
    }

    /** 包装成 BiConsumer（如 {@code CompletableFuture.whenCompleteAsync} 的回调） */
    public <T, U> BiConsumer<T, U> wrap(BiConsumer<T, U> consumer) {
        return (first, second) -> inScope(() -> {
            consumer.accept(first, second);
            return null;
        });
    }

    /** 统一的"进入快照 → 执行 → 恢复"模板 */
    private <R> R inScope(Supplier<R> action) {
        RpcContext previous = RpcContext.removeContext();
        Map<String, String> previousMdc = captureMdc();
        try {
            RpcContext.getContext().setAttachments(attachments);
            applyMdc(mdc);
            return action.get();
        } finally {
            RpcContext.attach(previous);
            applyMdc(previousMdc);
        }
    }

    /** 当前线程的 trace 相关 MDC 键值（用于恢复） */
    private static Map<String, String> captureMdc() {
        Map<String, String> current = new HashMap<>();
        for (String key : MDC_KEYS) {
            String value = MDC.get(key);
            if (value != null) {
                current.put(key, value);
            }
        }
        return current;
    }

    private static void applyMdc(Map<String, String> values) {
        for (String key : MDC_KEYS) {
            String value = values.get(key);
            if (value == null) {
                MDC.remove(key);
            } else {
                MDC.put(key, value);
            }
        }
    }
}
