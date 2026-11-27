package io.github.nitouge.conduit.api;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.slf4j.MDC;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * 上下文跨线程快照：搬过去、用完还原，且不把上下文泄漏给线程池里的下一个任务。
 */
class ContextSnapshotTest {

    private final ExecutorService pool = Executors.newSingleThreadExecutor();

    @AfterEach
    void tearDown() {
        pool.shutdownNow();
        RpcContext.removeContext();
        MDC.remove(RpcAttachments.TRACE_ID);
        MDC.remove(RpcAttachments.SPAN_ID);
    }

    @Test
    void wrapsTaskWithCapturedContextAndMdc() throws Exception {
        RpcContext.getContext().setAttachment(RpcAttachments.TRACE_ID, "trace-1");
        RpcContext.getContext().setAttachment(RpcAttachments.SPAN_ID, "span-1");
        MDC.put(RpcAttachments.TRACE_ID, "trace-1");
        MDC.put(RpcAttachments.SPAN_ID, "span-1");

        ContextSnapshot snapshot = ContextSnapshot.capture();
        RpcContext.removeContext(); // 发起线程的上下文被消耗掉（真实调用边界就是这样）

        String seen = CompletableFuture.supplyAsync(snapshot.wrap(() ->
                        RpcContext.getContext().getTraceId()
                                + "/" + MDC.get(RpcAttachments.TRACE_ID)
                                + "/" + MDC.get(RpcAttachments.SPAN_ID)), pool)
                .get(2, TimeUnit.SECONDS);

        assertEquals("trace-1/trace-1/span-1", seen, "attachments与MDC都应能在另一个线程读到");
    }

    @Test
    void restoresPoolThreadPreviousContextAfterRun() throws Exception {
        // 池线程先有个自己的上下文
        pool.submit(() -> RpcContext.getContext().setAttachment("tenant", "pool-own")).get(2, TimeUnit.SECONDS);

        RpcContext.getContext().setAttachment("tenant", "captured");
        ContextSnapshot snapshot = ContextSnapshot.capture();
        RpcContext.removeContext();

        assertEquals("captured", CompletableFuture.supplyAsync(snapshot.wrap(
                () -> RpcContext.getContext().getAttachment("tenant")), pool).get(2, TimeUnit.SECONDS));

        // 任务结束后，池线程恢复成它原来的上下文，不会把快照内容泄漏给下一个任务
        assertEquals("pool-own", CompletableFuture.supplyAsync(
                () -> RpcContext.getContext().getAttachment("tenant"), pool).get(2, TimeUnit.SECONDS));
    }

    @Test
    void emptySnapshotRunsWithNoAttachments() throws Exception {
        ContextSnapshot snapshot = ContextSnapshot.of(Map.of());
        RpcContext.getContext().setAttachment("leftover", "x");

        assertNull(CompletableFuture.supplyAsync(snapshot.wrap(
                        () -> RpcContext.getContext().getAttachment("leftover")), pool).get(2, TimeUnit.SECONDS),
                "快照执行期间应看不到发起线程的attachments");
        assertEquals("x", RpcContext.getContext().getAttachment("leftover"), "执行后发起线程不受影响");
    }
}
