package io.github.nitouge.conduit.remoting;

import io.github.nitouge.conduit.api.RpcResponse;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 请求-响应匹配的物理基础（{@link PendingRequests}）正确性：
 * 一个requestId只能被完成一次、未知id静默忽略、异常完成能唤醒、并发完成时恰好一个赢家。
 *
 * <p>这是"响应串包/丢唤醒"类问题的第一道防线：上层超时、重连、故障恢复都依赖这里的语义。</p>
 */
class PendingRequestsTest {

    @Test
    void completeWakesExactlyOneWaiter() {
        CompletableFuture<RpcResponse> future = new CompletableFuture<>();
        long requestId = PendingRequests.register(future);
        assertEquals(1, PendingRequests.pendingCount(), "注册后应有1个在途请求");

        PendingRequests.complete(requestId, RpcResponse.success("ok"));

        assertTrue(future.isDone());
        assertEquals("ok", future.join().getData());
        assertEquals(0, PendingRequests.pendingCount(), "完成后应从在途表移除");
    }

    @Test
    void duplicateCompletionDoesNotOverrideFirstResult() {
        CompletableFuture<RpcResponse> future = new CompletableFuture<>();
        long requestId = PendingRequests.register(future);

        PendingRequests.complete(requestId, RpcResponse.success("first"));
        PendingRequests.complete(requestId, RpcResponse.success("second"));

        assertEquals("first", future.join().getData(), "第二次完成不应覆盖第一次结果");
        assertEquals(0, PendingRequests.pendingCount());
    }

    @Test
    void unknownRequestIdIsSilentlyIgnored() {
        PendingRequests.complete(999_999_999L, RpcResponse.success("nobody"));
        PendingRequests.completeExceptionally(999_999_998L, new IllegalStateException("nobody"));

        assertEquals(0, PendingRequests.pendingCount());
    }

    @Test
    void exceptionalCompletionPropagatesToWaiter() {
        CompletableFuture<RpcResponse> future = new CompletableFuture<>();
        long requestId = PendingRequests.register(future);

        IllegalStateException cause = new IllegalStateException("连接断开");
        PendingRequests.completeExceptionally(requestId, cause);

        assertTrue(future.isCompletedExceptionally());
        assertEquals(cause, assertThrowsCause(future));
        assertEquals(0, PendingRequests.pendingCount(), "异常完成也必须清理在途请求，避免泄漏");
    }

    @Test
    void requestIdsAreUniqueAcrossThreads() throws Exception {
        int threads = 8;
        int perThread = 200;
        Set<Long> ids = ConcurrentHashMap.newKeySet();
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        List<Future<?>> tasks = new ArrayList<>();
        try {
            for (int t = 0; t < threads; t++) {
                tasks.add(pool.submit(() -> {
                    start.await();
                    for (int i = 0; i < perThread; i++) {
                        CompletableFuture<RpcResponse> future = new CompletableFuture<>();
                        long id = PendingRequests.register(future);
                        ids.add(id);
                        // 用一个真实异常清理（completeExceptionally(null) 会抛NPE）
                        PendingRequests.completeExceptionally(id, new IllegalStateException("cleanup"));
                    }
                    return null;
                }));
            }
            start.countDown();
            pool.shutdown();
            assertTrue(pool.awaitTermination(10, TimeUnit.SECONDS));
            for (Future<?> task : tasks) {
                task.get(1, TimeUnit.SECONDS); // 线程内异常要暴露出来，而不是被吞掉
            }

            assertEquals(threads * perThread, ids.size(), "并发注册的requestId必须全局唯一");
            assertEquals(0, PendingRequests.pendingCount(), "全部清理后在途数应归零");
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    void concurrentCompletionHasExactlyOneWinner() throws Exception {
        CompletableFuture<RpcResponse> future = new CompletableFuture<>();
        long requestId = PendingRequests.register(future);

        int threads = 8;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        AtomicInteger winners = new AtomicInteger();
        try {
            for (int t = 0; t < threads; t++) {
                String value = "response-" + t;
                pool.submit(() -> {
                    start.await();
                    PendingRequests.complete(requestId, RpcResponse.success(value));
                    if (value.equals(future.join().getData())) {
                        winners.incrementAndGet();
                    }
                    return null;
                });
            }
            start.countDown();
            pool.shutdown();
            assertTrue(pool.awaitTermination(10, TimeUnit.SECONDS));

            assertEquals(1, winners.get(), "并发完成同一个requestId，应只有一个线程的结果成为最终值");
            assertEquals(0, PendingRequests.pendingCount());
            assertNotNull(future.join());
            assertFalse(future.isCompletedExceptionally());
        } finally {
            pool.shutdownNow();
        }
    }

    private static Throwable assertThrowsCause(CompletableFuture<?> future) {
        try {
            future.join();
            throw new AssertionError("期望异常完成");
        } catch (java.util.concurrent.CompletionException e) {
            return e.getCause();
        }
    }
}
