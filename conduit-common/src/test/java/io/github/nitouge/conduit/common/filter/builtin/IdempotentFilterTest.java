package io.github.nitouge.conduit.common.filter.builtin;

import io.github.nitouge.conduit.api.RpcAttachments;
import io.github.nitouge.conduit.api.RpcRequest;
import io.github.nitouge.conduit.api.RpcResponse;
import io.github.nitouge.conduit.common.filter.FilterChain;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * 幂等去重Filter测试：同键只执行一次、并发重复共享结果、失败不入缓存、无键直通。
 */
class IdempotentFilterTest {

    private final IdempotentFilter filter = new IdempotentFilter();

    @Test
    void sameKeyExecutesOnceAndReturnsCachedResult() {
        CountingTerminal terminal = new CountingTerminal();

        RpcResponse first = filter.invoke(request("k-1"), terminal).join();
        RpcResponse second = filter.invoke(request("k-1"), terminal).join();

        assertEquals("executed", first.getData());
        assertEquals("executed", second.getData());
        assertEquals(1, terminal.executions.get(), "同一幂等键只能真正执行一次");
    }

    @Test
    void differentKeysExecuteIndependently() {
        CountingTerminal terminal = new CountingTerminal();

        filter.invoke(request("k-1"), terminal).join();
        filter.invoke(request("k-2"), terminal).join();

        assertEquals(2, terminal.executions.get());
    }

    @Test
    void concurrentDuplicateWaitsOnTheFirstExecution() throws Exception {
        CountDownLatch release = new CountDownLatch(1);
        AtomicInteger executions = new AtomicInteger();
        FilterChain slow = request -> {
            executions.incrementAndGet();
            return CompletableFuture.supplyAsync(() -> {
                await(release);
                return RpcResponse.success("executed");
            });
        };

        CompletableFuture<RpcResponse> first = filter.invoke(request("k-concurrent"), slow);
        // 首次执行还没结束，第二个同键请求应挂到同一个 future 上，而不是再执行一次
        CompletableFuture<RpcResponse> second = filter.invoke(request("k-concurrent"), slow);
        assertSame(first, second, "重复请求应共享第一次的future");

        release.countDown();
        assertEquals("executed", first.get(2, TimeUnit.SECONDS).getData());
        assertEquals("executed", second.get(2, TimeUnit.SECONDS).getData());
        assertEquals(1, executions.get(), "并发重复不应双重执行");
    }

    @Test
    void failureIsNotCachedSoRetryCanReallyRetry() {
        AtomicInteger executions = new AtomicInteger();
        FilterChain failing = request -> {
            executions.incrementAndGet();
            CompletableFuture<RpcResponse> failed = new CompletableFuture<>();
            failed.completeExceptionally(new IllegalStateException("boom"));
            return failed;
        };

        assertThrows(ExecutionException.class, () -> filter.invoke(request("k-fail"), failing).get());
        assertThrows(ExecutionException.class, () -> filter.invoke(request("k-fail"), failing).get());

        assertEquals(2, executions.get(), "失败的键必须可以被重试，而不是一直返回同一个失败");
    }

    @Test
    void requestWithoutKeyPassesThrough() {
        CountingTerminal terminal = new CountingTerminal();
        RpcRequest request = request(null);

        filter.invoke(request, terminal).join();
        filter.invoke(request, terminal).join();

        assertEquals(2, terminal.executions.get(), "未携带幂等键时不应做任何去重");
    }

    @Test
    void keyIsScopedToMethod() {
        CountingTerminal terminal = new CountingTerminal();

        filter.invoke(request("k-scope"), terminal).join();
        RpcRequest otherMethod = request("k-scope");
        otherMethod.setMethodName("other");
        filter.invoke(otherMethod, terminal).join();

        assertEquals(2, terminal.executions.get(), "不同方法上的同名键不应互相命中");
    }

    private static RpcRequest request(String key) {
        RpcRequest request = new RpcRequest();
        request.setInterfaceName("com.example.OrderService");
        request.setMethodName("create");
        if (key != null) {
            request.setAttachment(RpcAttachments.IDEMPOTENCY_KEY, key);
        }
        return request;
    }

    private static void await(CountDownLatch latch) {
        try {
            latch.await(3, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError("等待被中断", e);
        }
    }

    /** 记录真正进入业务终端的次数 */
    private static final class CountingTerminal implements FilterChain {

        private final AtomicInteger executions = new AtomicInteger();

        @Override
        public CompletableFuture<RpcResponse> invoke(RpcRequest request) {
            executions.incrementAndGet();
            return CompletableFuture.completedFuture(RpcResponse.success("executed"));
        }
    }
}
