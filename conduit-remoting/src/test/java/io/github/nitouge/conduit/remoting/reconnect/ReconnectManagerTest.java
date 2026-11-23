package io.github.nitouge.conduit.remoting.reconnect;

import io.github.nitouge.conduit.remoting.TestSupport;
import io.netty.channel.Channel;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * 断线重连调度单元测试：退避重试、成功后停止、达到上限后放弃、同一地址单链路。
 */
class ReconnectManagerTest {

    private static final InetSocketAddress ADDRESS = new InetSocketAddress("127.0.0.1", 1);

    @Test
    void retriesUntilSuccessThenStops() {
        AtomicInteger attempts = new AtomicInteger();
        Channel success = new EmbeddedChannel();
        ReconnectManager manager = new ReconnectManager(address -> {
            int attempt = attempts.incrementAndGet();
            if (attempt < 3) {
                return CompletableFuture.failedFuture(new IllegalStateException("connection refused"));
            }
            return CompletableFuture.completedFuture(success);
        }, 5, 20, 100);
        try {
            manager.onDisconnected(ADDRESS);

            TestSupport.awaitTrue("应在退避后重连成功", 3_000, () -> attempts.get() == 3);
            TestSupport.sleepQuietly(150);
            assertEquals(3, attempts.get(), "重连成功后不应继续重试");
        } finally {
            manager.close();
        }
    }

    @Test
    void givesUpAfterMaxAttempts() {
        AtomicInteger attempts = new AtomicInteger();
        ReconnectManager manager = new ReconnectManager(address -> {
            attempts.incrementAndGet();
            return CompletableFuture.failedFuture(new IllegalStateException("still down"));
        }, 2, 10, 50);
        try {
            manager.onDisconnected(ADDRESS);

            TestSupport.awaitTrue("应完成配置的最大重试次数", 3_000, () -> attempts.get() == 2);
            TestSupport.sleepQuietly(150);
            assertEquals(2, attempts.get(), "达到上限后不应继续重试");
        } finally {
            manager.close();
        }
    }

    @Test
    void duplicateDisconnectSignalsProduceSingleRetryChain() {
        AtomicInteger attempts = new AtomicInteger();
        ReconnectManager manager = new ReconnectManager(address -> {
            int attempt = attempts.incrementAndGet();
            if (attempt < 2) {
                return CompletableFuture.failedFuture(new IllegalStateException("down"));
            }
            return CompletableFuture.completedFuture(new EmbeddedChannel());
        }, 5, 10, 50);
        try {
            manager.onDisconnected(ADDRESS);
            manager.onDisconnected(ADDRESS);
            manager.onDisconnected(ADDRESS);

            TestSupport.awaitTrue("应重连成功", 3_000, () -> attempts.get() == 2);
            TestSupport.sleepQuietly(150);
            assertEquals(2, attempts.get(), "同一地址的重复断开信号不应并发多条重连链路");
        } finally {
            manager.close();
        }
    }

    @Test
    void closeStopsScheduledRetries() {
        AtomicInteger attempts = new AtomicInteger();
        ReconnectManager manager = new ReconnectManager(address -> {
            attempts.incrementAndGet();
            return CompletableFuture.failedFuture(new IllegalStateException("down"));
        }, 100, 200, 500);
        manager.onDisconnected(ADDRESS);
        TestSupport.sleepQuietly(50);

        manager.close();
        int attemptsAtClose = attempts.get();
        TestSupport.sleepQuietly(300);

        assertEquals(attemptsAtClose, attempts.get(), "关闭后不应再有任何重连尝试");
    }
}
