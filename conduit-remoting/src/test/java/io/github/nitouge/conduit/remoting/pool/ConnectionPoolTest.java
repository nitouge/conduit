package io.github.nitouge.conduit.remoting.pool;

import io.github.nitouge.conduit.remoting.TestSupport;
import io.netty.channel.Channel;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 连接池单元测试（EmbeddedChannel）：单飞建链、按负载选连接、扩容上限、闲置回收与关闭。
 */
class ConnectionPoolTest {

    private static final InetSocketAddress ADDRESS = new InetSocketAddress("127.0.0.1", 1);

    private final Map<Channel, AtomicInteger> loads = new ConcurrentHashMap<>();
    private ConnectionPool pool;

    @AfterEach
    void tearDown() {
        if (pool != null) {
            pool.close();
        }
    }

    private ConnectionPool pool(int max, int minIdle, long idleTimeout, AtomicInteger connects) {
        this.pool = new ConnectionPool(
                address -> {
                    connects.incrementAndGet();
                    return CompletableFuture.completedFuture(new EmbeddedChannel());
                },
                channel -> loads.getOrDefault(channel, new AtomicInteger()).get(),
                max, minIdle, idleTimeout);
        return pool;
    }

    private int load(Channel channel) {
        return loads.computeIfAbsent(channel, key -> new AtomicInteger()).get();
    }

    private void setLoad(Channel channel, int value) {
        loads.computeIfAbsent(channel, key -> new AtomicInteger()).set(value);
    }

    @Test
    void concurrentAcquiresShareSingleInFlightCreation() throws Exception {
        CompletableFuture<Channel> creation = new CompletableFuture<>();
        AtomicInteger connects = new AtomicInteger();
        pool = new ConnectionPool(address -> {
            connects.incrementAndGet();
            return creation;
        }, channel -> 0, 1, 0, 0);

        CompletableFuture<Channel> first = pool.acquire(ADDRESS);
        CompletableFuture<Channel> second = pool.acquire(ADDRESS);

        assertSame(first, second, "并发建链应共享同一个future");
        assertEquals(1, connects.get(), "同一地址同一时刻只应建链一次");

        Channel channel = new EmbeddedChannel();
        creation.complete(channel);

        assertSame(channel, first.getNow(null));
        assertSame(channel, second.getNow(null));
        assertEquals(1, pool.activeCount(ADDRESS));
    }

    @Test
    void picksLeastLoadedConnectionAndGrowsUpToMax() {
        AtomicInteger connects = new AtomicInteger();
        pool = pool(3, 0, 0, connects);

        Channel first = pool.acquire(ADDRESS).getNow(null);
        setLoad(first, 1);

        // 唯一连接繁忙：本次仍复用它（低延迟），同时后台扩容
        assertSame(first, pool.acquire(ADDRESS).getNow(null));
        TestSupport.awaitTrue("后台应扩容出第二条连接", 3_000, () -> pool.activeCount(ADDRESS) == 2);

        Channel second = pool.acquire(ADDRESS).getNow(null);
        assertNotSame(first, second, "应优先选择负载最小的新连接");
        setLoad(first, 1);
        setLoad(second, 1);

        // 两条都忙：再扩容一条
        pool.acquire(ADDRESS);
        TestSupport.awaitTrue("后台应扩容出第三条连接", 3_000, () -> pool.activeCount(ADDRESS) == 3);

        // 已达上限：无论多忙都不再新建
        for (int i = 0; i < 5; i++) {
            pool.acquire(ADDRESS);
        }
        TestSupport.sleepQuietly(100);
        assertEquals(3, pool.activeCount(ADDRESS));
        assertEquals(3, connects.get(), "连接数不应超过配置上限");
    }

    @Test
    void evictsIdleConnectionsBeyondMinIdle() {
        pool = pool(2, 1, 150, new AtomicInteger());
        Channel channel = pool.acquire(ADDRESS).getNow(null);
        assertTrue(channel.isActive());

        TestSupport.sleepQuietly(700);

        assertEquals(1, pool.activeCount(ADDRESS), "minIdle=1 应保留一条闲置连接");
        assertTrue(channel.isActive());
    }

    @Test
    void evictsAllIdleConnectionsWhenMinIdleIsZero() {
        pool = pool(2, 0, 120, new AtomicInteger());
        Channel channel = pool.acquire(ADDRESS).getNow(null);
        assertTrue(channel.isActive());

        TestSupport.awaitTrue("闲置连接应被回收", 3_000, () -> !channel.isActive());
        assertEquals(0, pool.activeCount(ADDRESS));
    }

    @Test
    void idleEvictionDoesNotTriggerReconnectNotification() {
        AtomicInteger notifications = new AtomicInteger();
        pool = pool(1, 0, 120, new AtomicInteger());
        pool.setConnectionListener(address -> notifications.incrementAndGet());

        Channel channel = pool.acquire(ADDRESS).getNow(null);
        TestSupport.awaitTrue("闲置连接应被回收", 3_000, () -> !channel.isActive());
        TestSupport.sleepQuietly(150);

        assertEquals(0, notifications.get(), "闲置回收是主动行为，不应触发断线重连");
    }

    @Test
    void connectionIsPrunedWhenClosedExternallyAndListenerNotified() {
        AtomicInteger closedNotifications = new AtomicInteger();
        pool = pool(1, 0, 0, new AtomicInteger());
        pool.setConnectionListener(address -> closedNotifications.incrementAndGet());

        Channel channel = pool.acquire(ADDRESS).getNow(null);
        assertEquals(1, pool.activeCount(ADDRESS));

        channel.close();
        TestSupport.awaitTrue("关闭的连接应从池中移除", 3_000, () -> pool.activeCount(ADDRESS) == 0);
        TestSupport.awaitTrue("应通知断线监听器（用于重连）", 1_000, () -> closedNotifications.get() >= 1);

        // 已关闭的连接不应再被复用：再次acquire应新建
        Channel fresh = pool.acquire(ADDRESS).getNow(null);
        assertNotSame(channel, fresh);
        assertEquals(1, pool.activeCount(ADDRESS));
    }

    @Test
    void closeClosesConnectionsAndRejectsFurtherAcquire() {
        pool = pool(1, 0, 0, new AtomicInteger());
        Channel channel = pool.acquire(ADDRESS).getNow(null);
        assertTrue(pool.acquire(ADDRESS).getNow(null).isActive());

        pool.close();

        assertTrue(pool.isClosed());
        assertFalse(channel.isActive(), "关闭连接池应关闭其连接");
        assertEquals(0, pool.totalActiveCount());
        assertTrue(pool.acquire(ADDRESS).isCompletedExceptionally(), "关闭后不应再发放连接");
    }

    @Test
    void failedCreationIsNotRetainedAndRetriesWork() {
        AtomicInteger attempts = new AtomicInteger();
        pool = new ConnectionPool(address -> {
            if (attempts.incrementAndGet() == 1) {
                return CompletableFuture.failedFuture(new IllegalStateException("refused"));
            }
            return CompletableFuture.completedFuture(new EmbeddedChannel());
        }, channel -> load(channel), 1, 0, 0);

        CompletableFuture<Channel> failed = pool.acquire(ADDRESS);
        assertTrue(failed.isCompletedExceptionally());
        assertEquals(0, pool.activeCount(ADDRESS));

        Channel channel = pool.acquire(ADDRESS).getNow(null);
        assertTrue(channel.isActive());
        assertEquals(1, pool.activeCount(ADDRESS));
    }
}
