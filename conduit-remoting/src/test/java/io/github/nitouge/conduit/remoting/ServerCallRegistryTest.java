package io.github.nitouge.conduit.remoting;

import io.github.nitouge.conduit.protocol.ProtocolConstants;
import io.netty.channel.ChannelId;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 在途调用表的键语义测试：requestId 只有「连接内唯一」，跨连接的同名 id 绝不能互相取消。
 */
class ServerCallRegistryTest {

    @AfterEach
    void clear() {
        ServerCallRegistry.reset();
    }

    @Test
    void cancelOnlyHitsTheSameConnection() {
        // 默认的 EmbeddedChannel 共用同一个 channelId，这里必须给每条连接不同的身份
        EmbeddedChannel first = new EmbeddedChannel(channelId("conn-a"));
        EmbeddedChannel second = new EmbeddedChannel(channelId("conn-b"));
        try {
            ServerCallRegistry.Call callOnFirst = ServerCallRegistry.register(first, 7L);
            ServerCallRegistry.Call callOnSecond = ServerCallRegistry.register(second, 7L);
            assertEquals(2, ServerCallRegistry.activeCount(), "两条连接各自的在途调用都应登记");

            assertTrue(ServerCallRegistry.cancel(first, 7L, ProtocolConstants.CANCEL_CLIENT_TIMEOUT));

            assertTrue(callOnFirst.isCancelled());
            assertEquals(ProtocolConstants.CANCEL_CLIENT_TIMEOUT, callOnFirst.reason());
            assertFalse(callOnSecond.isCancelled(), "同一 requestId 的另一条连接不应被误取消");
            assertEquals(1, ServerCallRegistry.activeCount());
        } finally {
            first.close();
            second.close();
        }
    }

    @Test
    void cancelOfUnknownRequestIsNoOp() {
        EmbeddedChannel channel = new EmbeddedChannel();
        try {
            assertFalse(ServerCallRegistry.cancel(channel, 999L, ProtocolConstants.CANCEL_CALLER_ABANDONED),
                    "未登记过的请求（已返回或从未到达）应返回 false，而不是抛异常");
        } finally {
            channel.close();
        }
    }

    @Test
    void expiredDeadlineCancelsImmediately() {
        EmbeddedChannel channel = new EmbeddedChannel();
        try {
            ServerCallRegistry.Call call = ServerCallRegistry.register(channel, 1L);

            ServerCallRegistry.scheduleDeadline(call, System.currentTimeMillis() - 1);

            assertTrue(call.isCancelled(), "已过期的 deadline 应立刻取消");
            assertEquals(ProtocolConstants.CANCEL_DEADLINE_EXCEEDED, call.reason());
        } finally {
            channel.close();
        }
    }

    @Test
    void unregisterIsIdempotent() {
        EmbeddedChannel channel = new EmbeddedChannel();
        try {
            ServerCallRegistry.Call call = ServerCallRegistry.register(channel, 2L);
            ServerCallRegistry.unregister(call);
            ServerCallRegistry.unregister(call);
            assertEquals(0, ServerCallRegistry.activeCount());
            assertFalse(call.isCancelled(), "正常完成不算取消");
        } finally {
            channel.close();
        }
    }

    @Test
    void attachWorkerAfterCancelInterruptsImmediately() throws Exception {
        EmbeddedChannel channel = new EmbeddedChannel();
        try {
            ServerCallRegistry.Call call = ServerCallRegistry.register(channel, 3L);
            ServerCallRegistry.cancel(channel, 3L, ProtocolConstants.CANCEL_CLIENT_TIMEOUT);

            // 取消比业务线程启动更早到达时，setThread 就应该立刻打断它，而不是让整个方法跑完
            Thread worker = Thread.currentThread();
            ServerCallRegistry.attachWorker(call, worker);
            assertTrue(worker.isInterrupted(), "启动瞬间已被取消的请求应立即拿到中断标记");
            assertTrue(Thread.interrupted(), "清掉标记，避免影响当前测试后续代码");
        } finally {
            channel.close();
        }
    }

    /** 测试专用连接身份：内容相同即相等，便于断言「同 id 不同连接互不影响」 */
    private static ChannelId channelId(String name) {
        return new ChannelId() {
            @Override
            public String asShortText() {
                return name;
            }

            @Override
            public String asLongText() {
                return name;
            }

            @Override
            public int compareTo(ChannelId other) {
                return name.compareTo(other == null ? "" : other.asLongText());
            }
        };
    }
}
