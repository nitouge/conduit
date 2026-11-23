package io.github.nitouge.conduit.remoting.netty;

import io.github.nitouge.conduit.api.RpcResponse;
import io.github.nitouge.conduit.remoting.ClientOptions;
import io.github.nitouge.conduit.remoting.RequestHandler;
import io.github.nitouge.conduit.remoting.TestSupport;
import io.netty.channel.Channel;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 断线自动重连集成测试（真实socket）：provider重启后无新调用也能恢复；在途请求在连接断开时快速失败。
 */
class ReconnectIntegrationTest {

    @Test
    void reconnectsInBackgroundAfterProviderRestart() throws Exception {
        int port = TestSupport.freePort();
        NettyServer first = startServerWithRetry(port, TestSupport.echoHandler());

        NettyClient client = new NettyClient(ClientOptions.builder()
                .reconnectEnabled(true)
                .reconnectMaxAttempts(10)
                .reconnectBaseDelayMillis(100)
                .reconnectMaxDelayMillis(500)
                .heartbeatIntervalMillis(200)
                .readIdleTimeoutMillis(1_000)
                .build());
        try {
            assertEquals("ok:ping", TestSupport.call(client, port, "ping").getData());
            assertEquals(1, client.activeConnections(TestSupport.HOST, port));

            first.close();
            TestSupport.awaitTrue("服务端关闭后连接应被移除",
                    3_000, () -> client.activeConnections(TestSupport.HOST, port) == 0);

            NettyServer second = startServerWithRetry(port, TestSupport.echoHandler());
            try {
                // 关键：不发任何新调用，仅靠后台重连恢复连接
                TestSupport.awaitTrue("provider恢复后应后台重连成功（无需新调用）",
                        6_000, () -> client.activeConnections(TestSupport.HOST, port) == 1);
                assertEquals("ok:ping", TestSupport.call(client, port, "ping").getData());
            } finally {
                second.close();
            }
        } finally {
            client.close();
        }
    }

    @Test
    void inFlightRequestsFailFastWhenConnectionDrops() throws Exception {
        int port = TestSupport.freePort();
        NettyServer server = startServerWithRetry(port, TestSupport.slowHandler(1_000));
        NettyClient client = new NettyClient(ClientOptions.builder().reconnectEnabled(false).build());
        try {
            Channel channel = client.getChannel(new InetSocketAddress(TestSupport.HOST, port));
            CompletableFuture<RpcResponse> future = client.send(TestSupport.request("slow"),
                    TestSupport.target(port), TestSupport.CALL_TIMEOUT_MILLIS);
            TestSupport.sleepQuietly(300); // 等请求真正写出并进入在途集合

            long start = System.nanoTime();
            channel.close(); // 模拟连接突然断开
            ExecutionException failure = assertThrows(ExecutionException.class,
                    () -> future.get(2, TimeUnit.SECONDS));
            long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);

            assertInstanceOf(RuntimeException.class, failure.getCause());
            assertTrue(elapsedMillis < 2_000, "在途请求应立刻失败，而不是干等到调用超时");
            assertEquals(0, client.pendingRequestCount(), "失败后不应残留在途请求");
        } finally {
            client.close();
            server.close();
        }
    }

    /** 端口刚释放时可能短暂不可绑定：重试几次，保证测试稳定 */
    private static NettyServer startServerWithRetry(int port, RequestHandler handler) {
        RuntimeException last = null;
        for (int i = 0; i < 30; i++) {
            try {
                NettyServer server = new NettyServer(TestSupport.HOST, port, handler);
                server.start();
                return server;
            } catch (RuntimeException e) {
                last = e;
                TestSupport.sleepQuietly(100);
            }
        }
        throw new AssertionError("无法在端口 " + port + " 启动服务端", last);
    }
}
