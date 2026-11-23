package io.github.nitouge.conduit.remoting.netty;

import io.github.nitouge.conduit.api.RpcResponse;
import io.github.nitouge.conduit.remoting.ClientOptions;
import io.github.nitouge.conduit.remoting.TestSupport;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 连接池集成测试（真实socket）：默认单连接复用、按负载扩容到上限、闲置回收。
 */
class ConnectionPoolIntegrationTest {

    @Test
    void defaultsToSingleSharedConnection() throws Exception {
        int port = TestSupport.freePort();
        NettyServer server = new NettyServer(TestSupport.HOST, port, TestSupport.slowHandler(300));
        server.start();

        NettyClient client = new NettyClient(ClientOptions.builder().reconnectEnabled(false).build());
        try {
            List<CompletableFuture<RpcResponse>> futures = new ArrayList<>();
            for (int i = 0; i < 6; i++) {
                futures.add(client.send(TestSupport.request("slow"), TestSupport.target(port),
                        TestSupport.CALL_TIMEOUT_MILLIS));
            }
            for (CompletableFuture<RpcResponse> future : futures) {
                assertEquals("slow:slow", future.get(5, TimeUnit.SECONDS).getData());
            }
            assertEquals(1, client.activeConnections(TestSupport.HOST, port), "默认每地址一条长连接复用");
        } finally {
            client.close();
            server.close();
        }
    }

    @Test
    void growsUpToConfiguredMaximumThenEvictsIdleConnections() throws Exception {
        int port = TestSupport.freePort();
        NettyServer server = new NettyServer(TestSupport.HOST, port, TestSupport.slowHandler(1_500));
        server.start();

        NettyClient client = new NettyClient(ClientOptions.builder()
                .maxConnectionsPerAddress(3)
                .idleConnectionTimeoutMillis(500)
                .reconnectEnabled(false)
                .build());
        try {
            List<CompletableFuture<RpcResponse>> futures = new ArrayList<>();
            // 分批发起：当已有连接都承载了在途请求、且未达上限时，连接池会后台扩容
            for (int i = 0; i < 5; i++) {
                futures.add(client.send(TestSupport.request("slow"), TestSupport.target(port),
                        TestSupport.CALL_TIMEOUT_MILLIS));
                TestSupport.sleepQuietly(150);
            }

            TestSupport.awaitTrue("所有连接都忙时应后台扩容到上限3条",
                    3_000, () -> client.activeConnections(TestSupport.HOST, port) == 3);
            assertTrue(client.activeConnections(TestSupport.HOST, port) <= 3);

            for (CompletableFuture<RpcResponse> future : futures) {
                assertEquals("slow:slow", future.get(10, TimeUnit.SECONDS).getData());
            }

            TestSupport.awaitTrue("无在途请求且空闲超时后应回收连接",
                    5_000, () -> client.activeConnections(TestSupport.HOST, port) == 0);
        } finally {
            client.close();
            server.close();
        }
    }

    @Test
    void neverExceedsConfiguredMaximumUnderBurst() throws Exception {
        int port = TestSupport.freePort();
        NettyServer server = new NettyServer(TestSupport.HOST, port, TestSupport.slowHandler(400));
        server.start();

        NettyClient client = new NettyClient(ClientOptions.builder()
                .maxConnectionsPerAddress(2)
                .reconnectEnabled(false)
                .build());
        try {
            List<CompletableFuture<RpcResponse>> futures = new ArrayList<>();
            for (int i = 0; i < 8; i++) {
                futures.add(client.send(TestSupport.request("slow"), TestSupport.target(port),
                        TestSupport.CALL_TIMEOUT_MILLIS));
                TestSupport.sleepQuietly(60);
                assertTrue(client.activeConnections(TestSupport.HOST, port) <= 2,
                        "连接数绝不能超过配置上限");
            }
            for (CompletableFuture<RpcResponse> future : futures) {
                assertEquals("slow:slow", future.get(5, TimeUnit.SECONDS).getData());
            }
        } finally {
            client.close();
            server.close();
        }
    }
}
