package io.github.nitouge.conduit.remoting.netty;

import io.github.nitouge.conduit.api.RpcRequest;
import io.github.nitouge.conduit.api.RpcResponse;
import io.github.nitouge.conduit.common.exception.RpcException;
import io.github.nitouge.conduit.remoting.ClientOptions;
import io.github.nitouge.conduit.remoting.RequestHandler;
import io.github.nitouge.conduit.remoting.TestSupport;
import io.netty.channel.Channel;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 客户端写背压测试：服务端变慢时，在途请求达到上限即快速失败，而不是无限堆积到内存或超时。
 */
class BackpressureIntegrationTest {

    @Test
    void inFlightCapFailsFastInsteadOfQueueing() throws Exception {
        int port = TestSupport.freePort();
        CountDownLatch firstReachedServer = new CountDownLatch(1);
        // 服务端处理很慢：第一条请求会长期占住一个在途槽位
        RequestHandler slow = request -> {
            firstReachedServer.countDown();
            return CompletableFuture.supplyAsync(() -> {
                TestSupport.sleepQuietly(800);
                return RpcResponse.success("slow");
            });
        };
        NettyServer server = new NettyServer(TestSupport.HOST, port, slow);
        server.start();

        NettyClient client = new NettyClient(ClientOptions.builder()
                .maxInFlightPerConnection(1)
                .reconnectEnabled(false)
                .build());
        try {
            CompletableFuture<RpcResponse> first = client.send(
                    TestSupport.request("first"), TestSupport.target(port), TestSupport.CALL_TIMEOUT_MILLIS);
            // 等服务端真的收到：此刻客户端一定已经把第一条记进了在途表（先登记、再写出）
            assertTrue(firstReachedServer.await(2, TimeUnit.SECONDS), "第一条请求应已到达服务端");

            Throwable cause = sendFailing(client, port, TestSupport.request("second"));

            assertTrue(cause instanceof RpcException, "背压拒绝应是RpcException: " + cause);
            assertTrue(cause.getMessage().contains("背压"), "错误信息应说明是背压: " + cause.getMessage());
            assertEquals(1, client.getBackpressureRejections(), "应记录一次背压拒绝");
            assertEquals("slow", first.get(TestSupport.CALL_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS).getData(),
                    "第一条请求不受影响，仍然正常完成");
        } finally {
            client.close();
            server.close();
        }
    }

    @Test
    void writeWaterMarkIsAppliedToChannel() {
        int port = TestSupport.freePort();
        NettyServer server = new NettyServer(TestSupport.HOST, port, TestSupport.echoHandler());
        server.start();

        NettyClient client = new NettyClient(ClientOptions.builder()
                .writeBufferWaterMark(1_024, 4_096)
                .build());
        try {
            Channel channel = client.getChannel(new InetSocketAddress(TestSupport.HOST, port));
            assertEquals(4_096, channel.config().getWriteBufferHighWaterMark());
            assertEquals(1_024, channel.config().getWriteBufferLowWaterMark());
        } finally {
            client.close();
            server.close();
        }
    }

    @Test
    void invalidWaterMarkIsRejectedAtBuildTime() {
        assertThrows(IllegalArgumentException.class, () -> ClientOptions.builder().writeBufferWaterMark(4_096, 4_096).build(),
                "高水位必须大于低水位，否则channel会永远不可写");
        assertThrows(IllegalArgumentException.class, () -> ClientOptions.builder().maxInFlightPerConnection(0).build());
    }

    private static Throwable sendFailing(NettyClient client, int port, RpcRequest request) {
        try {
            RpcResponse response = client.send(request, TestSupport.target(port), TestSupport.CALL_TIMEOUT_MILLIS)
                    .get(TestSupport.CALL_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
            throw new AssertionError("期望被背压拒绝，但成功了: " + response);
        } catch (ExecutionException e) {
            return e.getCause();
        } catch (TimeoutException e) {
            return e;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError("调用被中断", e);
        }
    }
}
