package io.github.nitouge.conduit.remoting.netty;

import io.github.nitouge.conduit.api.RpcResponse;
import io.github.nitouge.conduit.protocol.ProtocolConstants;
import io.github.nitouge.conduit.remoting.ClientOptions;
import io.github.nitouge.conduit.remoting.ServerOptions;
import io.github.nitouge.conduit.remoting.TestSupport;
import io.github.nitouge.conduit.remoting.handshake.Handshake;
import io.github.nitouge.conduit.remoting.handshake.Negotiated;
import io.netty.channel.Channel;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.net.Socket;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 握手集成测试（真实socket）：协议版本/codec/压缩能力协商，握手超时与未握手连接的清理。
 */
class HandshakeIntegrationTest {

    @Test
    void endToEndCallGoesThroughHandshakeAndNegotiatesCapabilities() throws Exception {
        int port = TestSupport.freePort();
        NettyServer server = new NettyServer(TestSupport.HOST, port, TestSupport.echoHandler());
        server.start();

        NettyClient client = new NettyClient(ClientOptions.builder()
                .compress(ProtocolConstants.COMPRESS_GZIP)
                .build());
        try {
            RpcResponse response = TestSupport.call(client, port, "hello");
            assertEquals("ok:hello", response.getData());

            Channel channel = client.getChannel(new InetSocketAddress(TestSupport.HOST, port));
            Negotiated negotiated = Handshake.negotiated(channel);
            assertNotNull(negotiated, "连接必须完成握手后才可承载业务帧");
            assertEquals(ProtocolConstants.VERSION, negotiated.getVersion());
            assertEquals(ProtocolConstants.CODEC_JDK, negotiated.getCodec());
            assertEquals(ProtocolConstants.COMPRESS_GZIP, negotiated.getCompress());

            assertEquals(1, client.activeConnections(TestSupport.HOST, port));
        } finally {
            client.close();
            server.close();
        }
    }

    @Test
    void serverClosesConnectionWhenClientNeverHandshakes() throws Exception {
        int port = TestSupport.freePort();
        NettyServer server = new NettyServer(TestSupport.HOST, port, TestSupport.echoHandler(),
                ServerOptions.builder()
                        .handshakeTimeoutMillis(200)
                        .readerIdleTimeoutMillis(0)
                        .build());
        server.start();
        try (Socket raw = new Socket()) {
            raw.connect(new InetSocketAddress(TestSupport.HOST, port), 2_000);
            raw.setSoTimeout(3_000);

            int read = raw.getInputStream().read();

            assertEquals(-1, read, "服务端应在握手超时后关闭连接（读到EOF）");
            TestSupport.awaitTrue("未握手的连接应被清理",
                    3_000, () -> server.getActiveConnectionCount() == 0);
        } finally {
            server.close();
        }
    }

    @Test
    void handshakeSucceedsOverMultipleSequentialCallsOnSameConnection() throws Exception {
        int port = TestSupport.freePort();
        NettyServer server = new NettyServer(TestSupport.HOST, port, TestSupport.echoHandler());
        server.start();

        NettyClient client = new NettyClient();
        try {
            for (int i = 0; i < 20; i++) {
                assertEquals("ok:call" + i, TestSupport.call(client, port, "call" + i).getData());
            }
            assertTrue(client.activeConnections(TestSupport.HOST, port) <= 1, "默认单连接复用");
        } finally {
            client.close();
            server.close();
        }
    }
}
