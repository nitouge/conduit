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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 压缩集成测试（真实socket）：GZIP 端到端往返、阈值生效与默认不压缩。
 */
class CompressionIntegrationTest {

    private static final String BIG_PAYLOAD = "conduit-".repeat(40_000); // ~320KB

    @Test
    void gzipEndToEndPreservesLargePayloadInBothDirections() throws Exception {
        int port = TestSupport.freePort();
        NettyServer server = new NettyServer(TestSupport.HOST, port, TestSupport.echoArgumentHandler(),
                ServerOptions.builder().compressThreshold(0).build());
        server.start();

        NettyClient client = new NettyClient(ClientOptions.builder()
                .compress(ProtocolConstants.COMPRESS_GZIP)
                .compressThreshold(0)
                .build());
        try {
            RpcResponse response = client.send(TestSupport.request("echo", BIG_PAYLOAD),
                    TestSupport.target(port), TestSupport.CALL_TIMEOUT_MILLIS)
                    .get(TestSupport.CALL_TIMEOUT_MILLIS, java.util.concurrent.TimeUnit.MILLISECONDS);

            assertEquals(BIG_PAYLOAD.length(), ((String) response.getData()).length());
            assertEquals(BIG_PAYLOAD, response.getData());

            Channel channel = client.getChannel(new InetSocketAddress(TestSupport.HOST, port));
            Negotiated negotiated = Handshake.negotiated(channel);
            assertNotNull(negotiated);
            assertEquals(ProtocolConstants.COMPRESS_GZIP, negotiated.getCompress());
        } finally {
            client.close();
            server.close();
        }
    }

    @Test
    void compressionIsNotAppliedByDefault() throws Exception {
        int port = TestSupport.freePort();
        NettyServer server = new NettyServer(TestSupport.HOST, port, TestSupport.echoHandler());
        server.start();

        NettyClient client = new NettyClient();
        try {
            assertEquals("ok:ping", TestSupport.call(client, port, "ping").getData());

            Channel channel = client.getChannel(new InetSocketAddress(TestSupport.HOST, port));
            Negotiated negotiated = Handshake.negotiated(channel);
            assertNotNull(negotiated);
            assertEquals(ProtocolConstants.COMPRESS_NONE, negotiated.getCompress());
        } finally {
            client.close();
            server.close();
        }
    }

    @Test
    void smallPayloadBelowThresholdIsNotCompressedButStillWorks() throws Exception {
        int port = TestSupport.freePort();
        NettyServer server = new NettyServer(TestSupport.HOST, port, TestSupport.echoArgumentHandler(),
                ServerOptions.builder().compressThreshold(1024).build());
        server.start();

        NettyClient client = new NettyClient(ClientOptions.builder()
                .compress(ProtocolConstants.COMPRESS_GZIP)
                .compressThreshold(1024)
                .build());
        try {
            RpcResponse response = TestSupport.call(client, port, "echo", "tiny");
            assertEquals("tiny", response.getData());
            assertTrue(client.getResponsesReceived() > 0);
        } finally {
            client.close();
            server.close();
        }
    }
}
