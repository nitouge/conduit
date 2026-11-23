package io.github.nitouge.conduit.remoting.handshake;

import io.github.nitouge.conduit.api.RpcResponse;
import io.github.nitouge.conduit.common.exception.RpcException;
import io.github.nitouge.conduit.protocol.ProtocolConstants;
import io.github.nitouge.conduit.protocol.RpcMessage;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 握手处理器单元测试（EmbeddedChannel）：协商成功/各类拒绝、握手前业务帧拦截、客户端future完成。
 */
class HandshakeHandlerTest {

    // ------------------------------------------------------------ 服务端

    private static RpcMessage offer(byte version, byte codec, byte compress) {
        return RpcMessage.handshake(version, codec, compress);
    }

    @Test
    void serverAcceptsSupportedOffer() {
        EmbeddedChannel channel = new EmbeddedChannel(new ServerHandshakeHandler(1_000));

        channel.writeInbound(offer(ProtocolConstants.MAX_SUPPORTED_VERSION,
                ProtocolConstants.CODEC_JDK, ProtocolConstants.COMPRESS_GZIP));

        RpcMessage ack = channel.readOutbound();
        assertNotNull(ack);
        assertTrue(ack.isHandshakeAck());
        assertEquals(ProtocolConstants.HANDSHAKE_OK, ack.getStatus());
        assertEquals(ProtocolConstants.VERSION, ack.getVersion());
        assertEquals(ProtocolConstants.CODEC_JDK, ack.getCodec());
        assertEquals(ProtocolConstants.COMPRESS_GZIP, ack.getCompress());

        Negotiated negotiated = Handshake.negotiated(channel);
        assertNotNull(negotiated);
        assertEquals(ProtocolConstants.VERSION, negotiated.getVersion());
        assertTrue(channel.isOpen());
    }

    @Test
    void serverRejectsTooOldVersion() {
        EmbeddedChannel channel = new EmbeddedChannel(new ServerHandshakeHandler(1_000));

        channel.writeInbound(offer((byte) 0, ProtocolConstants.CODEC_JDK, ProtocolConstants.COMPRESS_NONE));
        RpcMessage reject = channel.readOutbound();
        assertNotNull(reject);
        assertTrue(reject.isHandshakeReject());
        assertEquals(ProtocolConstants.HANDSHAKE_NO_COMMON_VERSION, reject.getStatus());
        assertEquals(ProtocolConstants.MAX_SUPPORTED_VERSION, reject.getVersion());
        assertNull(Handshake.negotiated(channel));
        assertFalse(channel.isOpen(), "拒绝握手后应关闭连接");
    }

    @Test
    void serverDowngradesNewerClientToHighestSupportedVersion() {
        EmbeddedChannel channel = new EmbeddedChannel(new ServerHandshakeHandler(1_000));

        // 未来客户端（声称支持到v9）连当前服务端：应平滑降级到v1，而不是拒绝
        channel.writeInbound(offer((byte) 9, ProtocolConstants.CODEC_JDK, ProtocolConstants.COMPRESS_NONE));

        RpcMessage ack = channel.readOutbound();
        assertNotNull(ack);
        assertTrue(ack.isHandshakeAck());
        assertEquals(ProtocolConstants.MAX_SUPPORTED_VERSION, ack.getVersion());
        assertNotNull(Handshake.negotiated(channel));
        assertEquals(ProtocolConstants.MAX_SUPPORTED_VERSION, Handshake.negotiated(channel).getVersion());
        assertTrue(channel.isOpen());
    }

    @Test
    void serverRejectsUnknownCodec() {
        EmbeddedChannel channel = new EmbeddedChannel(new ServerHandshakeHandler(1_000));

        channel.writeInbound(offer(ProtocolConstants.VERSION, (byte) 99, ProtocolConstants.COMPRESS_NONE));

        RpcMessage reject = channel.readOutbound();
        assertNotNull(reject);
        assertEquals(ProtocolConstants.HANDSHAKE_UNSUPPORTED_CODEC, reject.getStatus());
        assertFalse(channel.isOpen());
    }

    @Test
    void serverRejectsUnknownCompress() {
        EmbeddedChannel channel = new EmbeddedChannel(new ServerHandshakeHandler(1_000));

        channel.writeInbound(offer(ProtocolConstants.VERSION, ProtocolConstants.CODEC_JDK, (byte) 42));

        RpcMessage reject = channel.readOutbound();
        assertNotNull(reject);
        assertEquals(ProtocolConstants.HANDSHAKE_UNSUPPORTED_COMPRESS, reject.getStatus());
        assertFalse(channel.isOpen());
    }

    @Test
    void serverClosesConnectionWhenBusinessFrameArrivesBeforeHandshake() {
        EmbeddedChannel channel = new EmbeddedChannel(new ServerHandshakeHandler(1_000));

        channel.writeInbound(RpcMessage.request(1L, ProtocolConstants.CODEC_JDK, RpcResponse.success("x")));

        assertFalse(channel.isOpen(), "握手前收到业务帧应关闭连接");
    }

    @Test
    void serverPassesHeartbeatThroughBeforeHandshake() {
        EmbeddedChannel channel = new EmbeddedChannel(new ServerHandshakeHandler(1_000));

        assertTrue(channel.writeInbound(RpcMessage.heartbeat(0L, ProtocolConstants.CODEC_JDK)));

        RpcMessage heartbeat = channel.readInbound();
        assertNotNull(heartbeat);
        assertTrue(heartbeat.isHeartbeat());
        assertTrue(channel.isOpen());
    }

    @Test
    void serverIgnoresDuplicateHandshakeAndReplaysAck() {
        EmbeddedChannel channel = new EmbeddedChannel(new ServerHandshakeHandler(1_000));
        RpcMessage offer = offer(ProtocolConstants.VERSION, ProtocolConstants.CODEC_JDK,
                ProtocolConstants.COMPRESS_NONE);

        channel.writeInbound(offer);
        assertTrue(channel.readOutbound() != null);
        channel.writeInbound(offer);
        RpcMessage secondAck = channel.readOutbound();
        assertNotNull(secondAck);
        assertTrue(secondAck.isHandshakeAck());
        assertEquals(ProtocolConstants.VERSION, Handshake.negotiated(channel).getVersion());
    }

    // ------------------------------------------------------------ 客户端

    @Test
    void clientSendsOfferOnActivationAndCompletesFutureOnAck() throws Exception {
        EmbeddedChannel channel = new EmbeddedChannel(new ClientHandshakeHandler(
                ProtocolConstants.MAX_SUPPORTED_VERSION, ProtocolConstants.CODEC_JDK,
                ProtocolConstants.COMPRESS_NONE, 1_000));
        CompletableFuture<Negotiated> future = ClientHandshakeHandler.channelFuture(channel);

        RpcMessage offer = channel.readOutbound();
        assertNotNull(offer, "连接激活后应发出握手提议");
        assertTrue(offer.isHandshakeRequest());
        assertEquals(ProtocolConstants.MAX_SUPPORTED_VERSION, offer.getVersion());
        assertFalse(future.isDone());

        channel.writeInbound(RpcMessage.handshakeAck(ProtocolConstants.VERSION,
                ProtocolConstants.CODEC_KRYO, ProtocolConstants.COMPRESS_GZIP));

        Negotiated negotiated = future.getNow(null);
        assertNotNull(negotiated);
        assertEquals(ProtocolConstants.VERSION, negotiated.getVersion());
        assertEquals(ProtocolConstants.CODEC_KRYO, negotiated.getCodec());
        assertEquals(ProtocolConstants.COMPRESS_GZIP, negotiated.getCompress());
        assertTrue(channel.isOpen());
    }

    @Test
    void clientFailsFutureAndClosesOnReject() {
        EmbeddedChannel channel = new EmbeddedChannel(new ClientHandshakeHandler(
                ProtocolConstants.MAX_SUPPORTED_VERSION, ProtocolConstants.CODEC_JDK,
                ProtocolConstants.COMPRESS_NONE, 1_000));
        CompletableFuture<Negotiated> future = ClientHandshakeHandler.channelFuture(channel);
        assertNotNull(channel.readOutbound());

        channel.writeInbound(RpcMessage.handshakeReject(ProtocolConstants.HANDSHAKE_NO_COMMON_VERSION,
                ProtocolConstants.MAX_SUPPORTED_VERSION));

        assertTrue(future.isCompletedExceptionally(), "被拒绝时future应异常完成");
        assertFalse(channel.isOpen(), "被拒绝时应关闭连接");
    }

    @Test
    void clientFutureFailsWhenConnectionClosesDuringHandshake() throws Exception {
        EmbeddedChannel channel = new EmbeddedChannel(new ClientHandshakeHandler(
                ProtocolConstants.MAX_SUPPORTED_VERSION, ProtocolConstants.CODEC_JDK,
                ProtocolConstants.COMPRESS_NONE, 1_000));
        CompletableFuture<Negotiated> future = ClientHandshakeHandler.channelFuture(channel);

        channel.close();

        assertTrue(future.isCompletedExceptionally());
        ExecutionException thrown = assertThrows(ExecutionException.class, future::get);
        assertInstanceOf(RpcException.class, thrown.getCause());
    }
}
