package io.github.nitouge.conduit.protocol;

import io.github.nitouge.conduit.api.RpcRequest;
import io.github.nitouge.conduit.api.RpcResponse;
import io.github.nitouge.conduit.common.exception.RpcException;
import io.github.nitouge.conduit.protocol.codec.RpcDecoder;
import io.github.nitouge.conduit.protocol.codec.RpcEncoder;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 协议编解码单元测试：帧布局、控制帧（心跳/握手/取消）、压缩阈值、版本与长度校验。
 */
class RpcCodecTest {

    // ------------------------------------------------------------ 工具方法

    private static EmbeddedChannel decoderChannel() {
        return new EmbeddedChannel(
                new LengthFieldBasedFrameDecoder(
                        ProtocolConstants.MAX_FRAME_LENGTH,
                        ProtocolConstants.LENGTH_FIELD_OFFSET,
                        ProtocolConstants.LENGTH_FIELD_LENGTH,
                        0,
                        0),
                new RpcDecoder());
    }

    private static ByteBuf encode(RpcMessage message) {
        return encode(message, ProtocolOptions.defaults());
    }

    private static ByteBuf encode(RpcMessage message, ProtocolOptions options) {
        EmbeddedChannel channel = new EmbeddedChannel(new RpcEncoder(options));
        assertTrue(channel.writeOutbound(message), "编码器应产生出站字节");
        ByteBuf bytes = channel.readOutbound();
        assertNotNull(bytes);
        return bytes;
    }

    private static RpcMessage decodeOne(ByteBuf bytes) {
        EmbeddedChannel channel = decoderChannel();
        assertTrue(channel.writeInbound(bytes), "解码器应产出一条消息");
        RpcMessage message = channel.readInbound();
        assertNotNull(message);
        assertNull(channel.readInbound(), "一帧只应产出一条消息");
        return message;
    }

    private static ByteBuf frame(byte version, byte type, byte codec, byte compress, long requestId, byte[] body) {
        ByteBuf buf = Unpooled.buffer();
        buf.writeShort(ProtocolConstants.MAGIC);
        buf.writeByte(version);
        buf.writeByte(type);
        buf.writeByte(codec);
        buf.writeByte(compress);
        buf.writeLong(requestId);
        buf.writeInt(body.length);
        buf.writeBytes(body);
        return buf;
    }

    private static RpcRequest sampleRequest() {
        RpcRequest request = new RpcRequest();
        request.setInterfaceName("com.example.DemoService");
        request.setMethodName("hello");
        request.setParamTypes(new Class<?>[]{String.class});
        request.setArgs(new Object[]{"conduit"});
        return request;
    }

    // ------------------------------------------------------------ 业务帧

    @Test
    void requestRoundTripPreservesHeaderAndPayload() {
        RpcMessage message = RpcMessage.request(42L, ProtocolConstants.CODEC_JDK, sampleRequest());
        ByteBuf bytes = encode(message);

        // 定长头字段偏移核对：magic(2) version(1) type(1) codec(1) compress(1) requestId(8) length(4)
        assertEquals(ProtocolConstants.MAGIC, bytes.getShort(0));
        assertEquals(ProtocolConstants.VERSION, bytes.getByte(2));
        assertEquals(ProtocolConstants.TYPE_REQUEST, bytes.getByte(3));
        assertEquals(ProtocolConstants.CODEC_JDK, bytes.getByte(4));
        assertEquals(ProtocolConstants.COMPRESS_NONE, bytes.getByte(5));
        assertEquals(42L, bytes.getLong(6));
        assertEquals(bytes.readableBytes() - ProtocolConstants.HEADER_LENGTH, bytes.getInt(14));

        RpcMessage decoded = decodeOne(bytes);
        assertEquals(ProtocolConstants.TYPE_REQUEST, decoded.getType());
        assertEquals(ProtocolConstants.VERSION, decoded.getVersion());
        assertEquals(ProtocolConstants.CODEC_JDK, decoded.getCodec());
        assertEquals(ProtocolConstants.COMPRESS_NONE, decoded.getCompress());
        assertEquals(42L, decoded.getRequestId());

        RpcRequest request = assertInstanceOf(RpcRequest.class, decoded.getPayload());
        assertEquals("com.example.DemoService", request.getInterfaceName());
        assertEquals("hello", request.getMethodName());
        assertArrayEquals(new Class<?>[]{String.class}, request.getParamTypes());
        assertArrayEquals(new Object[]{"conduit"}, request.getArgs());
    }

    @Test
    void responseRoundTripPreservesPayload() {
        RpcMessage message = RpcMessage.response(7L, ProtocolConstants.CODEC_JDK,
                RpcResponse.success("pong"));
        RpcMessage decoded = decodeOne(encode(message));

        assertEquals(ProtocolConstants.TYPE_RESPONSE, decoded.getType());
        assertEquals(7L, decoded.getRequestId());
        RpcResponse response = assertInstanceOf(RpcResponse.class, decoded.getPayload());
        assertFalse(response.hasException());
        assertEquals("pong", response.getData());
    }

    // ------------------------------------------------------------ 控制帧

    @Test
    void heartbeatHasNoBody() {
        ByteBuf bytes = encode(RpcMessage.heartbeat(0L, ProtocolConstants.CODEC_JDK));
        assertEquals(ProtocolConstants.HEADER_LENGTH, bytes.readableBytes(), "心跳帧应只有18字节头");
        assertEquals(0, bytes.getInt(ProtocolConstants.LENGTH_FIELD_OFFSET));

        RpcMessage decoded = decodeOne(bytes);
        assertTrue(decoded.isHeartbeat());
        assertNull(decoded.getPayload());
    }

    @Test
    void heartbeatWithUnexpectedBodyDoesNotCorruptFollowingFrames() {
        // 异常对端给心跳帧带了body：解码器应跳过body，后续帧仍能正确解出
        ByteBuf heartbeat = frame(ProtocolConstants.VERSION, ProtocolConstants.TYPE_HEARTBEAT,
                ProtocolConstants.CODEC_JDK, ProtocolConstants.COMPRESS_NONE, 0L, new byte[]{1, 2, 3});
        ByteBuf request = encode(RpcMessage.request(9L, ProtocolConstants.CODEC_JDK, sampleRequest()));

        EmbeddedChannel channel = decoderChannel();
        assertTrue(channel.writeInbound(Unpooled.wrappedBuffer(heartbeat, request)));

        RpcMessage first = channel.readInbound();
        RpcMessage second = channel.readInbound();
        assertNotNull(first);
        assertTrue(first.isHeartbeat());
        assertNotNull(second);
        assertEquals(9L, second.getRequestId());
        assertInstanceOf(RpcRequest.class, second.getPayload());
    }

    @Test
    void handshakeOfferRoundTrip() {
        RpcMessage offer = RpcMessage.handshake(ProtocolConstants.MAX_SUPPORTED_VERSION,
                ProtocolConstants.CODEC_KRYO, ProtocolConstants.COMPRESS_GZIP);
        ByteBuf bytes = encode(offer);

        // 握手帧body固定1字节status，不走序列化器
        assertEquals(1, bytes.getInt(ProtocolConstants.LENGTH_FIELD_OFFSET));
        assertEquals(ProtocolConstants.HEADER_LENGTH + 1, bytes.readableBytes());

        RpcMessage decoded = decodeOne(bytes);
        assertTrue(decoded.isHandshakeRequest());
        assertEquals(ProtocolConstants.HANDSHAKE_OK, decoded.getStatus());
        assertEquals(ProtocolConstants.MAX_SUPPORTED_VERSION, decoded.getVersion());
        assertEquals(ProtocolConstants.CODEC_KRYO, decoded.getCodec());
        assertEquals(ProtocolConstants.COMPRESS_GZIP, decoded.getCompress());
        assertNull(decoded.getPayload());
    }

    @Test
    void handshakeAckRoundTrip() {
        RpcMessage ack = RpcMessage.handshakeAck((byte) 1, ProtocolConstants.CODEC_JSON,
                ProtocolConstants.COMPRESS_NONE);
        RpcMessage decoded = decodeOne(encode(ack));

        assertTrue(decoded.isHandshakeAck());
        assertEquals(ProtocolConstants.HANDSHAKE_OK, decoded.getStatus());
        assertEquals(1, decoded.getVersion());
        assertEquals(ProtocolConstants.CODEC_JSON, decoded.getCodec());
    }

    @Test
    void handshakeRejectRoundTripCarriesStatus() {
        RpcMessage reject = RpcMessage.handshakeReject(ProtocolConstants.HANDSHAKE_NO_COMMON_VERSION,
                ProtocolConstants.MAX_SUPPORTED_VERSION);
        RpcMessage decoded = decodeOne(encode(reject));

        assertTrue(decoded.isHandshakeReject());
        assertEquals(ProtocolConstants.HANDSHAKE_NO_COMMON_VERSION, decoded.getStatus());
        assertEquals(ProtocolConstants.MAX_SUPPORTED_VERSION, decoded.getVersion());
    }

    // ------------------------------------------------------------ 取消帧

    @Test
    void cancelRoundTripCarriesRequestIdAndReason() {
        RpcMessage cancel = RpcMessage.cancel(4242L, ProtocolConstants.CODEC_KRYO,
                ProtocolConstants.VERSION, ProtocolConstants.CANCEL_CLIENT_TIMEOUT);
        ByteBuf bytes = encode(cancel);

        // 与握手帧同构：body固定1字节（取消原因），不经过序列化器
        assertEquals(1, bytes.getInt(ProtocolConstants.LENGTH_FIELD_OFFSET));
        assertEquals(ProtocolConstants.HEADER_LENGTH + 1, bytes.readableBytes());
        assertEquals(ProtocolConstants.TYPE_CANCEL, bytes.getByte(3));
        assertEquals(ProtocolConstants.COMPRESS_NONE, bytes.getByte(5), "控制帧不压缩");
        assertEquals(4242L, bytes.getLong(6));

        RpcMessage decoded = decodeOne(bytes);
        assertTrue(decoded.isCancel());
        assertTrue(decoded.isControlFrame());
        assertEquals(4242L, decoded.getRequestId());
        assertEquals(ProtocolConstants.CANCEL_CLIENT_TIMEOUT, decoded.getStatus());
        assertNull(decoded.getPayload());
    }

    @Test
    void cancelWithIllegalBodyLengthClosesConnection() {
        ByteBuf bytes = frame(ProtocolConstants.VERSION, ProtocolConstants.TYPE_CANCEL,
                ProtocolConstants.CODEC_JDK, ProtocolConstants.COMPRESS_NONE, 1L, new byte[]{1, 2, 3});
        assertProtocolViolation(decoderChannel(), bytes);
    }

    @Test
    void cancelWithUnsupportedVersionClosesConnection() {
        ByteBuf bytes = frame((byte) 99, ProtocolConstants.TYPE_CANCEL,
                ProtocolConstants.CODEC_JDK, ProtocolConstants.COMPRESS_NONE, 1L, new byte[]{0});
        assertProtocolViolation(decoderChannel(), bytes);
    }

    // ------------------------------------------------------------ 压缩

    @Test
    void gzipCompressionIsAppliedWhenBodyReachesThreshold() {
        RpcRequest request = sampleRequest();
        // 大参数：确保body超过阈值
        request.setArgs(new Object[]{"x".repeat(4096)});
        ProtocolOptions options = ProtocolOptions.builder()
                .compress(ProtocolConstants.COMPRESS_GZIP)
                .compressThreshold(0)
                .build();

        RpcMessage message = RpcMessage.request(1L, ProtocolConstants.CODEC_JDK, request);
        message.setCompress(ProtocolConstants.COMPRESS_GZIP);
        ByteBuf bytes = encode(message, options);

        assertEquals(ProtocolConstants.COMPRESS_GZIP, bytes.getByte(5), "头里应如实记录实际压缩算法");
        RpcMessage decoded = decodeOne(bytes);
        RpcRequest decodedRequest = assertInstanceOf(RpcRequest.class, decoded.getPayload());
        assertEquals("x".repeat(4096), decodedRequest.getArgs()[0]);
    }

    @Test
    void smallBodyIsNotCompressedWhenBelowThreshold() {
        RpcMessage message = RpcMessage.request(2L, ProtocolConstants.CODEC_JDK,
                RpcResponse.success("tiny"));
        message.setCompress(ProtocolConstants.COMPRESS_GZIP);
        ProtocolOptions options = ProtocolOptions.builder()
                .compress(ProtocolConstants.COMPRESS_GZIP)
                .compressThreshold(1024)
                .build();

        ByteBuf bytes = encode(message, options);
        assertEquals(ProtocolConstants.COMPRESS_NONE, bytes.getByte(5), "小报文应回退为不压缩");

        RpcMessage decoded = decodeOne(bytes);
        assertEquals(ProtocolConstants.COMPRESS_NONE, decoded.getCompress());
        assertNotNull(decoded.getPayload());
    }

    // ------------------------------------------------------------ 防御性校验

    @Test
    void unsupportedVersionClosesConnection() {
        ByteBuf bytes = frame((byte) 99, ProtocolConstants.TYPE_REQUEST, ProtocolConstants.CODEC_JDK,
                ProtocolConstants.COMPRESS_NONE, 1L, new byte[]{1});
        assertProtocolViolation(decoderChannel(), bytes);
    }

    @Test
    void unknownCodecClosesConnection() {
        ByteBuf bytes = frame(ProtocolConstants.VERSION, ProtocolConstants.TYPE_REQUEST, (byte) 99,
                ProtocolConstants.COMPRESS_NONE, 1L, new byte[]{1});
        assertProtocolViolation(decoderChannel(), bytes);
    }

    @Test
    void negativeLengthClosesConnection() {
        // 直接喂给RpcDecoder（不经过切帧器）：负数length必须被判为协议违规，而不是让new byte[负数]炸掉
        ByteBuf buf = Unpooled.buffer();
        buf.writeShort(ProtocolConstants.MAGIC);
        buf.writeByte(ProtocolConstants.VERSION);
        buf.writeByte(ProtocolConstants.TYPE_REQUEST);
        buf.writeByte(ProtocolConstants.CODEC_JDK);
        buf.writeByte(ProtocolConstants.COMPRESS_NONE);
        buf.writeLong(1L);
        buf.writeInt(-1);
        assertProtocolViolation(new EmbeddedChannel(new RpcDecoder()), buf);
    }

    @Test
    void illegalMagicClosesConnection() {
        ByteBuf buf = Unpooled.buffer();
        buf.writeShort(0x1234);
        buf.writeBytes(new byte[ProtocolConstants.HEADER_LENGTH - 2]);
        assertProtocolViolation(new EmbeddedChannel(new RpcDecoder()), buf);
    }

    @Test
    void requestDecodeFailureReturnsErrorResponseInsteadOfClosing() {
        // JDK序列化的body用垃圾字节填充 -> 反序列化失败，解码器应回一帧错误响应而不是直接断链
        ByteBuf bytes = frame(ProtocolConstants.VERSION, ProtocolConstants.TYPE_REQUEST,
                ProtocolConstants.CODEC_JDK, ProtocolConstants.COMPRESS_NONE, 55L, new byte[]{0, 1, 2, 3});
        EmbeddedChannel channel = decoderChannel();

        channel.writeInbound(bytes);
        assertNull(channel.readInbound(), "解码失败不应产出一帧正常消息");

        RpcMessage errorResponse = channel.readOutbound();
        assertNotNull(errorResponse, "解码失败应回传错误响应");
        assertEquals(ProtocolConstants.TYPE_RESPONSE, errorResponse.getType());
        assertEquals(55L, errorResponse.getRequestId());
        RpcResponse response = assertInstanceOf(RpcResponse.class, errorResponse.getPayload());
        assertTrue(response.hasException());
        assertTrue(channel.isOpen(), "单帧解码失败不应关闭连接");
    }

    @Test
    void protocolViolationsThrowRpcException() {
        ByteBuf bytes = frame((byte) 99, ProtocolConstants.TYPE_REQUEST,
                ProtocolConstants.CODEC_JDK, ProtocolConstants.COMPRESS_NONE, 1L, new byte[0]);
        Throwable thrown = assertThrows(Throwable.class, () -> decoderChannel().writeInbound(bytes));

        Throwable cause = thrown;
        while (cause != null && !(cause instanceof RpcException)) {
            cause = cause.getCause();
        }
        assertNotNull(cause, "协议违规应包含RpcException作为原因: " + thrown);
    }

    /** 断言：协议违规帧会关闭连接并抛出（含）RpcException */
    private static void assertProtocolViolation(EmbeddedChannel channel, ByteBuf bytes) {
        assertThrows(Throwable.class, () -> channel.writeInbound(bytes));
        assertFalse(channel.isOpen(), "协议违规应关闭连接");
    }
}
