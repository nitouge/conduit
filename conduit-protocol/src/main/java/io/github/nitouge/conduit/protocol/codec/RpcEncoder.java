package io.github.nitouge.conduit.protocol.codec;

import io.github.nitouge.conduit.protocol.ProtocolConstants;
import io.github.nitouge.conduit.protocol.RpcMessage;

import io.github.nitouge.conduit.serialization.Serializer;
import io.github.nitouge.conduit.serialization.SerializerFactory;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 出站编码器：对象 -> 字节。先写18字节定长头，再写（序列化 + 压缩后的）body。
 *
 * <p>按codec字段选择 {@link Serializer}，<b>绝不出现任何具体序列化类名</b>——这正是序列化可替换在协议层的落地。</p>
 */
public class RpcEncoder extends MessageToByteEncoder<RpcMessage> {

    private static final Logger log = LoggerFactory.getLogger(RpcEncoder.class);

    @Override
    protected void encode(ChannelHandlerContext ctx, RpcMessage msg, ByteBuf out) {
        // 1.写定长头（顺序与偏移表一致）
        out.writeShort(ProtocolConstants.MAGIC);   // magic, 2
        out.writeByte(ProtocolConstants.VERSION);  // version, 1
        out.writeByte(msg.getType());              // type, 1
        out.writeByte(msg.getCodec());             // codec, 1
        out.writeByte(msg.getCompress());          // compress, 1
        out.writeLong(msg.getRequestId());         // requestId, 8

        // 2.心跳无body
        if (msg.isHeartbeat()) {
            out.writeInt(0);                       // length = 0
            log.debug("[RpcEncoder] type=HEARTBEAT, requestId={}, len=0", msg.getRequestId());
            return;
        }

        // 3.按codec选Serializer序列化payload（面向接口 + 工厂，绝不new具体实现）
        Serializer serializer = SerializerFactory.getByCodec(msg.getCodec());
        byte[] body = serializer.serialize(msg.getPayload());
        String serializerName = serializer.getClass().getSimpleName();
        int rawLen = body.length;

        // 4.按compress压缩（M1默认NONE）
        body = Compressor.compress(msg.getCompress(), body);
        int compressedLen = body.length;

        // 5.写length + body
        out.writeInt(body.length);                 // length, 4
        out.writeBytes(body);                      // body, N

        log.debug("[RpcEncoder] type={}, requestId={}, codec={}, compress={}, serializer={}, rawBytes={}, wireBytes={}, channel={}",
                typeName(msg.getType()), msg.getRequestId(), msg.getCodec(), msg.getCompress(),
                serializerName, rawLen, compressedLen,
                ctx.channel().remoteAddress());
    }

    private static String typeName(byte type) {
        if (type == ProtocolConstants.TYPE_REQUEST) return "REQUEST";
        if (type == ProtocolConstants.TYPE_RESPONSE) return "RESPONSE";
        if (type == ProtocolConstants.TYPE_HEARTBEAT) return "HEARTBEAT";
        return "UNKNOWN(" + type + ")";
    }
}
