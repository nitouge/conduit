package io.github.nitouge.conduit.protocol.codec;

import io.github.nitouge.conduit.api.RpcRequest;
import io.github.nitouge.conduit.api.RpcResponse;
import io.github.nitouge.conduit.common.exception.RpcException;
import io.github.nitouge.conduit.protocol.ProtocolConstants;
import io.github.nitouge.conduit.protocol.RpcMessage;
import io.github.nitouge.conduit.serialization.Serializer;
import io.github.nitouge.conduit.serialization.SerializerFactory;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * 入站解码器：字节 -> {@link RpcMessage}。
 *
 * <p>进入本解码器的ByteBuf已由上游 {@link LengthFieldBasedFrameDecoder} 切成一个完整帧，
 * 可放心顺序读取。先读头，再按codec选 {@link Serializer}、按type决定还原成请求还是响应。</p>
 */
public class RpcDecoder extends ByteToMessageDecoder {

    private static final Logger log = LoggerFactory.getLogger(RpcDecoder.class);

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) {
        // 防御性检查：至少要有完整的头
        if (in.readableBytes() < ProtocolConstants.HEADER_LENGTH) {
            return;
        }
        in.markReaderIndex();

        // 1.读头
        short magic = in.readShort();
        if (magic != ProtocolConstants.MAGIC) {
            in.resetReaderIndex();
            log.warn("[RpcDecoder] 非法的 magic=0x{} 来自 {}, 关连接", Integer.toHexString(magic & 0xffff), ctx.channel().remoteAddress());
            ctx.close();                 // 非法帧，关闭连接
            return;
        }
        byte version = in.readByte();    // 预留：版本兼容校验
        byte type = in.readByte();
        byte codec = in.readByte();
        byte compress = in.readByte();
        long requestId = in.readLong();
        int length = in.readInt();

        RpcMessage msg = new RpcMessage();
        msg.setType(type);
        msg.setCodec(codec);
        msg.setCompress(compress);
        msg.setRequestId(requestId);

        // 2.心跳无body
        if (type == ProtocolConstants.TYPE_HEARTBEAT) {
            log.debug("[RpcDecoder] type=HEARTBEAT, requestId={}", requestId);
            out.add(msg);
            return;
        }

        // 3.按type决定body应还原成哪种对象；非法type立即拒绝（明确报错 + 关连接，便于排障），心跳已在上面提前return，这里只可能是请求/响应。
        Class<?> clazz;
        switch (type) {
            case ProtocolConstants.TYPE_REQUEST:
                clazz = RpcRequest.class;
                break;
            case ProtocolConstants.TYPE_RESPONSE:
                clazz = RpcResponse.class;
                break;
            default:
                in.resetReaderIndex();
                log.warn("[RpcDecoder] 非法 type={}, 关连接", type);
                ctx.close();
                throw new RpcException("非法消息类型: " + type);
        }

        // 4.读body + 解压
        byte[] body = new byte[length];
        in.readBytes(body);
        body = Compressor.decompress(compress, body);

        // 5.按codec选Serializer反序列化
        Serializer serializer = SerializerFactory.getByCodec(codec);
        Object payload = serializer.deserialize(body, clazz);
        msg.setPayload(payload);

        log.debug("[RpcDecoder] type={}, requestId={}, codec={}, compress={}, serializer={}, wireBytes={}, payloadType={}, channel={}",
                typeName(type), requestId, codec, compress,
                serializer.getClass().getSimpleName(), length,
                clazz.getSimpleName(),
                ctx.channel().remoteAddress());

        out.add(msg);
    }

    private static String typeName(byte type) {
        if (type == ProtocolConstants.TYPE_REQUEST) return "REQUEST";
        if (type == ProtocolConstants.TYPE_RESPONSE) return "RESPONSE";
        if (type == ProtocolConstants.TYPE_HEARTBEAT) return "HEARTBEAT";
        return "UNKNOWN(" + type + ")";
    }
}
