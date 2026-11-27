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
 *
 * <p>稳健性约定（底层通信的"防御边界"）：</p>
 * <ul>
 *   <li>魔数/版本/类型/长度任一非法 -> 关闭连接并抛协议异常（不猜测、不静默丢弃）；</li>
 *   <li>握手帧的version允许超出本端点支持区间（否则服务端无法"礼貌拒绝"未来版本，只能静默断链）；</li>
 *   <li>业务帧的version必须落在本端点支持区间，否则同样是协议违规；</li>
 *   <li>请求body反序列化失败 -> 回一帧可序列化的错误响应（客户端快速失败，不必干等到超时）；
 *       响应body反序列化失败 -> 关闭连接（无法关联，交由客户端在途请求统一失败）。</li>
 * </ul>
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
            log.warn("[RpcDecoder] 非法的 magic=0x{} 来自 {}, 关连接",
                    Integer.toHexString(magic & 0xffff), ctx.channel().remoteAddress());
            ctx.close();                 // 非法帧，关闭连接
            throw new RpcException("非法的协议魔数: " + Integer.toHexString(magic & 0xffff));
        }
        byte version = in.readByte();
        byte type = in.readByte();
        byte codec = in.readByte();
        byte compress = in.readByte();
        long requestId = in.readLong();
        int length = in.readInt();

        // 1.1长度字段防御：上界防超大帧，下界防负数（负数会直接让new byte[]抛NegativeArraySize）
        if (length < 0 || length > ProtocolConstants.MAX_FRAME_LENGTH) {
            in.resetReaderIndex();
            log.warn("[RpcDecoder] 非法 length={} (允许区间[0, {}]) 来自 {}, 关连接",
                    length, ProtocolConstants.MAX_FRAME_LENGTH, ctx.channel().remoteAddress());
            ctx.close();
            throw new RpcException("非法的帧长度: " + length);
        }
        // 上游已切帧，此处只是防御：不足一个完整body时等待更多数据
        if (in.readableBytes() < length) {
            in.resetReaderIndex();
            return;
        }

        RpcMessage msg = new RpcMessage();
        msg.setVersion(version);
        msg.setType(type);
        msg.setCodec(codec);
        msg.setCompress(compress);
        msg.setRequestId(requestId);

        // 2.心跳：body必须为空（兼容异常对端：有body则跳过，不能留在缓冲区污染后续帧）
        if (msg.isHeartbeat()) {
            if (length != 0) {
                log.warn("[RpcDecoder] 心跳帧带有{}字节body，已忽略（requestId={}）", length, requestId);
                in.skipBytes(length);
            }
            log.debug("[RpcDecoder] type=HEARTBEAT, requestId={}", requestId);
            out.add(msg);
            return;
        }

        // 3.握手：body固定1字节status；控制帧不走序列化器
        if (msg.isHandshake()) {
            if (length != 1) {
                in.resetReaderIndex();
                log.warn("[RpcDecoder] 握手帧长度非法 length={}, 关连接", length);
                ctx.close();
                throw new RpcException("握手帧长度非法: " + length);
            }
            msg.setStatus(in.readByte());
            log.debug("[RpcDecoder] type={}, version={}, codec={}, compress={}, status={}",
                    ProtocolConstants.typeName(type), version, codec, compress,
                    ProtocolConstants.handshakeStatusText(msg.getStatus()));
            out.add(msg);
            return;
        }

        // 4.取消帧：body固定1字节原因，控制帧不走序列化器
        if (msg.isCancel()) {
            if (!ProtocolConstants.isSupportedVersion(version)) {
                in.resetReaderIndex();
                log.warn("[RpcDecoder] 取消帧协议版本不支持 version={}，关连接", version);
                ctx.close();
                throw new RpcException("不支持的协议版本: " + version);
            }
            if (length != 1) {
                in.resetReaderIndex();
                log.warn("[RpcDecoder] 取消帧长度非法 length={}, 关连接", length);
                ctx.close();
                throw new RpcException("取消帧长度非法: " + length);
            }
            msg.setStatus(in.readByte());
            log.debug("[RpcDecoder] type=CANCEL, requestId={}, reason={}",
                    requestId, ProtocolConstants.cancelReasonText(msg.getStatus()));
            out.add(msg);
            return;
        }

        // 5.业务帧：类型/版本必须合法
        if (type != ProtocolConstants.TYPE_REQUEST && type != ProtocolConstants.TYPE_RESPONSE) {
            in.resetReaderIndex();
            log.warn("[RpcDecoder] 非法 type={}, 关连接", type);
            ctx.close();
            throw new RpcException("非法消息类型: " + type);
        }
        if (!ProtocolConstants.isSupportedVersion(version)) {
            in.resetReaderIndex();
            log.warn("[RpcDecoder] 不支持的协议版本 version={}（支持区间[{}, {}]），关连接",
                    version, ProtocolConstants.MIN_SUPPORTED_VERSION, ProtocolConstants.MAX_SUPPORTED_VERSION);
            ctx.close();
            throw new RpcException("不支持的协议版本: " + version);
        }

        // 4.1序列化器/压缩算法必须是已知的：未知属于协议违规，直接断开（无法保证对端能读懂错误响应）
        Serializer serializer;
        try {
            serializer = SerializerFactory.getByCodec(codec);
        } catch (RuntimeException e) {
            in.skipBytes(length);
            log.warn("[RpcDecoder] 未知的codec={}（requestId={}），关连接", codec, requestId, e);
            ctx.close();
            throw new RpcException("未知的序列化codec: " + codec, e);
        }
        if (!Compressor.isSupported(compress)) {
            in.skipBytes(length);
            log.warn("[RpcDecoder] 未知的compress={}（requestId={}），关连接", compress, requestId);
            ctx.close();
            throw new RpcException("未知的压缩类型: " + compress);
        }

        // 4.2读body + 解压（解压失败=可疑/损坏报文，断开）
        byte[] body = new byte[length];
        in.readBytes(body);
        if (compress != ProtocolConstants.COMPRESS_NONE) {
            try {
                body = Compressor.decompress(compress, body);
            } catch (RuntimeException e) {
                log.warn("[RpcDecoder] 解压失败 requestId={}, compress={}，关连接", requestId, compress, e);
                ctx.close();
                return;
            }
        }

        // 4.3反序列化：请求失败回错误响应；响应失败只能断开
        Class<?> clazz = (type == ProtocolConstants.TYPE_REQUEST) ? RpcRequest.class : RpcResponse.class;
        try {
            msg.setPayload(serializer.deserialize(body, clazz));
        } catch (Throwable t) {
            if (type == ProtocolConstants.TYPE_REQUEST) {
                log.warn("[RpcDecoder] 请求反序列化失败 requestId={}, codec={}，回传错误响应",
                        requestId, codec, t);
                writeDecodeErrorResponse(ctx, version, codec, requestId, t);
            } else {
                log.warn("[RpcDecoder] 响应反序列化失败 requestId={}, codec={}，关连接", requestId, codec, t);
                ctx.close();
            }
            return;
        }

        log.debug("[RpcDecoder] type={}, version={}, requestId={}, codec={}, compress={}, serializer={}, wireBytes={}, payloadType={}, channel={}",
                ProtocolConstants.typeName(type), version, requestId, codec, compress,
                serializer.getClass().getSimpleName(), length, clazz.getSimpleName(),
                ctx.channel().remoteAddress());

        out.add(msg);
    }

    /**
     * 请求帧解码失败时回一帧可序列化的错误响应：让客户端立即拿到"请求无法被理解"的原因，
     * 而不是干等到调用超时。响应帧使用请求头的requestId与codec，compress固定NONE（错误报文很小，无需压缩）。
     */
    private static void writeDecodeErrorResponse(ChannelHandlerContext ctx, byte version, byte codec,
                                                 long requestId, Throwable cause) {
        RpcResponse error = RpcResponse.error(new RpcException("请求解码失败: " + cause.getMessage(), cause));
        RpcMessage response = RpcMessage.response(requestId, codec, error);
        response.setVersion(version);
        response.setCompress(ProtocolConstants.COMPRESS_NONE);
        ctx.channel().writeAndFlush(response).addListener(f -> {
            if (!f.isSuccess()) {
                log.warn("[RpcDecoder] 回传解码错误响应失败 requestId={}，关连接: {}",
                        requestId, String.valueOf(f.cause()));
                ctx.close();
            }
        });
    }
}
