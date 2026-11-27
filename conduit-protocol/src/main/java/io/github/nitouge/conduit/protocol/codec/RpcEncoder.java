package io.github.nitouge.conduit.protocol.codec;

import io.github.nitouge.conduit.common.exception.RpcException;
import io.github.nitouge.conduit.protocol.ProtocolConstants;
import io.github.nitouge.conduit.protocol.ProtocolOptions;
import io.github.nitouge.conduit.protocol.RpcMessage;
import io.github.nitouge.conduit.serialization.Serializer;
import io.github.nitouge.conduit.serialization.SerializerFactory;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

/**
 * 出站编码器：对象 -> 字节。先写18字节定长头，再写（序列化 + 压缩后的）body。
 *
 * <p>按codec字段选择 {@link Serializer}，<b>绝不出现任何具体序列化类名</b>——这正是序列化可替换在协议层的落地。</p>
 *
 * <p>控制帧：心跳无body（length=0）；握手帧body固定1字节status（协商结果见头里的
 * version/codec/compress，不走序列化器，避免"协商前还不知道用哪个序列化器"的死循环）；
 * 取消帧body固定1字节取消原因，同样不走序列化器。</p>
 *
 * <p>压缩策略：{@code msg.compress}给出期望算法，只有body达到阈值时才真正压缩，否则头里回填
 * {@link ProtocolConstants#COMPRESS_NONE}——接收端永远以头里的值为准，不会读错。</p>
 */
public class RpcEncoder extends MessageToByteEncoder<RpcMessage> {

    private static final Logger log = LoggerFactory.getLogger(RpcEncoder.class);

    private final ProtocolOptions options;

    /** 使用默认协议配置 */
    public RpcEncoder() {
        this(ProtocolOptions.defaults());
    }

    public RpcEncoder(ProtocolOptions options) {
        this.options = Objects.requireNonNull(options, "options");
    }

    @Override
    protected void encode(ChannelHandlerContext ctx, RpcMessage msg, ByteBuf out) {
        // 1.心跳：无body
        if (msg.isHeartbeat()) {
            writeHeader(out, msg.getVersion(), msg.getType(), msg.getCodec(),
                    ProtocolConstants.COMPRESS_NONE, msg.getRequestId(), 0);
            log.debug("[RpcEncoder] type=HEARTBEAT, requestId={}, channel={}",
                    msg.getRequestId(), ctx.channel().remoteAddress());
            return;
        }

        // 1.1取消帧：body仅1字节原因，不依赖任何序列化器（被取消的请求可能还没完成，与codec无关）
        if (msg.isCancel()) {
            writeHeader(out, msg.getVersion(), msg.getType(), msg.getCodec(),
                    ProtocolConstants.COMPRESS_NONE, msg.getRequestId(), 1);
            out.writeByte(msg.getStatus());
            log.debug("[RpcEncoder] type=CANCEL, requestId={}, reason={}, channel={}",
                    msg.getRequestId(), ProtocolConstants.cancelReasonText(msg.getStatus()),
                    ctx.channel().remoteAddress());
            return;
        }

        // 2.握手：body仅1字节status，不依赖任何序列化器；头里的compress承载"压缩能力提议"
        if (msg.isHandshake()) {
            writeHeader(out, msg.getVersion(), msg.getType(), msg.getCodec(),
                    msg.getCompress(), msg.getRequestId(), 1);
            out.writeByte(msg.getStatus());
            log.debug("[RpcEncoder] type={}, version={}, codec={}, compress={}, status={}, channel={}",
                    ProtocolConstants.typeName(msg.getType()), msg.getVersion(), msg.getCodec(),
                    msg.getCompress(), ProtocolConstants.handshakeStatusText(msg.getStatus()),
                    ctx.channel().remoteAddress());
            return;
        }

        // 3.业务帧
        if (msg.getPayload() == null) {
            throw new RpcException("业务帧缺少payload: " + msg);
        }
        Serializer serializer;
        try {
            serializer = SerializerFactory.getByCodec(msg.getCodec());
        } catch (RuntimeException e) {
            throw new RpcException("无法加载序列化器 codec=" + msg.getCodec()
                    + "（requestId=" + msg.getRequestId() + "）", e);
        }

        // 3.1 序列化payload（面向接口 + 工厂，绝不new具体实现）
        byte[] body;
        try {
            body = serializer.serialize(msg.getPayload());
        } catch (RuntimeException e) {
            throw new RpcException("序列化失败 codec=" + msg.getCodec()
                    + "（requestId=" + msg.getRequestId() + "，payload="
                    + msg.getPayload().getClass().getName() + "）", e);
        }
        int rawLength = body.length;

        // 3.2 按compress压缩：小报文不压缩，头里如实回填实际算法
        // 先校验算法是否已知：未知算法即使因阈值未触发压缩也必须报错，不能静默降级
        if (!Compressor.isSupported(msg.getCompress())) {
            throw new RpcException("未知的压缩类型: " + msg.getCompress()
                    + "（requestId=" + msg.getRequestId() + "）");
        }
        byte compress = options.shouldCompress(msg.getCompress(), rawLength)
                ? msg.getCompress() : ProtocolConstants.COMPRESS_NONE;
        if (compress != ProtocolConstants.COMPRESS_NONE) {
            body = Compressor.compress(compress, body);
        }
        if (body.length > ProtocolConstants.MAX_FRAME_LENGTH) {
            throw new RpcException("body超出单帧上限(" + ProtocolConstants.MAX_FRAME_LENGTH
                    + "字节): " + body.length + "（requestId=" + msg.getRequestId() + "）");
        }

        // 3.3 写头 + body
        writeHeader(out, msg.getVersion(), msg.getType(), msg.getCodec(), compress,
                msg.getRequestId(), body.length);
        out.writeBytes(body);

        log.debug("[RpcEncoder] type={}, version={}, requestId={}, codec={}, compress={}, serializer={}, rawBytes={}, wireBytes={}, channel={}",
                ProtocolConstants.typeName(msg.getType()), msg.getVersion(), msg.getRequestId(),
                msg.getCodec(), compress, serializer.getClass().getSimpleName(),
                rawLength, body.length, ctx.channel().remoteAddress());
    }

    /** 写18字节定长头（字段顺序必须与偏移表一致） */
    private static void writeHeader(ByteBuf out, byte version, byte type, byte codec, byte compress,
                                    long requestId, int length) {
        out.writeShort(ProtocolConstants.MAGIC);   // magic, 2
        out.writeByte(version);                    // version, 1
        out.writeByte(type);                       // type, 1
        out.writeByte(codec);                      // codec, 1
        out.writeByte(compress);                   // compress, 1
        out.writeLong(requestId);                  // requestId, 8
        out.writeInt(length);                      // length, 4
    }
}
