package io.github.nitouge.conduit.remoting.handshake;

import io.github.nitouge.conduit.protocol.ProtocolConstants;
import io.github.nitouge.conduit.protocol.RpcMessage;
import io.github.nitouge.conduit.protocol.codec.Compressor;
import io.github.nitouge.conduit.serialization.SerializerFactory;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.ssl.SslHandshakeCompletionEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * 服务端握手处理器：响应客户端的版本/能力协商，并在握手完成前拒绝业务帧。
 *
 * <p>协商规则：</p>
 * <ul>
 *   <li><b>版本</b>：在 {@code [MIN_SUPPORTED_VERSION, MAX_SUPPORTED_VERSION]} 与客户端提议版本之间取交集最高版；
 *       无交集则回 {@link ProtocolConstants#HANDSHAKE_NO_COMMON_VERSION} 并断开；</li>
 *   <li><b>codec</b>：服务端能加载该序列化器才接受，否则回 {@code UNSUPPORTED_CODEC}；</li>
 *   <li><b>压缩</b>：服务端支持该压缩算法才接受，否则回 {@code UNSUPPORTED_COMPRESS}。</li>
 * </ul>
 *
 * <p>安全默认：未握手就发业务帧的连接直接断开（协议违规），避免未协商的连接绕过版本/算法校验。</p>
 */
public class ServerHandshakeHandler extends ChannelInboundHandlerAdapter {

    private static final Logger log = LoggerFactory.getLogger(ServerHandshakeHandler.class);

    private final long timeoutMillis;

    private ScheduledFuture<?> timeoutTask;
    private boolean started;

    public ServerHandshakeHandler(long timeoutMillis) {
        this.timeoutMillis = timeoutMillis;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        // 启用TLS时等TLS握手完成再开始应用层握手计时，避免TLS握手慢被误判为协议握手超时
        if (ctx.pipeline().get(SslHandler.class) == null) {
            startTimeout(ctx);
        }
        super.channelActive(ctx);
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        if (evt instanceof SslHandshakeCompletionEvent sslEvent) {
            if (sslEvent.isSuccess()) {
                startTimeout(ctx);
            } else {
                log.warn("[ServerHandshake] TLS握手失败，关闭连接 {}: {}",
                        ctx.channel().remoteAddress(), String.valueOf(sslEvent.cause()));
                ctx.close();
            }
            return;
        }
        super.userEventTriggered(ctx, evt);
    }

    /** 连接建立（或TLS握手完成）后必须尽快握手；超时未握手则断开，避免僵尸连接占资源 */
    private void startTimeout(ChannelHandlerContext ctx) {
        if (started) {
            return;
        }
        started = true;
        timeoutTask = ctx.channel().eventLoop().schedule(() -> {
            if (!Handshake.isCompleted(ctx.channel())) {
                log.warn("[ServerHandshake] 握手超时({}ms)，关闭连接: {}", timeoutMillis, ctx.channel().remoteAddress());
                ctx.close();
            }
        }, timeoutMillis, TimeUnit.MILLISECONDS);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        if (!(msg instanceof RpcMessage rpcMessage)) {
            ctx.fireChannelRead(msg);
            return;
        }
        Channel channel = ctx.channel();

        // 心跳可在握手完成前到达（保活帧），交给心跳处理器
        if (rpcMessage.isHeartbeat()) {
            ctx.fireChannelRead(msg);
            return;
        }

        if (rpcMessage.isHandshakeAck() || rpcMessage.isHandshakeReject()) {
            log.warn("[ServerHandshake] 服务端收到了握手应答帧（角色错乱），忽略: {}", rpcMessage);
            return;
        }

        if (rpcMessage.isHandshakeRequest()) {
            handleHandshake(ctx, rpcMessage);
            return;
        }

        // 业务帧：必须已完成握手
        if (!Handshake.isCompleted(channel)) {
            log.warn("[ServerHandshake] 连接尚未完成握手就收到业务帧(type={})，关闭连接: {}",
                    ProtocolConstants.typeName(rpcMessage.getType()), channel.remoteAddress());
            ctx.close();
            return;
        }
        ctx.fireChannelRead(msg);
    }

    private void handleHandshake(ChannelHandlerContext ctx, RpcMessage offer) {
        Channel channel = ctx.channel();
        byte offeredVersion = offer.getVersion();
        byte offeredCodec = offer.getCodec();
        byte offeredCompress = offer.getCompress();

        // 幂等：重复握手直接回放已协商结果（重连重试场景下客户端可能重发）
        Negotiated existing = Handshake.negotiated(channel);
        if (existing != null) {
            log.debug("[ServerHandshake] 已完成握手，回放协商结果: {}", existing);
            writeAck(ctx, existing);
            return;
        }

        int negotiatedVersion = ProtocolConstants.negotiateVersion(offeredVersion);
        if (negotiatedVersion < 0) {
            reject(ctx, ProtocolConstants.HANDSHAKE_NO_COMMON_VERSION,
                    "无共同支持的协议版本: client=" + offeredVersion
                            + ", server=[" + ProtocolConstants.MIN_SUPPORTED_VERSION
                            + "," + ProtocolConstants.MAX_SUPPORTED_VERSION + "]");
            return;
        }
        try {
            SerializerFactory.getByCodec(offeredCodec);
        } catch (RuntimeException e) {
            reject(ctx, ProtocolConstants.HANDSHAKE_UNSUPPORTED_CODEC,
                    "不支持的序列化codec=" + offeredCodec);
            return;
        }
        if (!Compressor.isSupported(offeredCompress)) {
            reject(ctx, ProtocolConstants.HANDSHAKE_UNSUPPORTED_COMPRESS,
                    "不支持的压缩算法compress=" + offeredCompress);
            return;
        }

        Negotiated negotiated = new Negotiated((byte) negotiatedVersion, offeredCodec, offeredCompress);
        channel.attr(Handshake.NEGOTIATED).set(negotiated);
        cancelTimeout();
        log.info("[ServerHandshake] 握手成功 remote={}, {}", channel.remoteAddress(), negotiated);
        writeAck(ctx, negotiated);
    }

    private void writeAck(ChannelHandlerContext ctx, Negotiated negotiated) {
        ctx.channel().writeAndFlush(RpcMessage.handshakeAck(negotiated.getVersion(),
                        negotiated.getCodec(), negotiated.getCompress()))
                .addListener((ChannelFutureListener) f -> {
                    if (!f.isSuccess()) {
                        log.warn("[ServerHandshake] 握手ACK发送失败，关闭连接: {}", ctx.channel().remoteAddress(),
                                f.cause());
                        ctx.close();
                    }
                });
    }

    private void reject(ChannelHandlerContext ctx, byte status, String reason) {
        log.warn("[ServerHandshake] 拒绝握手({}): {}，remote={}",
                ProtocolConstants.handshakeStatusText(status), reason, ctx.channel().remoteAddress());
        ctx.channel().writeAndFlush(RpcMessage.handshakeReject(status, ProtocolConstants.MAX_SUPPORTED_VERSION))
                .addListener(ChannelFutureListener.CLOSE);
    }

    private void cancelTimeout() {
        if (timeoutTask != null) {
            timeoutTask.cancel(false);
            timeoutTask = null;
        }
    }
}
