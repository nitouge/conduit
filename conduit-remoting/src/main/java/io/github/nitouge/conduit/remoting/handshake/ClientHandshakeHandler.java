package io.github.nitouge.conduit.remoting.handshake;

import io.github.nitouge.conduit.common.exception.RpcException;
import io.github.nitouge.conduit.protocol.ProtocolConstants;
import io.github.nitouge.conduit.protocol.RpcMessage;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.ssl.SslHandshakeCompletionEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * 客户端握手处理器：连接激活后立即发起版本/能力协商，只有收到ACK后该连接才允许承载业务请求。
 *
 * <p>协商内容与协议头字段复用：握手帧的 version/codec/compress 即"提议值"，
 * ACK帧的 version/codec/compress 即"协商结果"（版本取交集最高版，codec/compress取服务端接受值）。</p>
 *
 * <p>失败路径：拒绝/超时/连接中途断开都会让 {@link Handshake#FUTURE} 异常完成并关闭连接，
 * 由连接池把失败传给等待中的调用方，绝不会出现"连接可用但协议不一致"的幻影连接。</p>
 */
public class ClientHandshakeHandler extends ChannelInboundHandlerAdapter {

    private static final Logger log = LoggerFactory.getLogger(ClientHandshakeHandler.class);

    private final byte offeredVersion;
    private final byte offeredCodec;
    private final byte offeredCompress;
    private final long timeoutMillis;

    private ScheduledFuture<?> timeoutTask;
    private boolean started;

    public ClientHandshakeHandler(byte offeredVersion, byte offeredCodec, byte offeredCompress, long timeoutMillis) {
        this.offeredVersion = offeredVersion;
        this.offeredCodec = offeredCodec;
        this.offeredCompress = offeredCompress;
        this.timeoutMillis = timeoutMillis;
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) {
        channelFuture(ctx.channel());
    }

    /** 取/惰性创建该连接的握手结果future（连接池据此等待握手完成） */
    public static CompletableFuture<Negotiated> channelFuture(Channel channel) {
        CompletableFuture<Negotiated> future = channel.attr(Handshake.FUTURE).get();
        if (future == null) {
            future = new CompletableFuture<>();
            CompletableFuture<Negotiated> prev = channel.attr(Handshake.FUTURE).setIfAbsent(future);
            if (prev != null) {
                future = prev;
            }
        }
        return future;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) {
        // 启用TLS时，应用层握手必须等TLS握手完成后再开始（否则明文/密文时序错乱，
        // 且TLS握手慢的客户端会被误判为协议握手超时）
        if (ctx.pipeline().get(SslHandler.class) == null) {
            begin(ctx);
        }
        ctx.fireChannelActive();
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) {
        if (evt instanceof SslHandshakeCompletionEvent sslEvent) {
            if (sslEvent.isSuccess()) {
                begin(ctx);
            } else {
                Channel channel = ctx.channel();
                fail(channelFuture(channel), channel, "TLS握手失败: " + sslEvent.cause());
                channel.close();
            }
            return;
        }
        ctx.fireUserEventTriggered(evt);
    }

    /** 开始应用层握手：挂超时任务 + 发出提议帧 */
    private void begin(ChannelHandlerContext ctx) {
        if (started) {
            return;
        }
        started = true;
        Channel channel = ctx.channel();
        CompletableFuture<Negotiated> future = channelFuture(channel);

        // 握手超时：到点仍未完成则失败该连接（避免连接建立后永远停在不一致状态）
        timeoutTask = channel.eventLoop().schedule(() -> {
            if (!future.isDone()) {
                String msg = "协议握手超时(" + timeoutMillis + "ms): " + channel.remoteAddress();
                log.warn(msg);
                future.completeExceptionally(new RpcException(msg));
                channel.close();
            }
        }, timeoutMillis, TimeUnit.MILLISECONDS);
        future.whenComplete((negotiated, error) -> cancelTimeout());

        log.debug("[ClientHandshake] 发起握手 version={}, codec={}, compress={}, remote={}",
                offeredVersion, offeredCodec, offeredCompress, channel.remoteAddress());
        channel.writeAndFlush(RpcMessage.handshake(offeredVersion, offeredCodec, offeredCompress));
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        if (!(msg instanceof RpcMessage rpcMessage) || !rpcMessage.isHandshake()) {
            ctx.fireChannelRead(msg);
            return;
        }
        Channel channel = ctx.channel();
        CompletableFuture<Negotiated> future = channelFuture(channel);

        if (rpcMessage.isHandshakeAck()) {
            Negotiated negotiated = new Negotiated(rpcMessage.getVersion(),
                    rpcMessage.getCodec(), rpcMessage.getCompress());
            if (rpcMessage.getStatus() != ProtocolConstants.HANDSHAKE_OK) {
                fail(future, channel, "服务端握手应答状态异常: "
                        + ProtocolConstants.handshakeStatusText(rpcMessage.getStatus()));
                return;
            }
            if (!ProtocolConstants.isSupportedVersion(negotiated.getVersion())) {
                fail(future, channel, "服务端协商出的协议版本不被本端支持: " + negotiated.getVersion());
                return;
            }
            channel.attr(Handshake.NEGOTIATED).set(negotiated);
            log.info("[ClientHandshake] 握手成功 remote={}, {}", channel.remoteAddress(), negotiated);
            future.complete(negotiated);
            return;
        }

        if (rpcMessage.isHandshakeReject()) {
            fail(future, channel, "服务端拒绝握手("
                    + ProtocolConstants.handshakeStatusText(rpcMessage.getStatus())
                    + ")，服务端支持的最高协议版本=" + rpcMessage.getVersion());
            return;
        }

        // 客户端不应收到握手请求；忽略并记录，避免把控制帧当成业务帧往下传
        log.debug("[ClientHandshake] 忽略非预期的握手帧: {}", rpcMessage);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        fail(channelFuture(ctx.channel()), ctx.channel(), "握手期间连接关闭: " + ctx.channel().remoteAddress());
        super.channelInactive(ctx);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        fail(channelFuture(ctx.channel()), ctx.channel(), "握手期间连接异常: " + cause);
        ctx.close();
    }

    private void fail(CompletableFuture<Negotiated> future, Channel channel, String reason) {
        if (!future.isDone()) {
            log.warn("[ClientHandshake] {} ({})", reason, channel.remoteAddress());
            future.completeExceptionally(new RpcException(reason));
        }
        // 握手失败的连接绝不能留在池里继续承载业务帧
        channel.close();
    }

    private void cancelTimeout() {
        if (timeoutTask != null) {
            timeoutTask.cancel(false);
            timeoutTask = null;
        }
    }
}
