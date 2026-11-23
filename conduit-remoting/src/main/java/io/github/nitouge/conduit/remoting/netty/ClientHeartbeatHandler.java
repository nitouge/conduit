package io.github.nitouge.conduit.remoting.netty;

import io.github.nitouge.conduit.protocol.RpcMessage;
import io.github.nitouge.conduit.remoting.handshake.Handshake;
import io.github.nitouge.conduit.remoting.handshake.Negotiated;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.LongAdder;

/**
 * 客户端心跳处理器：写空闲时发PING，读空闲超时判定对端失联并断开。
 *
 * <p>为什么需要读空闲探活：TCP在"对端主机断电/中间设备静默丢包"时不会主动通知应用，
 * 长连接会一直处于"半开"状态——调用方继续写、却永远等不到响应，只能逐个请求超时。
 * 因此客户端在写空闲时主动发PING，服务端收到后回PONG；只要读空闲计时器被PONG刷新，
 * 就证明链路双向可用。心跳间隔与读空闲阈值均可通过 {@code ClientOptions} 配置。</p>
 *
 * <p>心跳帧（type=HEARTBEAT，length=0）不进入业务关联表，开销极小；只在握手完成后发送，
 * 避免在协议尚未协商时把控制帧发出去。</p>
 */
public class ClientHeartbeatHandler extends ChannelInboundHandlerAdapter {

    private static final Logger log = LoggerFactory.getLogger(ClientHeartbeatHandler.class);

    private final long readIdleTimeoutMillis;
    private final LongAdder heartbeatsSent;
    private final LongAdder heartbeatsReceived;

    public ClientHeartbeatHandler(long readIdleTimeoutMillis,
                                  LongAdder heartbeatsSent, LongAdder heartbeatsReceived) {
        this.readIdleTimeoutMillis = readIdleTimeoutMillis;
        this.heartbeatsSent = heartbeatsSent;
        this.heartbeatsReceived = heartbeatsReceived;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        if (msg instanceof RpcMessage rpcMessage && rpcMessage.isHeartbeat()) {
            heartbeatsReceived.increment();
            log.debug("[ClientHeartbeat] 收到PONG from {}", ctx.channel().remoteAddress());
            return;
        }
        ctx.fireChannelRead(msg);
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) {
        if (!(evt instanceof IdleStateEvent idleEvent)) {
            ctx.fireUserEventTriggered(evt);
            return;
        }
        if (idleEvent.state() == IdleState.WRITER_IDLE) {
            sendHeartbeat(ctx);
            return;
        }
        if (idleEvent.state() == IdleState.READER_IDLE) {
            log.warn("[ClientHeartbeat] 读空闲超过{}ms未收到任何帧，判定对端失联，关闭连接: {}",
                    readIdleTimeoutMillis, ctx.channel().remoteAddress());
            ctx.close();
        }
    }

    private void sendHeartbeat(ChannelHandlerContext ctx) {
        Negotiated negotiated = Handshake.negotiated(ctx.channel());
        if (negotiated == null) {
            log.debug("[ClientHeartbeat] 握手未完成，暂不发送心跳: {}", ctx.channel().remoteAddress());
            return;
        }
        RpcMessage ping = RpcMessage.heartbeat(0L, negotiated.getCodec());
        ping.setVersion(negotiated.getVersion());
        heartbeatsSent.increment();
        log.debug("[ClientHeartbeat] 写空闲触发，发送PING to {}", ctx.channel().remoteAddress());
        ctx.channel().writeAndFlush(ping).addListener(f -> {
            if (!f.isSuccess()) {
                log.debug("[ClientHeartbeat] PING发送失败 {}: {}",
                        ctx.channel().remoteAddress(), String.valueOf(f.cause()));
            }
        });
    }
}
