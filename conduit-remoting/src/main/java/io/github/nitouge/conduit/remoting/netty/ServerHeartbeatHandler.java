package io.github.nitouge.conduit.remoting.netty;

import io.github.nitouge.conduit.protocol.RpcMessage;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.LongAdder;

/**
 * 服务端心跳处理器：收到PING立即回PONG（保持客户端读空闲计时器被刷新），
 * 并对"长时间收不到任何帧"的连接做读空闲清理。
 *
 * <p>服务端不主动发心跳：它只需要回应客户端的PING即可证明"我还在"，同时用读空闲判定
 * 对端是否早已消失（客户端宕机、网络断开），及时释放连接与线程资源。</p>
 */
public class ServerHeartbeatHandler extends ChannelInboundHandlerAdapter {

    private static final Logger log = LoggerFactory.getLogger(ServerHeartbeatHandler.class);

    private final long readIdleTimeoutMillis;
    private final LongAdder heartbeatsReceived;

    public ServerHeartbeatHandler(long readIdleTimeoutMillis, LongAdder heartbeatsReceived) {
        this.readIdleTimeoutMillis = readIdleTimeoutMillis;
        this.heartbeatsReceived = heartbeatsReceived;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        if (msg instanceof RpcMessage rpcMessage && rpcMessage.isHeartbeat()) {
            heartbeatsReceived.increment();
            RpcMessage pong = RpcMessage.heartbeat(rpcMessage.getRequestId(), rpcMessage.getCodec());
            pong.setVersion(rpcMessage.getVersion());
            log.debug("[ServerHeartbeat] 收到PING，回PONG from {}", ctx.channel().remoteAddress());
            ctx.channel().writeAndFlush(pong).addListener(f -> {
                if (!f.isSuccess()) {
                    log.debug("[ServerHeartbeat] PONG发送失败，关闭连接 {}: {}",
                            ctx.channel().remoteAddress(), String.valueOf(f.cause()));
                    ctx.close();
                }
            });
            return;
        }
        ctx.fireChannelRead(msg);
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) {
        if (evt instanceof IdleStateEvent idleEvent && idleEvent.state() == IdleState.READER_IDLE) {
            log.info("[ServerHeartbeat] 读空闲超过{}ms，关闭疑似失联连接: {}",
                    readIdleTimeoutMillis, ctx.channel().remoteAddress());
            ctx.close();
            return;
        }
        ctx.fireUserEventTriggered(evt);
    }
}
