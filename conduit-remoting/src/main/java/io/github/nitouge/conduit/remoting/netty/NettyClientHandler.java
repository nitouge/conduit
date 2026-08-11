package io.github.nitouge.conduit.remoting.netty;

import io.github.nitouge.conduit.api.RpcResponse;
import io.github.nitouge.conduit.common.exception.RpcException;
import io.github.nitouge.conduit.protocol.ProtocolConstants;
import io.github.nitouge.conduit.protocol.RpcMessage;
import io.github.nitouge.conduit.remoting.PendingRequests;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import org.apache.commons.collections4.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;

/**
 * 客户端业务Handler：收到响应帧后按requestId唤醒等待中的future；
 * 写空闲时主动发心跳维持长连接活性。
 */
public class NettyClientHandler extends SimpleChannelInboundHandler<RpcMessage> {

    private static final Logger log = LoggerFactory.getLogger(NettyClientHandler.class);

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, RpcMessage msg) {
        if (msg.isHeartbeat()) {
            log.debug("[NettyClientHandler] 收到心跳响应 from {}", ctx.channel().remoteAddress());
            return; // 心跳响应，忽略
        }
        // 响应帧：按协议头requestId关联回对应的future
        RpcResponse response = (RpcResponse) msg.getPayload();
        Set<Long> inFlight = ctx.channel().attr(NettyClient.IN_FLIGHT).get();
        if (inFlight != null) {
            inFlight.remove(msg.getRequestId());
        }
        log.debug("[NettyClientHandler] 收到响应 requestId={}, hasException={}, from {}",
                msg.getRequestId(), response.hasException(), ctx.channel().remoteAddress());
        PendingRequests.complete(msg.getRequestId(), response);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        // 连接断开：把这条连接上所有已写出、未收到响应的请求统一失败，
        // 避免调用方干等到超时、也避免PendingRequests永久泄漏。
        Set<Long> inFlight = ctx.channel().attr(NettyClient.IN_FLIGHT).get();
        if (CollectionUtils.isNotEmpty(inFlight)) {
            log.debug("[NettyClientHandler] 连接断开，失败 {} 个在途请求，remote={}", inFlight.size(), ctx.channel().remoteAddress());
            RpcException cause = new RpcException("连接已关闭: " + ctx.channel().remoteAddress());
            for (Long id : inFlight) {
                PendingRequests.completeExceptionally(id, cause);
            }
            inFlight.clear();
        }
        super.channelInactive(ctx);
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) {
        // 写空闲：一段时间没写出数据，发一帧心跳保活
        if (evt instanceof IdleStateEvent && ((IdleStateEvent) evt).state() == IdleState.WRITER_IDLE) {
            log.debug("[NettyClientHandler] 写空闲触发，发心跳 to {}", ctx.channel().remoteAddress());
            ctx.channel().writeAndFlush(RpcMessage.heartbeat(0L, ProtocolConstants.CODEC_JDK));
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.warn("客户端连接异常，关闭: {}", ctx.channel().remoteAddress(), cause);
        ctx.close();
    }
}
