package io.github.nitouge.conduit.remoting.netty;

import io.github.nitouge.conduit.api.RpcResponse;
import io.github.nitouge.conduit.common.exception.RpcException;
import io.github.nitouge.conduit.protocol.ProtocolConstants;
import io.github.nitouge.conduit.protocol.RpcMessage;
import io.github.nitouge.conduit.remoting.PendingRequests;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.concurrent.atomic.LongAdder;

/**
 * 客户端业务Handler：收到响应帧后按requestId唤醒等待中的future。
 *
 * <p>心跳/握手等控制帧在上游处理器已被消费，这里只会收到业务响应；若收到非响应帧说明对端角色错乱
 * 或协议实现有误，直接断开而不是把错误数据塞给等待中的调用方。</p>
 *
 * <p>连接断开时把该连接上所有在途请求统一失败，避免调用方干等到超时、也避免关联表永久泄漏。</p>
 */
public class NettyClientHandler extends SimpleChannelInboundHandler<RpcMessage> {

    private static final Logger log = LoggerFactory.getLogger(NettyClientHandler.class);

    private final LongAdder responsesReceived;

    public NettyClientHandler(LongAdder responsesReceived) {
        this.responsesReceived = responsesReceived;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, RpcMessage msg) {
        if (!(msg.getPayload() instanceof RpcResponse response)) {
            log.warn("[NettyClientHandler] 收到非响应帧(type={}, requestId={})，关闭连接: {}",
                    ProtocolConstants.typeName(msg.getType()), msg.getRequestId(), ctx.channel().remoteAddress());
            ctx.close();
            return;
        }

        responsesReceived.increment();

        // 响应帧：按协议头requestId关联回对应的future
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
        Set<Long> inFlight = ctx.channel().attr(NettyClient.IN_FLIGHT).get();
        if (inFlight != null && !inFlight.isEmpty()) {
            log.debug("[NettyClientHandler] 连接断开，失败 {} 个在途请求，remote={}",
                    inFlight.size(), ctx.channel().remoteAddress());
            RpcException cause = new RpcException("连接已关闭: " + ctx.channel().remoteAddress());
            for (Long id : inFlight) {
                PendingRequests.completeExceptionally(id, cause);
            }
            inFlight.clear();
        }
        super.channelInactive(ctx);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.warn("客户端连接异常，关闭: {}", ctx.channel().remoteAddress(), cause);
        ctx.close();
    }
}
