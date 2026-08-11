package io.github.nitouge.conduit.remoting.netty;

import io.github.nitouge.conduit.api.RpcRequest;
import io.github.nitouge.conduit.api.RpcResponse;
import io.github.nitouge.conduit.common.exception.RpcException;
import io.github.nitouge.conduit.protocol.RpcMessage;
import io.github.nitouge.conduit.remoting.RequestHandler;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;

/**
 * 服务端业务Handler：收到请求帧后，提交业务线程池调用 {@link RequestHandler}，
 * 再把结果包成响应帧（复用同一个requestId与codec）写回。
 *
 * <p>杜绝反射调用跑在Netty的I/O线程上，故统一派发到业务线程池。</p>
 */
public class NettyServerHandler extends SimpleChannelInboundHandler<RpcMessage> {

    private static final Logger log = LoggerFactory.getLogger(NettyServerHandler.class);

    private final RequestHandler handler;
    private final ExecutorService businessPool;

    public NettyServerHandler(RequestHandler handler, ExecutorService businessPool) {
        this.handler = handler;
        this.businessPool = businessPool;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, RpcMessage msg) {
        // 心跳：收到即可（重置读空闲计时），无需处理
        if (msg.isHeartbeat()) {
            log.debug("[NettyServerHandler] 收到心跳 from {}", ctx.channel().remoteAddress());
            return;
        }

        final long requestId = msg.getRequestId();
        final byte codec = msg.getCodec();
        final RpcRequest request = (RpcRequest) msg.getPayload();

        log.debug("[NettyServerHandler] 收到请求 requestId={}, {}#{}, from {}, 派发业务线程池",
                requestId, request.getInterfaceName(), request.getMethodName(),
                ctx.channel().remoteAddress());

        // 派发到业务线程池调用业务（避免阻塞I/O线程）。handle返回future：
        // 同步方法立即完成；返回CompletableFuture的异步方法不占住业务线程，响应在future完成时回写。
        try {
            businessPool.execute(() ->
                    handler.handle(request).whenComplete((response, error) -> {
                        RpcResponse resp = (error != null) ? RpcResponse.error(unwrap(error)) : response;
                        writeResponse(ctx, requestId, codec, resp);
                    }));
        } catch (RejectedExecutionException ex) {
            // 业务线程池过载/已停机：立即回写明确错误，让客户端快速失败而非干等到超时
            log.warn("业务线程池拒绝请求(requestId={})，回传繁忙/停机错误", requestId);
            writeResponse(ctx, requestId, codec, RpcResponse.error(new RpcException("服务端繁忙或正在停机，请求被拒绝")));
        }
    }

    /**
     * 从channel（pipeline尾部）写出响应，确保经过尾部的RpcEncoder编码。
     * 若响应编码失败（如返回值/业务异常不可序列化），回传一个可序列化的兜底错误，复用同一requestId，
     * 避免客户端只能干等到超时、完全拿不到原因。
     */
    private void writeResponse(ChannelHandlerContext ctx, long requestId, byte codec, RpcResponse response) {
        RpcMessage out = RpcMessage.response(requestId, codec, response);
        boolean hasEx = response != null && response.hasException();
        log.debug("[NettyServerHandler] 写出响应 requestId={}, hasException={}, to {}", requestId, hasEx, ctx.channel().remoteAddress());
        ctx.channel().writeAndFlush(out).addListener((ChannelFutureListener) f -> {
            if (!f.isSuccess()) {
                log.warn("响应发送失败(requestId={})，回传兜底错误: {}", requestId, String.valueOf(f.cause()));
                RpcResponse fallback = RpcResponse.error(new RpcException("服务端响应序列化失败: " + f.cause()));
                f.channel().writeAndFlush(RpcMessage.response(requestId, codec, fallback));
            }
        });
    }

    private static Throwable unwrap(Throwable t) {
        return (t instanceof CompletionException && t.getCause() != null) ? t.getCause() : t;
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) {
        // 读空闲：长时间没收到任何数据（连心跳都没有），判定对端失联，关闭连接
        if (evt instanceof IdleStateEvent && ((IdleStateEvent) evt).state() == IdleState.READER_IDLE) {
            log.info("读空闲超时，关闭疑似失联连接: {}", ctx.channel().remoteAddress());
            ctx.close();
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.warn("服务端连接异常，关闭: {}", ctx.channel().remoteAddress(), cause);
        ctx.close();
    }
}
