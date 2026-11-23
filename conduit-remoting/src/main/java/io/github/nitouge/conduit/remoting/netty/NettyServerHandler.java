package io.github.nitouge.conduit.remoting.netty;

import io.github.nitouge.conduit.api.RpcRequest;
import io.github.nitouge.conduit.api.RpcResponse;
import io.github.nitouge.conduit.common.exception.RpcException;
import io.github.nitouge.conduit.protocol.ProtocolConstants;
import io.github.nitouge.conduit.protocol.RpcMessage;
import io.github.nitouge.conduit.remoting.RequestHandler;
import io.github.nitouge.conduit.remoting.handshake.Handshake;
import io.github.nitouge.conduit.remoting.handshake.Negotiated;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;

/**
 * 服务端业务Handler：收到请求帧后，提交业务线程池调用 {@link RequestHandler}，
 * 再把结果包成响应帧（复用同一个requestId、协议版本与协商出的codec/压缩算法）写回。
 *
 * <p>杜绝反射调用跑在Netty的I/O线程上，故统一派发到业务线程池。心跳/握手由上游处理器处理，
 * 到这里的一定是业务请求。</p>
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
        if (!(msg.getPayload() instanceof RpcRequest request)) {
            log.warn("[NettyServerHandler] 收到非请求帧(type={})，关闭连接: {}",
                    ProtocolConstants.typeName(msg.getType()), ctx.channel().remoteAddress());
            ctx.close();
            return;
        }

        final long requestId = msg.getRequestId();
        final byte codec = msg.getCodec();
        final byte version = msg.getVersion();
        final Negotiated negotiated = Handshake.negotiated(ctx.channel());
        final byte compress = negotiated != null ? negotiated.getCompress() : ProtocolConstants.COMPRESS_NONE;

        log.debug("[NettyServerHandler] 收到请求 requestId={}, {}#{}, version={}, codec={}, compress={}, from {}, 派发业务线程池",
                requestId, request.getInterfaceName(), request.getMethodName(), version, codec, compress,
                ctx.channel().remoteAddress());

        // 派发到业务线程池调用业务（避免阻塞I/O线程）。handle返回future：
        // 同步方法立即完成；返回CompletableFuture的异步方法不占住业务线程，响应在future完成时回写。
        try {
            businessPool.execute(() -> {
                CompletableFuture<RpcResponse> result;
                try {
                    result = handler.handle(request);
                } catch (Throwable t) {
                    // 兜底：RequestHandler实现异常不能导致"没有响应"，否则客户端只能等到超时
                    result = CompletableFuture.completedFuture(RpcResponse.error(t));
                }
                if (result == null) {
                    result = CompletableFuture.completedFuture(
                            RpcResponse.error(new RpcException("RequestHandler返回null future")));
                }
                result.whenComplete((response, error) -> {
                    RpcResponse resp = (error != null) ? RpcResponse.error(unwrap(error))
                            : (response != null ? response : RpcResponse.error(new RpcException("业务返回null响应")));
                    writeResponse(ctx, requestId, version, codec, compress, resp);
                });
            });
        } catch (RejectedExecutionException ex) {
            // 业务线程池过载/已停机：立即回写明确错误，让客户端快速失败而非干等到超时
            log.warn("业务线程池拒绝请求(requestId={})，回传繁忙/停机错误", requestId);
            writeResponse(ctx, requestId, version, codec, compress,
                    RpcResponse.error(new RpcException("服务端繁忙或正在停机，请求被拒绝")));
        }
    }

    /**
     * 从channel（pipeline尾部）写出响应，确保经过尾部的RpcEncoder编码。
     * 若响应编码失败（如返回值/业务异常不可序列化），回传一个可序列化的兜底错误，复用同一requestId，
     * 避免客户端只能干等到超时、完全拿不到原因。兜底响应再失败则关闭连接（客户端在途请求统一失败）。
     */
    private void writeResponse(ChannelHandlerContext ctx, long requestId, byte version, byte codec,
                               byte compress, RpcResponse response) {
        RpcMessage out = RpcMessage.response(requestId, codec, response);
        out.setVersion(version);
        out.setCompress(compress);
        boolean hasEx = response.hasException();
        log.debug("[NettyServerHandler] 写出响应 requestId={}, hasException={}, to {}",
                requestId, hasEx, ctx.channel().remoteAddress());
        ctx.channel().writeAndFlush(out).addListener((ChannelFutureListener) f -> {
            if (f.isSuccess()) {
                return;
            }
            log.warn("响应发送失败(requestId={})，回传兜底错误: {}", requestId, String.valueOf(f.cause()));
            RpcResponse fallback = RpcResponse.error(new RpcException("服务端响应序列化失败: " + f.cause()));
            RpcMessage fallbackMessage = RpcMessage.response(requestId, codec, fallback);
            fallbackMessage.setVersion(version);
            fallbackMessage.setCompress(ProtocolConstants.COMPRESS_NONE);
            f.channel().writeAndFlush(fallbackMessage).addListener((ChannelFutureListener) f2 -> {
                if (!f2.isSuccess()) {
                    log.warn("兜底错误响应仍发送失败(requestId={})，关闭连接: {}",
                            requestId, String.valueOf(f2.cause()));
                    f2.channel().close();
                }
            });
        });
    }

    private static Throwable unwrap(Throwable t) {
        return (t instanceof CompletionException && t.getCause() != null) ? t.getCause() : t;
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.warn("服务端连接异常，关闭: {}", ctx.channel().remoteAddress(), cause);
        ctx.close();
    }
}
