package io.github.nitouge.conduit.remoting.netty;

import io.github.nitouge.conduit.api.RpcAttachments;
import io.github.nitouge.conduit.api.RpcRequest;
import io.github.nitouge.conduit.api.RpcResponse;
import io.github.nitouge.conduit.common.exception.RpcException;
import io.github.nitouge.conduit.protocol.ProtocolConstants;
import io.github.nitouge.conduit.protocol.RpcMessage;
import io.github.nitouge.conduit.remoting.RequestHandler;
import io.github.nitouge.conduit.remoting.ServerCallRegistry;
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
 * 到这里的是业务请求或取消帧。</p>
 *
 * <p><b>取消与deadline：</b>每个请求派发前登记到 {@link ServerCallRegistry}；客户端发来
 * CANCEL 帧、或请求携带的 deadline 到点，都会中断正在执行业务方法的线程。对端主动取消时不再
 * 回写响应（它已经不收结果了）；本地 deadline 中止时回一帧错误说明原因。deadline 已过期的请求
 * 连业务线程都不占，直接回错误。</p>
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
        // 取消帧：不派发业务，直接在在途表里找到目标请求并中断它
        if (msg.isCancel()) {
            handleCancel(ctx, msg);
            return;
        }
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

        // deadline：上游已经不等了，最便宜的"不白跑"就是连业务线程都不占用
        final long deadline = RpcAttachments.deadlineOf(request);
        final long remaining = deadline > 0 ? deadline - System.currentTimeMillis() : 0L;
        if (deadline > 0 && remaining <= 0) {
            log.debug("[NettyServerHandler] requestId={} 的deadline已过({}ms)，不派发业务直接回错误",
                    requestId, -remaining);
            writeResponse(ctx, requestId, version, codec, compress, RpcResponse.error(
                    new RpcException("请求已超过deadline（超出 " + (-remaining) + "ms），服务端未执行")));
            return;
        }

        log.debug("[NettyServerHandler] 收到请求 requestId={}, {}#{}, version={}, codec={}, compress={}, deadline={}, from {}, 派发业务线程池",
                requestId, request.getInterfaceName(), request.getMethodName(), version, codec, compress,
                deadline > 0 ? remaining + "ms后" : "无", ctx.channel().remoteAddress());

        // 先登记再派发：CANCEL 帧与 deadline 都要能在这个（连接, requestId）上找到在途调用
        final ServerCallRegistry.Call call = ServerCallRegistry.register(ctx.channel(), requestId);
        if (deadline > 0) {
            ServerCallRegistry.scheduleDeadline(call, deadline);
        }

        // 派发到业务线程池调用业务（避免阻塞I/O线程）。handle返回future：
        // 同步方法立即完成；返回CompletableFuture的异步方法不占住业务线程，响应在future完成时回写。
        try {
            businessPool.execute(() -> runOnBusinessPool(ctx, request, call, requestId, version, codec, compress));
        } catch (RejectedExecutionException ex) {
            ServerCallRegistry.unregister(call);
            // 业务线程池过载/已停机：立即回写明确错误，让客户端快速失败而非干等到超时
            log.warn("业务线程池拒绝请求(requestId={})，回传繁忙/停机错误", requestId);
            writeResponse(ctx, requestId, version, codec, compress,
                    RpcResponse.error(new RpcException("服务端繁忙或正在停机，请求被拒绝")));
        }
    }

    /**
     * 业务线程池里执行一次请求：进入业务方法前再确认一次是否已被取消，执行期间允许被中断，
     * 返回后立刻「脱钩」并清中断标记，最后按是否被取消决定要不要回写响应。
     */
    private void runOnBusinessPool(ChannelHandlerContext ctx, RpcRequest request, ServerCallRegistry.Call call,
                                   long requestId, byte version, byte codec, byte compress) {
        if (call.isCancelled()) {
            // 排队期间就被取消：连业务方法都不必进
            ServerCallRegistry.unregister(call);
            log.debug("[NettyServerHandler] requestId={} 在排队期间已被取消({})，不执行业务",
                    requestId, ProtocolConstants.cancelReasonText(call.reason()));
            respondIfAborted(ctx, call, requestId, version, codec, compress);
            return;
        }

        ServerCallRegistry.attachWorker(call, Thread.currentThread());
        CompletableFuture<RpcResponse> result;
        try {
            result = handler.handle(request);
        } catch (Throwable t) {
            // 兜底：RequestHandler实现异常不能导致"没有响应"，否则客户端只能等到超时
            result = CompletableFuture.completedFuture(RpcResponse.error(t));
        } finally {
            // 业务方法已返回：不再接受中断；并清掉可能残留的中断标记——
            // 否则这个标记会带给线程池里的下一个任务，制造难以复现的"随机中断"。
            ServerCallRegistry.detachWorker(call);
            Thread.interrupted();
        }
        if (result == null) {
            result = CompletableFuture.completedFuture(
                    RpcResponse.error(new RpcException("RequestHandler返回null future")));
        }
        result.whenComplete((response, error) -> {
            ServerCallRegistry.unregister(call);
            if (call.isCancelled()) {
                respondIfAborted(ctx, call, requestId, version, codec, compress);
                return;
            }
            RpcResponse resp = (error != null) ? RpcResponse.error(unwrap(error))
                    : (response != null ? response : RpcResponse.error(new RpcException("业务返回null响应")));
            writeResponse(ctx, requestId, version, codec, compress, resp);
        });
    }

    /**
     * 被取消的请求要不要回一帧：取决于「谁取消的」。
     *
     * <ul>
     *   <li><b>对端发的 CANCEL</b>：它明确表示不再需要结果，回写只是浪费带宽；</li>
     *   <li><b>本地 deadline</b>：对端可能还在等，回一帧「我中止了、原因是deadline」比让它
     *       各自等到超时更有价值——错误原因能直接体现在调用方日志里。</li>
     * </ul>
     */
    private void respondIfAborted(ChannelHandlerContext ctx, ServerCallRegistry.Call call,
                                  long requestId, byte version, byte codec, byte compress) {
        byte reason = call.reason();
        if (reason == ProtocolConstants.CANCEL_DEADLINE_EXCEEDED) {
            writeResponse(ctx, requestId, version, codec, compress, RpcResponse.error(
                    new RpcException("服务端已到达deadline，中止执行（未完成）")));
            return;
        }
        // 客户端早就不等了：写回一帧没人收的响应只会浪费带宽，也会让日志误导
        log.debug("[NettyServerHandler] requestId={} 已被取消({})，跳过响应回写",
                requestId, ProtocolConstants.cancelReasonText(reason));
    }

    /**
     * 处理 CANCEL 控制帧：尽力取消在途调用，<b>不回任何响应</b>——取消是单向通知，
     * 如果它也要响应语义，就等于给"不等了"又加了一次等待。
     */
    private void handleCancel(ChannelHandlerContext ctx, RpcMessage msg) {
        boolean hit = ServerCallRegistry.cancel(ctx.channel(), msg.getRequestId(), msg.getStatus());
        log.debug("[NettyServerHandler] 收到CANCEL requestId={}, reason={}, 命中={}, from {}",
                msg.getRequestId(), ProtocolConstants.cancelReasonText(msg.getStatus()), hit,
                ctx.channel().remoteAddress());
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
