package io.github.nitouge.conduit.remoting;

import io.github.nitouge.conduit.api.RpcRequest;
import io.github.nitouge.conduit.api.RpcResponse;
import io.github.nitouge.conduit.remoting.netty.NettyServer;

import java.util.concurrent.CompletableFuture;

/**
 * 业务请求处理回调：由 {@code conduit-server} 实现（反射调用本地实现类），
 * 注入给 {@link NettyServer}，使传输层与业务逻辑解耦。
 *
 * <p>传输层解出 {@link RpcRequest} 后在业务线程池中回调本接口；它只做请求 -> 响应
 * 的纯业务转换，不接触协议头（requestId的读取与回填全在传输层完成）。</p>
 *
 * <p>返回 {@link CompletableFuture}：同步方法返回已完成的future；服务方法本身返回
 * {@link CompletableFuture} 时，可直接把它映射过来，从而<b>不阻塞业务线程</b>，
 * 响应在future完成时由传输层回写。</p>
 */
@FunctionalInterface
public interface RequestHandler {

    CompletableFuture<RpcResponse> handle(RpcRequest request);
}
