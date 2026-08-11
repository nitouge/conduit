package io.github.nitouge.conduit.cluster;

import io.github.nitouge.conduit.api.RpcRequest;
import io.github.nitouge.conduit.api.RpcResponse;
import io.github.nitouge.conduit.api.ServiceMeta;

import java.util.concurrent.CompletableFuture;

/**
 * 远程调用回调：把对某个具体实例发起一次远程调用交给上层（client）注入，
 * 这样cluster只负责选节点 + 容错策略，不直接依赖remoting（保持下层纯净）。
 *
 * <p>约定：调用本身永不同步抛异常——统一返回future。成功拿到响应（即便响应里带的是业务异常）
 * future正常完成；<b>调用本身失败</b>（连接失败/超时等传输层错误）future异常完成——
 * 容错策略据此决定重试/失败。</p>
 */
@FunctionalInterface
public interface RemoteInvoker {

    CompletableFuture<RpcResponse> invoke(ServiceMeta target, RpcRequest request);
}
