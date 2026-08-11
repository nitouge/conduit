package io.github.nitouge.conduit.common.filter;

import io.github.nitouge.conduit.api.RpcRequest;
import io.github.nitouge.conduit.api.RpcResponse;

import java.util.concurrent.CompletableFuture;

/**
 * 责任链的一环：既表示调用下一个filter，也表示链尾「真正干活」的调用终端。
 *
 * <p>消费侧链尾 = 走集群容错/负载均衡发起远程调用；提供侧链尾 = 反射调用业务方法。
 * filter通过持有下一环 {@link FilterChain} 实现前置逻辑 → chain.invoke → 后置逻辑。</p>
 *
 * <p><b>为何返回 {@link CompletableFuture}：</b>统一覆盖同步与异步两条路径——同步调用返回一个
 * 已完成的future（即时可取），异步调用返回真实future；一套filter写一次即两路通用，
 * 后置逻辑用 {@code future.whenComplete(...)} 挂载。这刻意偏离BACKLOG §0.A的同步草图签名
 * （{@code RpcResponse invoke(Invocation, FilterChain)}），因为同步签名无法诚实覆盖异步路径。</p>
 */
@FunctionalInterface
public interface FilterChain {

    /**
     * 触发链的下一环（或链尾终端）。
     *
     * <p>约定：<b>不抛受检异常、也不应同步抛出</b>——所有失败（含filter前置逻辑抛出的
     * 限流拒绝等）都应体现为future的异常完成，由 {@link FilterChainBuilder} 统一保证。</p>
     */
    CompletableFuture<RpcResponse> invoke(RpcRequest request);
}
