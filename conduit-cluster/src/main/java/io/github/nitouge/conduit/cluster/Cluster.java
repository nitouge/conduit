package io.github.nitouge.conduit.cluster;

import io.github.nitouge.conduit.api.RpcRequest;
import io.github.nitouge.conduit.api.RpcResponse;
import io.github.nitouge.conduit.api.ServiceMeta;
import io.github.nitouge.conduit.common.spi.SPI;

import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * 集群容错：把一批候选实例收敛成一次成功（或按策略处理失败）的调用。@SPI，默认failover。
 *
 * <p>策略实现内部用 {@link LoadBalancer} 选节点、用 {@link RemoteInvoker} 真正发起调用，
 * 并按各自语义处理失败（重试其它节点/直接失败/忽略/后台重试）。</p>
 *
 * <p>注意：这里的失败指 {@link RemoteInvoker} 对应future异常完成所代表的传输层异常；业务异常会
 * 包在成功完成的 {@link RpcResponse} 里照常返回，不触发容错重试。</p>
 *
 * <p><b>为何返回 {@link CompletableFuture}：</b>让同步与异步调用共享同一条容错链——
 * 同步侧在链外统一 {@code .get(timeout)}，异步侧直接把future返回给调用方；重试/熔断等策略
 * 借助future组合（{@code handle}/{@code whenComplete}）实现，不阻塞任何线程。</p>
 */
@SPI("failover")
public interface Cluster {

    CompletableFuture<RpcResponse> invoke(List<ServiceMeta> services, RpcRequest request, LoadBalancer loadBalancer, RemoteInvoker remoteInvoker);
}
