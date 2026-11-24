package io.github.nitouge.conduit.cluster;

import io.github.nitouge.conduit.api.RpcRequest;
import io.github.nitouge.conduit.api.RpcResponse;
import io.github.nitouge.conduit.api.ServiceMeta;
import io.github.nitouge.conduit.common.spi.Adaptive;
import io.github.nitouge.conduit.common.spi.SPI;

import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * 集群容错：把一批候选实例收敛成一次成功（或按策略处理失败）的调用。@SPI，默认failover。
 *
 * <p>策略实现内部用 {@link LoadBalancer} 选节点、用 {@link RemoteInvoker} 真正发起调用，
 * 并按各自语义处理失败（重试其它节点/直接失败/忽略/后台重试）。</p>
 *
 * <p><b>可配置重试：</b>{@link #invoke(List, RpcRequest, LoadBalancer, RemoteInvoker, ClusterOptions)}
 * 把重试次数/退避/熔断参数按<b>每次调用</b>传入（来自 {@code @RpcReference} 或全局配置）；未传时
 * 退回四参版本，保持既有SPI扩展的兼容性。</p>
 *
 * <p><b>自适应：</b>五参方法标注 {@link Adaptive}，配合
 * {@code ExtensionLoader.getAdaptiveExtension()} 可从 {@code RpcContext} 的
 * {@code cluster} attachment按次覆盖容错策略，无需为每个方法单独声明。</p>
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

    /** 经典四参调用（使用策略自身的默认参数，兼容既有扩展实现） */
    CompletableFuture<RpcResponse> invoke(List<ServiceMeta> services, RpcRequest request,
                                          LoadBalancer loadBalancer, RemoteInvoker remoteInvoker);

    /**
     * 带配置的五参调用。
     *
     * <p>默认实现忽略 {@code options} 并委托给四参版本；需要读取重试配置的策略（如FailOver）覆盖它即可。</p>
     */
    @Adaptive({"cluster"})
    default CompletableFuture<RpcResponse> invoke(List<ServiceMeta> services, RpcRequest request,
                                                  LoadBalancer loadBalancer, RemoteInvoker remoteInvoker,
                                                  ClusterOptions options) {
        return invoke(services, request, loadBalancer, remoteInvoker);
    }
}
