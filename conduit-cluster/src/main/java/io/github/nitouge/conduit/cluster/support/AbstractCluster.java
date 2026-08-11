package io.github.nitouge.conduit.cluster.support;

import io.github.nitouge.conduit.api.RpcRequest;
import io.github.nitouge.conduit.api.RpcResponse;
import io.github.nitouge.conduit.api.ServiceMeta;
import io.github.nitouge.conduit.cluster.Cluster;
import io.github.nitouge.conduit.cluster.RemoteInvoker;
import io.github.nitouge.conduit.cluster.breaker.CircuitBreaker;
import io.github.nitouge.conduit.cluster.breaker.CircuitBreakerRegistry;
import io.github.nitouge.conduit.cluster.breaker.CircuitOpenException;
import io.github.nitouge.conduit.common.exception.RpcException;
import org.apache.commons.collections4.CollectionUtils;

import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * 集群容错基类：统一带熔断的单节点调用。
 *
 * <p>调用前查节点熔断器，打开则快速失败（让FailOver转其它节点）；future正常完成 = onSuccess、
 * 异常完成 = onFailure。拿到响应即视为调用成功（即便响应里是业务异常——远程是通的），
 * 只有传输层异常才算失败。</p>
 */
public abstract class AbstractCluster implements Cluster {

    /**
     * 带熔断闸门地调用单个节点，契约如下：
     * <ul>
     *   <li>调用前先过该节点的熔断器闸门：闸门关闭（OPEN）则<b>不发起远程调用</b>，直接返回
     *       异常完成的future（{@link CircuitOpenException}，零成本快速跳过，供FailOver区别于真实失败）。</li>
     *   <li>放行后发起远程调用：future正常完成即视为<b>调用成功</b>（onSuccess）——即便响应体里是
     *       业务异常，也说明远程是通的；future异常完成（传输层错误）才算<b>失败</b>（onFailure）。</li>
     * </ul>
     *
     * <p>熔断回报挂在 {@code whenComplete} 上，不阻塞任何线程；对调用方而言语义与之前的
     * try/catch完全一致，只是从同步返回/抛出变成了future的正常/异常完成。</p>
     */
    protected CompletableFuture<RpcResponse> callWithBreaker(ServiceMeta node, RpcRequest request,
                                                              RemoteInvoker remoteInvoker) {
        CircuitBreaker breaker = CircuitBreakerRegistry.of(node);
        if (!breaker.allowRequest()) {
            CompletableFuture<RpcResponse> rejected = new CompletableFuture<>();
            rejected.completeExceptionally(
                    new CircuitOpenException("熔断器打开，跳过节点" + node.getHost() + ":" + node.getPort()));
            return rejected;
        }
        return remoteInvoker.invoke(node, request)
                .whenComplete((resp, err) -> {
                    if (err != null) {
                        breaker.onFailure();
                    } else {
                        breaker.onSuccess();
                    }
                });
    }

    protected static void requireNonEmpty(List<ServiceMeta> services) {
        if (CollectionUtils.isEmpty(services)) {
            throw new RpcException("无可用服务提供者");
        }
    }
}
