package io.github.nitouge.conduit.cluster.support;

import io.github.nitouge.conduit.api.RpcRequest;
import io.github.nitouge.conduit.api.RpcResponse;
import io.github.nitouge.conduit.api.ServiceMeta;
import io.github.nitouge.conduit.cluster.LoadBalancer;
import io.github.nitouge.conduit.cluster.RemoteInvoker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * 失败安全：调用失败时忽略异常、返回空结果（data=null）。适合写日志、统计等失败也无所谓的操作。
 *
 * <p>两处可能失败都要接住：节点选择阶段的同步异常（如无可用服务）、以及远程调用future的异常完成
 * （通过 {@code exceptionally} 兜底），最终都落到一个正常完成的空 {@link RpcResponse}。</p>
 */
public class FailsafeCluster extends AbstractCluster {

    private static final Logger log = LoggerFactory.getLogger(FailsafeCluster.class);

    @Override
    public CompletableFuture<RpcResponse> invoke(List<ServiceMeta> services, RpcRequest request,
                                                  LoadBalancer loadBalancer, RemoteInvoker remoteInvoker) {
        try {
            requireNonEmpty(services);
            ServiceMeta node = loadBalancer.select(services, request);
            log.debug("[FailsafeCluster] 单次调用 {}#{} -> {}:{}",
                    request.getInterfaceName(), request.getMethodName(),
                    node.getHost(), node.getPort());
            return callWithBreaker(node, request, remoteInvoker)
                    .exceptionally(err -> {
                        log.warn("[FailsafeCluster] 忽略调用失败，返回空结果: {}", err.toString());
                        return new RpcResponse(); // data=null, exception=null
                    });
        } catch (Exception e) {
            log.warn("[FailsafeCluster] 忽略调用失败，返回空结果: {}", e.toString());
            return CompletableFuture.completedFuture(new RpcResponse());
        }
    }
}
