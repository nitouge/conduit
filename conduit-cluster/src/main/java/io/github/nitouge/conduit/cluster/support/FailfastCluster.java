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
 * 快速失败：只调用一次，失败立即让future异常完成，不重试。适合非幂等的写操作。
 */
public class FailfastCluster extends AbstractCluster {

    private static final Logger log = LoggerFactory.getLogger(FailfastCluster.class);

    @Override
    public CompletableFuture<RpcResponse> invoke(List<ServiceMeta> services, RpcRequest request,
                                                  LoadBalancer loadBalancer, RemoteInvoker remoteInvoker) {
        requireNonEmpty(services);
        ServiceMeta node = loadBalancer.select(services, request);
        log.debug("[FailfastCluster] 单次调用 {}#{} -> {}:{}",
                request.getInterfaceName(), request.getMethodName(),
                node.getHost(), node.getPort());
        return callWithBreaker(node, request, remoteInvoker);
    }
}
