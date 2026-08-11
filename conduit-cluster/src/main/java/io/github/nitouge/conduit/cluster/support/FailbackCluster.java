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
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * 失败自动恢复：调用失败时先返回空结果，并在后台定时重试若干次（best-effort）。
 * 适合消息通知等最终送达即可的场景。
 *
 * <p>前台返回给调用方的future始终正常完成（要么带真实响应，要么带空 {@link RpcResponse}）——
 * 调用方（同步/异步都一样）永远看不到异常；真正的重试在后台 {@link #SCHEDULER} 上进行，
 * 与前台future完全脱钩，不影响调用方的超时预算。</p>
 */
public class FailbackCluster extends AbstractCluster {

    private static final Logger log = LoggerFactory.getLogger(FailbackCluster.class);

    private static final int MAX_RETRY = 3;

    private static final long RETRY_DELAY_MS = 2000;

    private static final ScheduledExecutorService SCHEDULER =
            Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "conduit-failback");
                t.setDaemon(true);
                return t;
            });

    @Override
    public CompletableFuture<RpcResponse> invoke(List<ServiceMeta> services, RpcRequest request,
                                                  LoadBalancer loadBalancer, RemoteInvoker remoteInvoker) {
        try {
            requireNonEmpty(services);
            ServiceMeta node = loadBalancer.select(services, request);
            log.debug("[FailbackCluster] 单次调用 {}#{} -> {}:{}",
                    request.getInterfaceName(), request.getMethodName(),
                    node.getHost(), node.getPort());
            return callWithBreaker(node, request, remoteInvoker)
                    .exceptionally(err -> {
                        log.warn("[FailbackCluster] 调用失败，加入后台重试，先返回空结果: {}", err.toString());
                        scheduleRetry(services, request, loadBalancer, remoteInvoker, 1);
                        return new RpcResponse();
                    });
        } catch (Exception e) {
            log.warn("[FailbackCluster] 调用失败，加入后台重试，先返回空结果: {}", e.toString());
            scheduleRetry(services, request, loadBalancer, remoteInvoker, 1);
            return CompletableFuture.completedFuture(new RpcResponse());
        }
    }

    private void scheduleRetry(List<ServiceMeta> services, RpcRequest request,
                              LoadBalancer loadBalancer, RemoteInvoker remoteInvoker, int attempt) {
        SCHEDULER.schedule(() -> {
            try {
                requireNonEmpty(services);
                ServiceMeta node = loadBalancer.select(services, request);
                callWithBreaker(node, request, remoteInvoker).whenComplete((resp, err) -> {
                    if (err != null) {
                        retryOrGiveUp(services, request, loadBalancer, remoteInvoker, attempt, err);
                    } else {
                        log.info("[FailbackCluster] 后台重试成功（第{}次）", attempt);
                    }
                });
            } catch (Exception e) {
                retryOrGiveUp(services, request, loadBalancer, remoteInvoker, attempt, e);
            }
        }, RETRY_DELAY_MS, TimeUnit.MILLISECONDS);
    }

    private void retryOrGiveUp(List<ServiceMeta> services, RpcRequest request,
                              LoadBalancer loadBalancer, RemoteInvoker remoteInvoker,
                              int attempt, Throwable err) {
        if (attempt < MAX_RETRY) {
            scheduleRetry(services, request, loadBalancer, remoteInvoker, attempt + 1);
        } else {
            log.warn("[FailbackCluster] 后台重试{}次仍失败，放弃: {}", MAX_RETRY, err.toString());
        }
    }
}
