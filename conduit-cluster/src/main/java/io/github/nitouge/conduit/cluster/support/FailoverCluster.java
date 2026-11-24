package io.github.nitouge.conduit.cluster.support;

import io.github.nitouge.conduit.api.RpcRequest;
import io.github.nitouge.conduit.api.RpcResponse;
import io.github.nitouge.conduit.api.ServiceMeta;
import io.github.nitouge.conduit.cluster.ClusterOptions;
import io.github.nitouge.conduit.cluster.LoadBalancer;
import io.github.nitouge.conduit.cluster.RemoteInvoker;
import io.github.nitouge.conduit.cluster.breaker.CircuitOpenException;
import io.github.nitouge.conduit.common.exception.RpcException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * 失败自动切换（默认策略）：调用失败时自动重试其它节点，直到成功或用尽重试。
 *
 * <p>最多尝试 {@code retries + 1} 次（{@code retries} 由 {@link ClusterOptions} 按调用传入，
 * 来自 {@code @RpcReference(retries=...)} 或全局 {@code conduit.cluster.retries}），每次从
 * 未试过的候选里重选，避免重复打同一坏节点；配合熔断器，坏节点会被快速跳过。
 * 可选重试退避（{@code retryBackoffMs}）在两次真实失败之间异步等待，不占用线程。</p>
 *
 * <p>最坏总耗时≈(retries+1)×单次超时+退避——这里的重试是<b>异步递归</b>（每次的future完成后触发
 * 下一次尝试），不占用任何线程等待，也不阻塞调用方；同步调用方在链外统一 {@code get(timeout)}
 * 拿总耗时上限，异步调用方拿到的是这条重试链的future。</p>
 *
 * <p>仅适合幂等方法；写操作应改用failfast。</p>
 */
public class FailoverCluster extends AbstractCluster {

    private static final Logger log = LoggerFactory.getLogger(FailoverCluster.class);

    /** 构造器默认重试次数（四参调用时使用） */
    private final int defaultRetries;

    public FailoverCluster() {
        this(ClusterOptions.DEFAULT_RETRIES);
    }

    public FailoverCluster(int retries) {
        this.defaultRetries = Math.max(0, retries);
    }

    @Override
    public CompletableFuture<RpcResponse> invoke(List<ServiceMeta> services, RpcRequest request,
                                                 LoadBalancer loadBalancer, RemoteInvoker remoteInvoker) {
        return invoke(services, request, loadBalancer, remoteInvoker,
                ClusterOptions.builder().retries(defaultRetries).build());
    }

    @Override
    public CompletableFuture<RpcResponse> invoke(List<ServiceMeta> services, RpcRequest request,
                                                 LoadBalancer loadBalancer, RemoteInvoker remoteInvoker,
                                                 ClusterOptions options) {
        requireNonEmpty(services);
        ClusterOptions opts = (options != null) ? options : ClusterOptions.defaults();
        int maxAttempts = opts.getRetries() + 1;
        log.debug("[FailoverCluster] 开始 {}#{}, 候选{}个, maxAttempts={}, backoff={}ms",
                request.getInterfaceName(), request.getMethodName(), services.size(),
                maxAttempts, opts.getRetryBackoffMs());
        return attempt(new ArrayList<>(services), request, loadBalancer, remoteInvoker,
                0, maxAttempts, opts, new ArrayList<>(), null);
    }

    /**
     * 尝试一个节点，future完成后决定：成功直接返回；熔断跳过（不计入预算、不退避）或传输失败
     * （计入预算，按配置退避）则递归尝试下一个候选，直至用尽候选或用尽预算。
     *
     * <p>终止性：每轮都从candidates移除一个节点，candidates耗尽即结束（即便全是熔断跳过也不会
     * 无限递归）；attempts达maxAttempts也会结束。</p>
     *
     * @param attempts 只统计真正发起了远程调用的次数；熔断快速跳过不消耗这个预算
     * @param lastError 上一次失败的原因，仅用于用尽候选/预算时的最终异常cause
     */
    private CompletableFuture<RpcResponse> attempt(List<ServiceMeta> candidates, RpcRequest request,
                                                   LoadBalancer loadBalancer, RemoteInvoker remoteInvoker,
                                                   int attempts, int maxAttempts, ClusterOptions options,
                                                   List<String> tried, Throwable lastError) {
        if (attempts >= maxAttempts || candidates.isEmpty()) {
            return failed(tried, lastError);
        }
        ServiceMeta node = loadBalancer.select(candidates, request);
        if (node == null) {
            return failed(tried, lastError);
        }
        List<ServiceMeta> remaining = new ArrayList<>(candidates);
        remaining.remove(node); // 重选时避开已试节点
        String nodeAddr = node.getHost() + ":" + node.getPort();
        tried.add(nodeAddr);

        return callWithBreaker(node, request, remoteInvoker).handle((resp, err) -> {
            if (err == null) {
                log.debug("[FailoverCluster] 调用成功 {}#{} -> {} (第{}次尝试)",
                        request.getInterfaceName(), request.getMethodName(), nodeAddr, attempts + 1);
                return CompletableFuture.completedFuture(resp);
            }
            Throwable cause = unwrap(err);
            if (cause instanceof CircuitOpenException) {
                // 熔断器打开：零成本跳过坏节点，不消耗有限重试预算、也不退避，继续尝试下一个候选
                log.debug("[FailoverCluster] 跳过已熔断节点{}，尝试其它节点", nodeAddr);
                return retryLater(remaining, request, loadBalancer, remoteInvoker,
                        attempts, maxAttempts, options, tried, cause, 0L);
            }
            log.warn("[FailoverCluster] 第{}次调用{}失败，转其它节点: {}", attempts + 1, nodeAddr, cause.toString());
            return retryLater(remaining, request, loadBalancer, remoteInvoker,
                    attempts + 1, maxAttempts, options, tried, cause, options.getRetryBackoffMs());
        }).thenCompose(Function.identity());
    }

    /** 按配置的退避时长异步发起下一次尝试（0表示立即） */
    private CompletableFuture<RpcResponse> retryLater(List<ServiceMeta> candidates, RpcRequest request,
                                                      LoadBalancer loadBalancer, RemoteInvoker remoteInvoker,
                                                      int attempts, int maxAttempts, ClusterOptions options,
                                                      List<String> tried, Throwable lastError, long backoffMs) {
        if (backoffMs <= 0) {
            return attempt(candidates, request, loadBalancer, remoteInvoker,
                    attempts, maxAttempts, options, tried, lastError);
        }
        CompletableFuture<RpcResponse> delayed = new CompletableFuture<>();
        try {
            CompletableFuture.delayedExecutor(backoffMs, TimeUnit.MILLISECONDS).execute(() -> {
                try {
                    attempt(candidates, request, loadBalancer, remoteInvoker,
                            attempts, maxAttempts, options, tried, lastError)
                            .whenComplete((resp, err) -> {
                                if (err != null) {
                                    delayed.completeExceptionally(err);
                                } else {
                                    delayed.complete(resp);
                                }
                            });
                } catch (Throwable t) {
                    delayed.completeExceptionally(t);
                }
            });
        } catch (Throwable t) {
            delayed.completeExceptionally(t);
        }
        return delayed;
    }

    private static CompletableFuture<RpcResponse> failed(List<String> tried, Throwable last) {
        CompletableFuture<RpcResponse> failed = new CompletableFuture<>();
        failed.completeExceptionally(new RpcException("[FailoverCluster] 调用失败，已尝试节点 " + tried, last));
        return failed;
    }

    /** future异常链可能把原始异常包一层CompletionException，取出真实原因用于类型判断/日志 */
    private static Throwable unwrap(Throwable t) {
        return (t instanceof java.util.concurrent.CompletionException && t.getCause() != null) ? t.getCause() : t;
    }
}
