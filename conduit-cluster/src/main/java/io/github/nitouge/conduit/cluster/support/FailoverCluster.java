package io.github.nitouge.conduit.cluster.support;

import io.github.nitouge.conduit.api.RpcRequest;
import io.github.nitouge.conduit.api.RpcResponse;
import io.github.nitouge.conduit.api.ServiceMeta;
import io.github.nitouge.conduit.cluster.LoadBalancer;
import io.github.nitouge.conduit.cluster.RemoteInvoker;
import io.github.nitouge.conduit.cluster.breaker.CircuitOpenException;
import io.github.nitouge.conduit.common.exception.RpcException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

/**
 * 失败自动切换（默认策略）：调用失败时自动重试其它节点，直到成功或用尽重试。
 *
 * <p>最多尝试 {@code retries + 1} 次，每次从「未试过」的候选里重选，避免重复打同一坏节点；
 * 配合熔断器，坏节点会被快速跳过。最坏总耗时≈(retries+1)×单次超时——这里的重试是
 * <b>异步递归</b>（每次的future完成后触发下一次尝试），不占用任何线程等待，也不阻塞调用方；
 * 同步调用方在链外统一 {@code .get(timeout)} 拿总耗时上限，异步调用方拿到的是这条重试链的future。</p>
 *
 * <p>仅适合幂等方法；写操作应改用failfast。</p>
 *
 * <p><b>注意：</b>当前Cluster经SPI以无参构造缓存为单例，故 {@code retries} 实际固定为默认值2；
 * {@code @RpcReference} 暂只能选策略名（cluster），还不能配置retries次数——把次数做成可配置属于后续增强
 * （需要让带参策略不走SPI单例，或在取出后注入配置）。</p>
 */
public class FailoverCluster extends AbstractCluster {

    private static final Logger log = LoggerFactory.getLogger(FailoverCluster.class);

    private final int retries;

    public FailoverCluster() {
        this(2);
    }

    public FailoverCluster(int retries) {
        this.retries = retries;
    }

    @Override
    public CompletableFuture<RpcResponse> invoke(List<ServiceMeta> services, RpcRequest request,
                                                  LoadBalancer loadBalancer, RemoteInvoker remoteInvoker) {
        requireNonEmpty(services);
        int maxAttempts = retries + 1;
        log.debug("[FailoverCluster] 开始 {}#{}, 候选{}个, maxAttempts={}",
                request.getInterfaceName(), request.getMethodName(), services.size(), maxAttempts);
        return attempt(new ArrayList<>(services), request, loadBalancer, remoteInvoker,
                0, maxAttempts, new ArrayList<>(), null);
    }

    /**
     * 尝试一个节点，future完成后决定：成功直接返回；熔断跳过（不计入预算）或传输失败（计入预算）
     * 则递归尝试下一个候选，直至用尽候选或用尽预算。
     *
     * <p>终止性：每轮都从candidates移除一个节点，candidates耗尽即结束（即便全是熔断跳过也不会
     * 无限递归）；attempts达maxAttempts也会结束。</p>
     *
     * @param attempts 只统计真正发起了远程调用的次数；熔断快速跳过不消耗这个预算
     * @param lastError 上一次失败的原因，仅用于用尽候选/预算时的最终异常cause
     */
    private CompletableFuture<RpcResponse> attempt(List<ServiceMeta> candidates, RpcRequest request,
                                                    LoadBalancer loadBalancer, RemoteInvoker remoteInvoker,
                                                    int attempts, int maxAttempts, List<String> tried,
                                                    Throwable lastError) {
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
                // 熔断器打开：零成本跳过坏节点，不消耗有限重试预算，继续尝试下一个候选
                log.debug("[FailoverCluster] 跳过已熔断节点{}，尝试其它节点", nodeAddr);
                return attempt(remaining, request, loadBalancer, remoteInvoker, attempts, maxAttempts, tried, cause);
            }
            log.warn("[FailoverCluster] 第{}次调用{}失败，转其它节点: {}", attempts + 1, nodeAddr, cause.toString());
            return attempt(remaining, request, loadBalancer, remoteInvoker, attempts + 1, maxAttempts, tried, cause);
        }).thenCompose(Function.identity());
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
