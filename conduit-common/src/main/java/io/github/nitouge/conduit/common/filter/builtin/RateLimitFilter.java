package io.github.nitouge.conduit.common.filter.builtin;

import io.github.nitouge.conduit.api.RpcRequest;
import io.github.nitouge.conduit.api.RpcResponse;
import io.github.nitouge.conduit.common.config.ConduitConfig;
import io.github.nitouge.conduit.common.exception.RpcException;
import io.github.nitouge.conduit.common.filter.Filter;
import io.github.nitouge.conduit.common.filter.FilterChain;
import io.github.nitouge.conduit.common.filter.Side;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

/**
 * QPS 限流Filter（提供侧，令牌桶）：限制单方法/全服务每秒可进入的请求数，超限快速失败。
 *
 * <p><b>与 {@link ActiveLimitFilter} 的分工</b>：并发隔离挡的是「同时在跑的请求数」（持续流量稳定、
 * 突发时也有效），令牌桶挡的是「单位时间进入的请求数」（防瞬时尖峰把下游/线程池冲垮）。两者互补，
 * 可同时开。</p>
 *
 * <p><b>默认关闭</b>——用 {@code -Dconduit.filter.ratelimit.enabled=true} 开启。</p>
 *
 * <table border="1">
 *   <caption>配置项</caption>
 *   <tr><th>键</th><th>默认</th><th>说明</th></tr>
 *   <tr><td>{@code conduit.filter.ratelimit.qps}</td><td>1000</td><td>每方法每秒令牌数（=每秒允许的请求数）</td></tr>
 *   <tr><td>{@code conduit.filter.ratelimit.<接口#方法>.qps}</td><td>—</td><td>单方法覆盖</td></tr>
 *   <tr><td>{@code conduit.filter.ratelimit.global-qps}</td><td>0</td><td>全服务共用桶；{@code 0} 表示不启用全局桶</td></tr>
 *   <tr><td>{@code conduit.filter.ratelimit.burst}</td><td>=qps</td><td>桶容量（允许的瞬时突发量）</td></tr>
 * </table>
 *
 * <p><b>快速失败</b>：拿不到令牌立即以 {@link RpcException} 失败，与并发隔离一致——限流的意义就是
 * 拒绝，不是排队。排队只会把超时和内存一起放大。</p>
 *
 * <p>取舍：桶按 JVM 本地计数，多实例部署时是「每实例 QPS」而不是集群总 QPS；需要集群总量时
 * 应接集中式限流（Redis/网关），不属于本Filter职责。配置在桶创建时读取一次，运行期变更需重启或换实现。</p>
 */
public class RateLimitFilter implements Filter {

    /** 每方法默认 QPS */
    private static final int DEFAULT_QPS = 1000;

    private final Map<String, TokenBucket> methodBuckets = new ConcurrentHashMap<>();

    /** 全局桶：懒解析（配置源可能在Filter实例创建之后才就绪，如Spring Environment） */
    private volatile TokenBucket globalBucket;
    private volatile boolean globalResolved;

    @Override
    public CompletableFuture<RpcResponse> invoke(RpcRequest request, FilterChain chain) {
        String key = request.getInterfaceName() + "#" + request.getMethodName();
        String rejected = null;

        // 先看方法桶再看全局桶：方法级拒绝时不消耗全局预算
        TokenBucket bucket = methodBuckets.computeIfAbsent(key, this::createMethodBucket);
        if (!bucket.tryAcquire()) {
            rejected = "方法限流";
        } else {
            TokenBucket global = globalBucket();
            if (global != null && !global.tryAcquire()) {
                rejected = "全局限流";
            }
        }
        if (rejected != null) {
            CompletableFuture<RpcResponse> failed = new CompletableFuture<>();
            failed.completeExceptionally(new RpcException(
                    rejected + "(RateLimit)，方法 " + key + " 超过QPS上限，快速失败"));
            return failed;
        }
        return chain.invoke(request);
    }

    @Override
    public int order() {
        // 比并发隔离(50)更外层：QPS 是更便宜也更早的拒绝，先挡掉不该进来的流量
        return 40;
    }

    @Override
    public boolean appliesTo(Side side) {
        // 保护提供者：消费侧的吞吐控制属于调用方自己的事（网关/业务队列更合适）
        return side == Side.PROVIDER;
    }

    @Override
    public boolean enabled() {
        return false; // 默认关闭：明确开启才限流
    }

    @Override
    public String name() {
        return "ratelimit";
    }

    private TokenBucket createMethodBucket(String key) {
        int qps = ConduitConfig.getPositiveInt(
                "conduit.filter.ratelimit." + key + ".qps",
                ConduitConfig.getPositiveInt("conduit.filter.ratelimit.qps", DEFAULT_QPS));
        return createBucket(qps);
    }

    /** 全局桶（首次调用时解析配置；未配置/{@code 0} 表示不启用） */
    private TokenBucket globalBucket() {
        if (!globalResolved) {
            synchronized (this) {
                if (!globalResolved) {
                    globalBucket = createBucket(
                            ConduitConfig.getPositiveInt("conduit.filter.ratelimit.global-qps", 0));
                    globalResolved = true;
                }
            }
        }
        return globalBucket;
    }

    private static TokenBucket createBucket(int qps) {
        return qps > 0 ? new TokenBucket(qps) : null;
    }

    /**
     * 令牌桶：容量与速率都等于 QPS（1 秒的量），允许一次突发打满，之后按速率匀速补充。
     *
     * <p>用 {@code synchronized} 而不是 CAS 重试：临界区只有几次浮点运算，锁竞争远小于自旋浪费；
     * 每秒最多 1000~几万次加锁在 JVM 里可以忽略。</p>
     */
    private static final class TokenBucket {

        private final double ratePerSecond;
        private final double capacity;
        private double tokens;
        private long lastRefillNanos;

        private TokenBucket(int qps) {
            this.ratePerSecond = qps;
            this.capacity = qps;
            this.tokens = qps;
            this.lastRefillNanos = System.nanoTime();
        }

        private synchronized boolean tryAcquire() {
            long now = System.nanoTime();
            double elapsedSeconds = (now - lastRefillNanos) / 1_000_000_000.0;
            if (elapsedSeconds > 0) {
                tokens = Math.min(capacity, tokens + elapsedSeconds * ratePerSecond);
                lastRefillNanos = now;
            }
            if (tokens >= 1.0) {
                tokens -= 1.0;
                return true;
            }
            return false;
        }
    }
}
