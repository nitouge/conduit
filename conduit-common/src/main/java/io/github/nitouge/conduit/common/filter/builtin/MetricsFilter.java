package io.github.nitouge.conduit.common.filter.builtin;

import io.github.nitouge.conduit.api.RpcRequest;
import io.github.nitouge.conduit.api.RpcResponse;
import io.github.nitouge.conduit.common.filter.Filter;
import io.github.nitouge.conduit.common.filter.FilterChain;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;

/**
 * 指标统计Filter：双侧记录QPS/RT/错误率（轻量内存版）。
 *
 * <p><b>默认关闭</b>——用 {@code -Dconduit.filter.metrics.enabled=true} 开启。
 * 内部按 (interface + method) 维度分桶，用 {@link LongAdder} 做无锁并发计数。</p>
 *
 * <p><b>如何读取指标：</b>
 * <pre>{@code
 *   Map<String, Stats> snapshot = MetricsFilter.snapshot();
 *   for (Map.Entry<String, Stats> e : snapshot.entrySet()) {
 *       Stats s = e.getValue();
 *       System.out.printf("%s qps=%d rt=%.2fms errRate=%.2f%%%n",
 *           e.getKey(), s.total.sum(), s.avgRtMs(), s.errorRate() * 100);
 *   }
 * }</pre>
 *
 * <p><b>取舍：</b>不做滑动窗口（§1.6 熔断增强会做）；只做全量累计，可重置。
 * 无外部依赖（无Micrometer/Prometheus耦合），prometheus导出器作后续。</p>
 */
public class MetricsFilter implements Filter {

    /** 指标快照：key=side:interface#method；值=该桶累计 */
    private static final Map<String, Stats> STATS = new ConcurrentHashMap<>();

    public static void reset() {
        STATS.clear();
    }

    /** 取所有桶的当前快照（只读引用） */
    public static Map<String, Stats> snapshot() {
        return STATS;
    }

    @Override
    public CompletableFuture<RpcResponse> invoke(RpcRequest request, FilterChain chain) {
        long startNanos = System.nanoTime();
        String key = keyOf(request);

        return chain.invoke(request).whenComplete((resp, err) -> {
            long elapsedNanos = System.nanoTime() - startNanos;
            boolean success = (err == null && (resp == null || !resp.hasException()));
            Stats stats = STATS.computeIfAbsent(key, k -> new Stats());
            stats.total.increment();
            if (success) {
                stats.success.increment();
            } else {
                stats.error.increment();
            }
            stats.rtNanos.add(elapsedNanos);
        });
    }

    @Override
    public int order() {
        return 0; // 中间层
    }

    @Override
    public boolean enabled() {
        return false; // 默认关闭：明确开启才统计（无统计需求时零开销）
    }

    @Override
    public String name() {
        return "metrics";
    }

    private static String keyOf(RpcRequest request) {
        // Filter单例被消费侧与提供侧共享，本身不知side——统一按interface#method分桶。
        // 单JVM端到端下一次调用会被消费/提供各计一次；跨进程部署时各进程只有本侧调用，天然干净。
        // 需要严格区分side，可由部署（consumer/provider分进程）或包装side-aware key实现。
        return request.getInterfaceName() + "#" + request.getMethodName();
    }

    /** 一个桶的累计统计（线程安全） */
    public static final class Stats {
        final LongAdder total = new LongAdder();
        final LongAdder success = new LongAdder();
        final LongAdder error = new LongAdder();
        final LongAdder rtNanos = new LongAdder();

        public long total() {
            return total.sum();
        }

        public long success() {
            return success.sum();
        }

        public long error() {
            return error.sum();
        }

        /** 平均RT（毫秒），total=0时返回0 */
        public double avgRtMs() {
            long t = total.sum();
            return t == 0 ? 0.0 : (rtNanos.sum() / 1_000_000.0) / t;
        }

        /** 错误率（0~1），total=0时返回0 */
        public double errorRate() {
            long t = total.sum();
            return t == 0 ? 0.0 : (double) error.sum() / t;
        }
    }
}
