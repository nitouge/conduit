package io.github.nitouge.conduit.common.filter.builtin;

import io.github.nitouge.conduit.api.RpcRequest;
import io.github.nitouge.conduit.api.RpcResponse;
import io.github.nitouge.conduit.common.filter.Filter;
import io.github.nitouge.conduit.common.filter.FilterChain;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAccumulator;
import java.util.concurrent.atomic.LongAdder;

/**
 * 指标统计Filter：双侧记录调用量、成功率、RT（平均/最大/最小/分位估算）与延迟分布。
 *
 * <p><b>默认关闭</b>——用 {@code -Dconduit.filter.metrics.enabled=true} 开启。
 * 内部按 (interface + method) 维度分桶，用 {@link LongAdder}/{@link LongAccumulator} 做无锁并发统计。</p>
 *
 * <p><b>如何读取指标：</b>
 * <pre>{@code
 *   for (Map.Entry<String, MetricsFilter.Stats> e : MetricsFilter.snapshot().entrySet()) {
 *       Stats s = e.getValue();
 *       System.out.printf("%s total=%d err=%.2f%% avg=%.2fms p95≈%.0fms max=%.0fms%n",
 *           e.getKey(), s.total(), s.errorRate() * 100, s.avgRtMs(),
 *           s.estimatedPercentileMs(0.95), s.maxRtMs());
 *   }
 * }</pre>
 *
 * <p><b>取舍：</b>分位由固定桶（1/5/10/50/100/500/1000ms）估算，非精确直方图；全量累计、可重置。
 * 无外部依赖（不耦合 Micrometer/Prometheus），导出器作为后续能力。</p>
 */
public class MetricsFilter implements Filter {

    /** 延迟分桶上界（毫秒）；超过最后一个上界计入 extra 桶 */
    static final long[] BUCKET_BOUNDS_MS = {1, 5, 10, 50, 100, 500, 1000};

    /** 指标快照：key=interface#method；值=该桶累计 */
    private static final Map<String, Stats> STATS = new ConcurrentHashMap<>();

    public static void reset() {
        STATS.clear();
    }

    /** 取所有桶的当前快照（只读引用） */
    public static Map<String, Stats> snapshot() {
        return STATS;
    }

    /** 全量调用次数（所有接口/方法求和） */
    public static long totalCalls() {
        long total = 0;
        for (Stats stats : STATS.values()) {
            total += stats.total();
        }
        return total;
    }

    /** 全量失败次数 */
    public static long totalErrors() {
        long total = 0;
        for (Stats stats : STATS.values()) {
            total += stats.error();
        }
        return total;
    }

    @Override
    public CompletableFuture<RpcResponse> invoke(RpcRequest request, FilterChain chain) {
        long startNanos = System.nanoTime();
        String key = keyOf(request);

        return chain.invoke(request).whenComplete((resp, err) -> {
            long elapsedNanos = System.nanoTime() - startNanos;
            boolean success = (err == null && (resp == null || !resp.hasException()));
            STATS.computeIfAbsent(key, k -> new Stats()).record(success, elapsedNanos);
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
        return request.getInterfaceName() + "#" + request.getMethodName();
    }

    /** 一个桶的累计统计（线程安全） */
    public static final class Stats {

        private final LongAdder total = new LongAdder();
        private final LongAdder success = new LongAdder();
        private final LongAdder error = new LongAdder();
        private final LongAdder rtNanos = new LongAdder();
        private final LongAccumulator maxRtNanos = new LongAccumulator(Long::max, 0L);
        private final LongAccumulator minRtNanos = new LongAccumulator(Long::min, Long.MAX_VALUE);
        private final LongAdder[] buckets = new LongAdder[BUCKET_BOUNDS_MS.length + 1];

        private Stats() {
            for (int i = 0; i < buckets.length; i++) {
                buckets[i] = new LongAdder();
            }
        }

        private void record(boolean ok, long elapsedNanos) {
            total.increment();
            if (ok) {
                success.increment();
            } else {
                error.increment();
            }
            rtNanos.add(elapsedNanos);
            maxRtNanos.accumulate(elapsedNanos);
            minRtNanos.accumulate(elapsedNanos);
            buckets[bucketIndex(elapsedNanos)].increment();
        }

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

        /** 最大RT（毫秒），total=0时返回0 */
        public double maxRtMs() {
            long t = total.sum();
            return t == 0 ? 0.0 : maxRtNanos.get() / 1_000_000.0;
        }

        /** 最小RT（毫秒），total=0时返回0 */
        public double minRtMs() {
            long t = total.sum();
            return t == 0 ? 0.0 : minRtNanos.get() / 1_000_000.0;
        }

        /** 错误率（0~1），total=0时返回0 */
        public double errorRate() {
            long t = total.sum();
            return t == 0 ? 0.0 : (double) error.sum() / t;
        }

        /** 第i个延迟桶的计数；i=0..6 对应上界，i=7 为超过1000ms */
        public long bucketCount(int index) {
            return (index >= 0 && index < buckets.length) ? buckets[index].sum() : 0L;
        }

        /** 所有桶计数（副本，下标语义同 {@link #bucketCount(int)}） */
        public long[] bucketCounts() {
            long[] copy = new long[buckets.length];
            for (int i = 0; i < buckets.length; i++) {
                copy[i] = buckets[i].sum();
            }
            return copy;
        }

        /**
         * 基于固定桶估算分位延迟（毫秒）。
         *
         * @param percentile 0~1，如0.95
         * @return 命中桶的上界；超过1000ms时返回最大RT
         */
        public double estimatedPercentileMs(double percentile) {
            long t = total.sum();
            if (t <= 0) {
                return 0.0;
            }
            double p = Math.min(Math.max(percentile, 0.0), 1.0);
            long target = Math.max(1, (long) Math.ceil(t * p));
            long cumulative = 0;
            for (int i = 0; i < buckets.length; i++) {
                cumulative += buckets[i].sum();
                if (cumulative >= target) {
                    return i < BUCKET_BOUNDS_MS.length ? BUCKET_BOUNDS_MS[i] : maxRtMs();
                }
            }
            return maxRtMs();
        }

        private static int bucketIndex(long elapsedNanos) {
            double ms = elapsedNanos / 1_000_000.0;
            for (int i = 0; i < BUCKET_BOUNDS_MS.length; i++) {
                if (ms <= BUCKET_BOUNDS_MS[i]) {
                    return i;
                }
            }
            return BUCKET_BOUNDS_MS.length;
        }
    }
}
