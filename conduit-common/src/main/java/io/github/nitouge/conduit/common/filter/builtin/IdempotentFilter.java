package io.github.nitouge.conduit.common.filter.builtin;

import io.github.nitouge.conduit.api.RpcAttachments;
import io.github.nitouge.conduit.api.RpcRequest;
import io.github.nitouge.conduit.api.RpcResponse;
import io.github.nitouge.conduit.common.config.ConduitConfig;
import io.github.nitouge.conduit.common.filter.Filter;
import io.github.nitouge.conduit.common.filter.FilterChain;
import io.github.nitouge.conduit.common.filter.Side;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 幂等去重Filter（提供侧）：同一个幂等键只真正执行一次，重复请求共享第一次的结果。
 *
 * <p><b>它解决什么</b>：Failover 会重放请求，网络超时也会让「到底执行没执行」变成未知。带上幂等键
 * （见 {@link IdempotencyFilter}）后，重放落到同一节点时能命中结果缓存，不会重复下单。</p>
 *
 * <p><b>三种命中情形</b>：</p>
 * <ol>
 *   <li>键已存在且已完成 → 直接返回缓存结果（不执行业务）；</li>
 *   <li>键已存在但仍在执行 → 把新请求挂到同一个 future 上（并发重复不会双重执行）；</li>
 *   <li>键不存在 → 正常执行，结果按 TTL 保留供后续去重。</li>
 * </ol>
 *
 * <p>失败<b>不缓存</b>：传输/框架异常时移除条目并向下传播，否则一个偶然的失败会让这个键在 TTL 内
 * 永远只能拿到失败结果，重试就失去意义。</p>
 *
 * <p>默认启用（{@code conduit.filter.idempotent.enabled=false} 关闭）。配置：</p>
 * <ul>
 *   <li>{@code conduit.filter.idempotent.ttl-ms}（默认 60000）：结果保留多久；</li>
 *   <li>{@code conduit.filter.idempotent.max-entries}（默认 4096）：缓存条目上限，超出后淘汰已完成的旧条目。</li>
 * </ul>
 *
 * <p><b>边界（重要）</b>：去重表是<b>进程内</b>的，只对「重放落到同一实例」有效。跨节点的重复执行
 * 需要集中式存储（Redis/DB 唯一键）；本Filter的定位是「不引入外部依赖时的第一道防线」，而不是
 * 分布式事务。另外它只按 key 去重，不校验参数是否一致——幂等键必须由调用方保证语义唯一。</p>
 */
public class IdempotentFilter implements Filter {

    private static final Logger log = LoggerFactory.getLogger(IdempotentFilter.class);

    private static final long DEFAULT_TTL_MS = 60_000L;
    private static final int DEFAULT_MAX_ENTRIES = 4096;

    /** 两次清理之间的最小间隔：避免每个请求都扫一遍表 */
    private static final long SWEEP_INTERVAL_MS = 1_000L;

    private final Map<String, Entry> cache = new ConcurrentHashMap<>();
    private final AtomicLong lastSweep = new AtomicLong();

    @Override
    public CompletableFuture<RpcResponse> invoke(RpcRequest request, FilterChain chain) {
        String rawKey = request.getAttachment(RpcAttachments.IDEMPOTENCY_KEY);
        if (rawKey == null || rawKey.isBlank()) {
            // 未携带幂等键：保持原有语义，零行为变化
            return chain.invoke(request);
        }
        String key = request.getInterfaceName() + '#' + request.getMethodName() + ':' + rawKey.trim();
        long now = System.currentTimeMillis();
        evictIfNeeded(now);

        Entry existing = cache.get(key);
        if (existing != null) {
            log.debug("[IdempotentFilter] 命中幂等键 {}（{}），复用已有结果/等待首次执行",
                    key, existing.future.isDone() ? "已完成" : "执行中");
            return existing.future;
        }
        Entry mine = new Entry(now);
        Entry previous = cache.putIfAbsent(key, mine);
        if (previous != null) {
            // 竞态：另一个线程刚把这个键放进去，让它当执行者，我们等它的结果
            return previous.future;
        }

        CompletableFuture<RpcResponse> result;
        try {
            result = chain.invoke(request);
        } catch (Throwable t) {
            // 同步抛出：不能把失败留在缓存里，否则这个键在TTL内都无法重试
            cache.remove(key, mine);
            mine.future.completeExceptionally(t);
            return mine.future;
        }
        result.whenComplete((response, error) -> {
            if (error != null) {
                cache.remove(key, mine);
                mine.future.completeExceptionally(error);
            } else {
                mine.future.complete(response);
            }
        });
        return mine.future;
    }

    @Override
    public int order() {
        // 在限流(40)/并发隔离(50)之外：重复请求应尽早被吸收，不该消耗限流配额和并发许可
        return 20;
    }

    @Override
    public boolean appliesTo(Side side) {
        return side == Side.PROVIDER;
    }

    @Override
    public String name() {
        return "idempotent";
    }

    /**
     * 按 TTL / 容量上界清理：内存必须有上界，否则去重表本身就成了泄漏源。
     *
     * <p>懒清理（在请求路径上顺带做）+ 最小间隔：不额外起线程，也不让每次请求都全表扫描。</p>
     */
    private void evictIfNeeded(long now) {
        int maxEntries = ConduitConfig.getPositiveInt("conduit.filter.idempotent.max-entries", DEFAULT_MAX_ENTRIES);
        long ttlMillis = ConduitConfig.getPositiveLong("conduit.filter.idempotent.ttl-ms", DEFAULT_TTL_MS);
        long last = lastSweep.get();
        if (now - last < SWEEP_INTERVAL_MS && cache.size() <= maxEntries) {
            return;
        }
        if (!lastSweep.compareAndSet(last, now)) {
            return;
        }
        cache.entrySet().removeIf(entry -> {
            Entry value = entry.getValue();
            return value.future.isDone() && now - value.createdAt > ttlMillis;
        });
        if (cache.size() > maxEntries) {
            // 仍然超限（短时间大量不同键）：淘汰已完成的旧条目，优先保住执行中的条目
            int toRemove = cache.size() - maxEntries;
            Iterator<Map.Entry<String, Entry>> iterator = cache.entrySet().iterator();
            while (toRemove > 0 && iterator.hasNext()) {
                if (iterator.next().getValue().future.isDone()) {
                    iterator.remove();
                    toRemove--;
                }
            }
        }
    }

    /** 一条去重记录：future 同时服务首次执行者与所有重复请求 */
    private static final class Entry {

        private final CompletableFuture<RpcResponse> future = new CompletableFuture<>();
        private final long createdAt;

        private Entry(long createdAt) {
            this.createdAt = createdAt;
        }
    }
}
