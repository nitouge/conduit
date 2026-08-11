package io.github.nitouge.conduit.common.filter;

import io.github.nitouge.conduit.api.RpcResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * 负责把有序的 {@link Filter} 列表反向包裹到一个「链尾终端」上，组成可调用的 {@link FilterChain}。
 *
 * <p>包裹方式：列表中第一个filter是最外层（先执行前置），最后一个是内层（最靠近终端）。
 * 例：filters=[A, B]，terminal=T → 调用链为A.before → B.before → T → B.after → A.after。
 * 反向遍历filters，每层用lambda捕获当前filter与下一环并构造新的chain。</p>
 *
 * <p>每个外层包裹都包了一个 {@code try/catch}：若某个filter在「同步前置」里抛了异常
 * （如限流拒绝直接throw），lambda会把它转成failed future——保证future语义统一，
 * 不让异常穿透到调用方线程上失控。</p>
 */
public final class FilterChainBuilder {

    private static final Logger log = LoggerFactory.getLogger(FilterChainBuilder.class);

    private FilterChainBuilder() {
    }

    /**
     * 把 {@code filters} 反向包裹在 {@code terminal} 上。
     *
     * <p>无filter时直接返回terminal（直通，零开销）。
     * {@code filters} 应已经按 {@code Filter.order()} 从小到大排好（{@link FilterLoader} 负责）。</p>
     */
    public static FilterChain build(List<Filter> filters, FilterChain terminal) {
        if (filters == null || filters.isEmpty()) {
            return terminal;
        }
        FilterChain chain = terminal;
        // 反向遍历：最后一个filter离terminal最近，第一个离terminal最远（最外层）
        for (int i = filters.size() - 1; i >= 0; i--) {
            final Filter current = filters.get(i);
            final FilterChain next = chain;
            chain = request -> {
                try {
                    return current.invoke(request, next);
                } catch (Throwable t) {
                    // filter的同步前置抛了异常——不该穿透到调用方线程，转failedFuture
                    log.warn("Filter: {} 同步前置抛异常，已包成failedFuture: {}", current.getClass().getName(), t.toString());
                    CompletableFuture<RpcResponse> failed = new CompletableFuture<>();
                    failed.completeExceptionally(t);
                    return failed;
                }
            };
        }
        return chain;
    }
}
