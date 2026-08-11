package io.github.nitouge.conduit.demo.filter;

import io.github.nitouge.conduit.api.RpcRequest;
import io.github.nitouge.conduit.api.RpcResponse;
import io.github.nitouge.conduit.common.filter.Filter;
import io.github.nitouge.conduit.common.filter.FilterChain;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Filter验收测试用：双侧生效，记录进入/退出次数与顺序。
 *
 * <p>注意：在单JVM端到端测试里，SPI单例复用意味着<b>同一个实例</b>会被消费侧与提供侧各调用一次
 * （因 {@code ExtensionLoader.getExtension} 按名缓存单例）。因此一次端到端调用中：
 * before计数+2、after计数+2，顺序体现「外层先进后出」的责任链嵌套。</p>
 *
 * <p>静态状态 + {@code synchronized} 列表提供跨线程（异步after可能在回调线程）读取快照。</p>
 */
public class CountingFilter implements Filter {

    public static final AtomicInteger BEFORE = new AtomicInteger();

    public static final AtomicInteger AFTER = new AtomicInteger();

    /** 记录before/after交错顺序，验证责任链嵌套（before,before,...,after,after） */
    public static final List<String> ORDER = Collections.synchronizedList(new ArrayList<>());

    public static void reset() {
        BEFORE.set(0);
        AFTER.set(0);
        ORDER.clear();
    }

    @Override
    public CompletableFuture<RpcResponse> invoke(RpcRequest request, FilterChain chain) {
        BEFORE.incrementAndGet();
        ORDER.add("before");
        return chain.invoke(request).whenComplete((resp, err) -> {
            AFTER.incrementAndGet();
            ORDER.add("after");
        });
    }
}