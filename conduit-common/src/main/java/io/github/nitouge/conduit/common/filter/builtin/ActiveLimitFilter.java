package io.github.nitouge.conduit.common.filter.builtin;

import io.github.nitouge.conduit.api.RpcRequest;
import io.github.nitouge.conduit.api.RpcResponse;
import io.github.nitouge.conduit.common.exception.RpcException;
import io.github.nitouge.conduit.common.filter.Filter;
import io.github.nitouge.conduit.common.filter.FilterChain;
import io.github.nitouge.conduit.common.filter.Side;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;

/**
 * 并发隔离Filter（提供侧，信号量舱壁）：限制单个接口#方法在途并发数，超限快速失败。
 *
 * <p><b>默认关闭</b>——用 {@code -Dconduit.filter.activelimit.enabled=true} 开启。
 * 每方法上限用系统属性 {@code conduit.filter.activelimit.permits}（默认200）；
 * 也可用 {@code conduit.filter.activelimit.<method>.permits} 覆盖单个方法（method=接口#方法）。</p>
 *
 * <p>用 {@link Semaphore#tryAcquire()} 非阻塞获取令牌：拿不到立即抛 {@link RpcException}（快速失败，
 * 保护提供者不被瞬时高并发压垮），拿到则在调用完成（无论成功失败）后释放。这是信号量隔离，相比线程池隔离更轻量、无额外线程切换。</p>
 *
 * <p>取舍：只做并发数隔离，不做QPS限流（令牌桶留 §1.3 RateLimitFilter）；permits静态配置，
 * 动态调整依赖§1.8配置中心。</p>
 */
public class ActiveLimitFilter implements Filter {

    private static final int DEFAULT_PERMITS = 200;

    /** 每方法一个信号量：key=接口#方法 */
    private final Map<String, Semaphore> semaphores = new ConcurrentHashMap<>();

    @Override
    public CompletableFuture<RpcResponse> invoke(RpcRequest request, FilterChain chain) {
        String key = request.getInterfaceName() + "#" + request.getMethodName();
        Semaphore semaphore = semaphores.computeIfAbsent(key, this::createSemaphore);

        if (!semaphore.tryAcquire()) {
            CompletableFuture<RpcResponse> failed = new CompletableFuture<>();
            failed.completeExceptionally(new RpcException("并发超限(ActiveLimit)，方法 " + key + " 在途请求已达上限，快速失败"));
            return failed;
        }

        try {
            // 调用完成（成功/失败）后释放令牌——用whenComplete保证异步路径也能释放
            return chain.invoke(request).whenComplete((resp, err) -> semaphore.release());
        } catch (Throwable t) {
            // chain.invoke同步抛出（理论上不会，FilterChainBuilder已兜底）——补释放，避免令牌泄漏
            semaphore.release();
            throw t;
        }
    }

    @Override
    public int order() {
        return 50; // 介于access-log(-100)与exception(100)之间：先记日志，再限流
    }

    @Override
    public boolean appliesTo(Side side) {
        return side == Side.PROVIDER;
    }

    @Override
    public boolean enabled() {
        return false; // 默认关闭：明确开启才隔离
    }

    @Override
    public String name() {
        return "activelimit";
    }

    private Semaphore createSemaphore(String key) {
        // 单方法覆盖优先，否则用全局默认
        String perMethod = System.getProperty("conduit.filter.activelimit." + key + ".permits");
        String global = System.getProperty("conduit.filter.activelimit.permits");
        int permits = parsePositive(perMethod, parsePositive(global, DEFAULT_PERMITS));
        return new Semaphore(permits);
    }

    private static int parsePositive(String val, int fallback) {
        if (val == null) {
            return fallback;
        }
        try {
            int n = Integer.parseInt(val.trim());
            return n > 0 ? n : fallback;
        } catch (NumberFormatException e) {
            return fallback;
        }
    }
}
