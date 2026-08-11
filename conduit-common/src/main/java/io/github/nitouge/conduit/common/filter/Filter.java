package io.github.nitouge.conduit.common.filter;

import io.github.nitouge.conduit.api.RpcRequest;
import io.github.nitouge.conduit.api.RpcResponse;
import io.github.nitouge.conduit.common.spi.SPI;

import java.util.concurrent.CompletableFuture;

/**
 * Filter拦截链上的可插拔拦截点。
 *
 * <p>{@link #invoke(RpcRequest, FilterChain)} 内部通常写法：
 * <pre>{@code
 *   public CompletableFuture<RpcResponse> invoke(RpcRequest req, FilterChain next) {
 *       // 前置：限流/打日志/生成 traceId
 *       return next.invoke(req).whenComplete((resp, err) -> {
 *           // 后置：计时/计数/打日志（线程可能不在调用线程）
 *       });
 *   }
 * }</pre>
 * 返回future让同步/异步两条路径统一处理——同步方法返回已完成future；异步方法返回真实future。</p>
 *
 * <p><b>实现细节：</b></p>
 * <ul>
 *   <li>同步抛出的异常（如限流直接拒绝）须自行包成 {@link CompletableFuture} 失败——
 *       不要让异常穿透到链外，那样会丢失调用现场、不被业务侧感知；
 *       {@link FilterChainBuilder} 会在外层catch一次做兜底（但应习惯自己处理）。</li>
 *   <li>{@link #invoke} 应在有限时间内完成「前置」同步逻辑；不要在「前置」里阻塞等待异步IO
 *       ——若必须，让filter自己异步化（已有实现均为同步前置）。</li>
 * </ul>
 *
 * <p><b>SPI配置：</b>无默认扩展名（语义是「全部加载」而非「选默认」）。
 * 多条filter的执行顺序由 {@link #order()} 决定：<b>值小的靠外层、先执行</b>。
 * 双侧生效默认开启，按 {@link #appliesTo(Side)} 控制——例如某些filter只跑消费侧。</p>
 *
 * <p>框架默认不提供任何内置filter；用户按需在 {@code META-INF/rpc/io.github.nitouge.conduit.common.filter.Filter}中注册。无注册时所有调用直通，零行为变化。</p>
 */
@SPI
public interface Filter {

    /**
     * 执行拦截；返回下一环（或链尾）的 {@link RpcResponse} future。
     *
     * <p>任何失败应体现在future的异常完成上，而非同步抛出。</p>
     */
    CompletableFuture<RpcResponse> invoke(RpcRequest request, FilterChain chain);

    /**
     * Filter排序：值小的<b>靠外层</b>（先执行前置）；同序时按SPI注册顺序稳定排序。
     *
     * <p>默认0。可选±10、±100等整数层级约定，避免相邻挤在一起。</p>
     */
    default int order() {
        return 0;
    }

    /**
     * 本filter是否作用于 {@link Side}。默认双侧都生效。
     *
     * <p>Consistency check：消费侧加载时，过滤出 {@code appliesTo(CONSUMER)} 的子集；
     * 提供侧加载时过滤出 {@code appliesTo(PROVIDER)} 的子集。</p>
     */
    default boolean appliesTo(Side side) {
        return true;
    }

    /**
     * 是否启用本filter。默认启用。
     *
     * <p>由 {@link FilterLoader} 在装载时按系统属性 {@code conduit.filter.<key>.enabled}
     * 决定——{@code <key>} 优先用 {@link #name()} 的小写形式（用户可读短名），
     * 找不到时退化到类全限定名小写。属性未设置时一律按 {@code true}（即无配置 = 启用）。</p>
     *
     * <p>典型用途：内置filter默认注册、用户用环境变量/JVM参数按需关掉：
     * <pre>{@code
     *   -Dconduit.filter.accesslog.enabled=false
     *   -Dconduit.filter.metrics.enabled=true
     * }</pre>
     */
    default boolean enabled() {
        return true;
    }

    /**
     * Filter的稳定标识（用于 {@code enabled} 开关查找与日志）。
     *
     * <p>默认用类全限定名（稳定、与类一一对应）。子类可覆盖为短名（如 {@code "accesslog"}），
     * 方便配置时使用更短的属性键。</p>
     */
    default String name() {
        return getClass().getName();
    }
}
