package io.github.nitouge.conduit.cluster.breaker;

import io.github.nitouge.conduit.api.ServiceMeta;
import io.github.nitouge.conduit.cluster.CircuitBreakerOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 按节点（host:port）维护熔断器：同一坏节点被单独熔断，FailOver重试时会自然避开它。
 *
 * <p>参数可配置：{@link #configure(CircuitBreakerOptions)} 调整默认失败阈值与打开时长（Spring场景
 * 对应 {@code conduit.cluster.breaker.*}）；已创建的熔断器保持创建时的参数，新节点使用新参数。</p>
 *
 * <p>{@link #clear()} 用于测试隔离：清空所有节点状态<b>并恢复默认参数</b>。</p>
 */
public final class CircuitBreakerRegistry {

    private static final Logger log = LoggerFactory.getLogger(CircuitBreakerRegistry.class);

    private static final Map<String, CircuitBreaker> BREAKERS = new ConcurrentHashMap<>();

    private static volatile CircuitBreakerOptions options = CircuitBreakerOptions.defaults();

    private CircuitBreakerRegistry() {
    }

    /** 调整新建熔断器的默认参数（已存在的节点熔断器不受影响） */
    public static void configure(CircuitBreakerOptions newOptions) {
        options = (newOptions == null) ? CircuitBreakerOptions.defaults() : newOptions;
        log.debug("[CircuitBreaker] 配置更新: {}", options);
    }

    /** 当前默认参数 */
    public static CircuitBreakerOptions options() {
        return options;
    }

    /** 取/创建某节点的熔断器（使用当前默认参数） */
    public static CircuitBreaker of(ServiceMeta node) {
        CircuitBreakerOptions current = options;
        String key = key(node);
        return BREAKERS.computeIfAbsent(key,
                k -> new CircuitBreaker(current.getFailureThreshold(), current.getOpenDurationMs()));
    }

    /** 某节点当前熔断状态；未创建过熔断器返回null */
    public static CircuitBreaker.State state(ServiceMeta node) {
        CircuitBreaker breaker = BREAKERS.get(key(node));
        return breaker == null ? null : breaker.getState();
    }

    /** 移除某节点的熔断状态（下次会按当前参数重建） */
    public static void reset(ServiceMeta node) {
        BREAKERS.remove(key(node));
    }

    /** 当前维护的节点数（观测用） */
    public static int size() {
        return BREAKERS.size();
    }

    /** 清空所有熔断器状态并恢复默认参数（测试/运维重置用） */
    public static void clear() {
        BREAKERS.clear();
        options = CircuitBreakerOptions.defaults();
    }

    private static String key(ServiceMeta node) {
        return node.getHost() + ":" + node.getPort();
    }
}
