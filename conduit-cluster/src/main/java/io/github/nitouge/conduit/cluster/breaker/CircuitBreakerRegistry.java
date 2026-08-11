package io.github.nitouge.conduit.cluster.breaker;

import io.github.nitouge.conduit.api.ServiceMeta;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 按节点（host:port）维护熔断器：同一坏节点被单独熔断，FailOver重试时会自然避开它。
 *
 * <p>默认参数：连续失败 {@value #DEFAULT_FAILURE_THRESHOLD} 次打开，打开 {@value #DEFAULT_OPEN_DURATION_MS} ms后转半开探测。
 * M5可改为按服务/方法配置。</p>
 */
public final class CircuitBreakerRegistry {

    private CircuitBreakerRegistry() {
    }

    static final int DEFAULT_FAILURE_THRESHOLD = 5;
    static final long DEFAULT_OPEN_DURATION_MS = 5000;

    private static final Map<String, CircuitBreaker> BREAKERS = new ConcurrentHashMap<>();

    public static CircuitBreaker of(ServiceMeta node) {
        String key = node.getHost() + ":" + node.getPort();
        return BREAKERS.computeIfAbsent(key,
                k -> new CircuitBreaker(DEFAULT_FAILURE_THRESHOLD, DEFAULT_OPEN_DURATION_MS));
    }

    /** 测试用：清空所有熔断器状态 */
    public static void clear() {
        BREAKERS.clear();
    }
}
