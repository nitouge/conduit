package io.github.nitouge.conduit.cluster;

import io.github.nitouge.conduit.api.ServiceMeta;
import io.github.nitouge.conduit.cluster.breaker.CircuitBreaker;
import io.github.nitouge.conduit.cluster.breaker.CircuitBreakerRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 熔断器参数可配置：新节点使用新参数，已创建节点保持不变；clear 恢复默认。
 */
class CircuitBreakerRegistryTest {

    private final ServiceMeta node = new ServiceMeta("svc", "10.1.1.1", 1);
    private final ServiceMeta otherNode = new ServiceMeta("svc", "10.1.1.2", 2);

    @AfterEach
    void tearDown() {
        CircuitBreakerRegistry.clear();
    }

    @Test
    void configureAppliesToNewlyCreatedBreakers() throws InterruptedException {
        CircuitBreakerRegistry.configure(CircuitBreakerOptions.builder()
                .failureThreshold(2)
                .openDurationMs(100)
                .build());

        CircuitBreaker breaker = CircuitBreakerRegistry.of(node);
        breaker.onFailure();
        assertEquals(CircuitBreaker.State.CLOSED, CircuitBreakerRegistry.state(node));
        breaker.onFailure();
        assertEquals(CircuitBreaker.State.OPEN, CircuitBreakerRegistry.state(node));

        Thread.sleep(150);
        assertTrue(breaker.allowRequest());
        assertEquals(CircuitBreaker.State.HALF_OPEN, CircuitBreakerRegistry.state(node));
    }

    @Test
    void existingBreakerKeepsItsOptionsAfterReconfigure() {
        CircuitBreaker existing = CircuitBreakerRegistry.of(node); // 默认阈值5

        CircuitBreakerRegistry.configure(CircuitBreakerOptions.builder().failureThreshold(1).build());

        assertSame(existing, CircuitBreakerRegistry.of(node), "同一节点应复用已存在的熔断器");
        existing.onFailure();
        assertEquals(CircuitBreaker.State.CLOSED, existing.getState(), "已有熔断器不应被新配置改写");

        CircuitBreaker fresh = CircuitBreakerRegistry.of(otherNode);
        fresh.onFailure();
        assertEquals(CircuitBreaker.State.OPEN, fresh.getState(), "新节点应使用新阈值");
    }

    @Test
    void resetRemovesSingleNodeState() {
        CircuitBreakerRegistry.of(node);
        assertNotNull(CircuitBreakerRegistry.state(node));

        CircuitBreakerRegistry.reset(node);

        assertNull(CircuitBreakerRegistry.state(node));
        assertEquals(0, CircuitBreakerRegistry.size());
    }

    @Test
    void clearRestoresDefaults() {
        CircuitBreakerRegistry.configure(CircuitBreakerOptions.builder()
                .failureThreshold(2)
                .openDurationMs(10)
                .build());
        CircuitBreakerRegistry.of(node);

        CircuitBreakerRegistry.clear();

        assertEquals(CircuitBreakerOptions.DEFAULT_FAILURE_THRESHOLD,
                CircuitBreakerRegistry.options().getFailureThreshold());
        assertEquals(CircuitBreakerOptions.DEFAULT_OPEN_DURATION_MS,
                CircuitBreakerRegistry.options().getOpenDurationMs());
        assertEquals(0, CircuitBreakerRegistry.size());
        assertNull(CircuitBreakerRegistry.state(node));
    }
}
