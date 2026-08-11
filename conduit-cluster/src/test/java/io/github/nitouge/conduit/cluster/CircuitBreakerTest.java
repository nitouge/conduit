package io.github.nitouge.conduit.cluster;

import io.github.nitouge.conduit.cluster.breaker.CircuitBreaker;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CircuitBreakerTest {

    @Test
    void opensAfterThresholdThenHalfOpensAndRecovers() throws InterruptedException {
        CircuitBreaker cb = new CircuitBreaker(3, 100); // 3次失败打开，100ms后半开

        assertTrue(cb.allowRequest());
        cb.onFailure();
        cb.onFailure();
        assertEquals(CircuitBreaker.State.CLOSED, cb.getState()); // 未达阈值
        cb.onFailure(); // 第3次
        assertEquals(CircuitBreaker.State.OPEN, cb.getState());
        assertFalse(cb.allowRequest()); // 打开期间拒绝

        Thread.sleep(150);
        assertTrue(cb.allowRequest()); // 转半开，放探测
        assertEquals(CircuitBreaker.State.HALF_OPEN, cb.getState());

        cb.onSuccess(); // 探测成功 → 关闭
        assertEquals(CircuitBreaker.State.CLOSED, cb.getState());
        assertTrue(cb.allowRequest());
    }

    @Test
    void halfOpenFailureReopens() throws InterruptedException {
        CircuitBreaker cb = new CircuitBreaker(1, 100);

        cb.onFailure(); // 阈值1 → 直接打开
        assertEquals(CircuitBreaker.State.OPEN, cb.getState());
        assertFalse(cb.allowRequest());

        Thread.sleep(150);
        assertTrue(cb.allowRequest()); // 半开
        cb.onFailure();                // 探测失败 → 重新打开
        assertEquals(CircuitBreaker.State.OPEN, cb.getState());
        assertFalse(cb.allowRequest());
    }

    /** 修复：迟到的onSuccess（请求在转OPEN前发出）不应把刚打开的熔断器撤销回CLOSED */
    @Test
    void staleSuccessDoesNotCloseOpenBreaker() {
        CircuitBreaker cb = new CircuitBreaker(3, 1000);
        cb.onFailure();
        cb.onFailure();
        cb.onFailure(); // → OPEN
        assertEquals(CircuitBreaker.State.OPEN, cb.getState());

        cb.onSuccess(); // 迟到成功，应被忽略
        assertEquals(CircuitBreaker.State.OPEN, cb.getState());
        assertFalse(cb.allowRequest());
    }

    /** 修复：HALF_OPEN只放一个探测，其余请求被拒绝 */
    @Test
    void halfOpenAdmitsOnlyOneProbe() throws InterruptedException {
        CircuitBreaker cb = new CircuitBreaker(1, 100);
        cb.onFailure(); // → OPEN
        Thread.sleep(150);

        assertTrue(cb.allowRequest());  // 第一个：探测放行
        assertFalse(cb.allowRequest()); // 第二个：半开期已有探测在途，拒绝
    }
}
