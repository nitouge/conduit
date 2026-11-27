package io.github.nitouge.conduit.testkit;

import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

/**
 * 条件等待：替代`Thread.sleep(固定值)`，让"注册生效/订阅推送/连接就绪"这类最终一致的断言稳定但不慢。
 */
public final class Await {

    /** 默认超时（毫秒） */
    public static final long DEFAULT_TIMEOUT_MILLIS = 5_000;

    private static final long POLL_INTERVAL_MILLIS = 20;

    private Await() {
    }

    /** 轮询直到条件成立；超时抛 {@link AssertionError}（带描述，便于定位） */
    public static void until(String description, long timeoutMillis, BooleanSupplier condition) {
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
        while (System.nanoTime() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            sleepQuietly(POLL_INTERVAL_MILLIS);
        }
        if (!condition.getAsBoolean()) {
            throw new AssertionError("等待超时(" + timeoutMillis + "ms): " + description);
        }
    }

    /** 同上，用默认超时 */
    public static void until(String description, BooleanSupplier condition) {
        until(description, DEFAULT_TIMEOUT_MILLIS, condition);
    }

    /** 等待布尔条件为真（可直接传lambda） */
    public static void untilTrue(String description, BooleanSupplier condition) {
        until(description, DEFAULT_TIMEOUT_MILLIS, condition);
    }

    /** 安静地睡一会儿；被中断时恢复中断标记 */
    public static void sleepQuietly(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
