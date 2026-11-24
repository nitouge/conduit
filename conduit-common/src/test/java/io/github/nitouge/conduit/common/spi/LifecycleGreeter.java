package io.github.nitouge.conduit.common.spi;

import java.util.concurrent.atomic.AtomicInteger;

/** 生命周期测试用扩展：记录init/destroy次数。 */
public class LifecycleGreeter implements Greeter, Lifecycle {

    public static final AtomicInteger INIT_COUNT = new AtomicInteger();
    public static final AtomicInteger DESTROY_COUNT = new AtomicInteger();

    public static void resetCounters() {
        INIT_COUNT.set(0);
        DESTROY_COUNT.set(0);
    }

    @Override
    public void init() {
        INIT_COUNT.incrementAndGet();
    }

    @Override
    public void destroy() {
        DESTROY_COUNT.incrementAndGet();
    }

    @Override
    public String greet() {
        return "lifecycle";
    }
}
