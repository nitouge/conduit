package io.github.nitouge.conduit.it;

import io.github.nitouge.conduit.api.ServiceMeta;
import io.github.nitouge.conduit.registry.local.InMemoryRegistry;
import io.github.nitouge.conduit.testkit.linearizability.HistoryOperation;
import io.github.nitouge.conduit.testkit.linearizability.LinearizabilityChecker;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Random;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 阶段三：对"并发语义可归约为寄存器"的组件做线性一致性检测。
 *
 * <p>模型：{@code InMemoryRegistry} 里"某实例是否已注册"就是一个布尔寄存器——
 * register = 写true、unregister = 写false、discover是否包含 = 读。
 * 多线程随机混合这三类操作，记录每次操作的调用/返回区间与观察结果，
 * 交给 {@link LinearizabilityChecker}（Wing &amp; Gong）判断是否存在合法的全局顺序。</p>
 *
 * <p>这类检测能抓到"注册后立刻发现不到""注销后仍被读到"等靠普通断言很难覆盖的竞态。</p>
 */
class InMemoryRegistryLinearizabilityTest {

    private static final String SERVICE = "io.github.nitouge.conduit.it.LinearizableService";
    private static final int THREADS = 4;
    private static final int OPS_PER_THREAD = 5;

    @Test
    void membershipFlagIsLinearizable() throws Exception {
        InMemoryRegistry registry = new InMemoryRegistry();
        ServiceMeta meta = new ServiceMeta(SERVICE, "127.0.0.1", 7001);
        List<HistoryOperation> history = new CopyOnWriteArrayList<>();

        ExecutorService pool = Executors.newFixedThreadPool(THREADS);
        CountDownLatch start = new CountDownLatch(1);
        List<Future<?>> tasks = new CopyOnWriteArrayList<>();
        try {
            for (int t = 0; t < THREADS; t++) {
                String process = "t" + t;
                tasks.add(pool.submit(() -> {
                    Random random = new Random(process.hashCode());
                    start.await();
                    for (int i = 0; i < OPS_PER_THREAD; i++) {
                        int choice = random.nextInt(3);
                        long startNanos = System.nanoTime();
                        if (choice == 0) {
                            registry.register(meta);
                            history.add(HistoryOperation.write(process, Boolean.TRUE,
                                    startNanos, System.nanoTime()));
                        } else if (choice == 1) {
                            registry.unregister(meta);
                            history.add(HistoryOperation.write(process, Boolean.FALSE,
                                    startNanos, System.nanoTime()));
                        } else {
                            boolean present = registry.discover(SERVICE).contains(meta);
                            history.add(HistoryOperation.read(process, present,
                                    startNanos, System.nanoTime()));
                        }
                    }
                    return null;
                }));
            }
            start.countDown();
            pool.shutdown();
            assertTrue(pool.awaitTermination(30, TimeUnit.SECONDS));
            for (Future<?> task : tasks) {
                task.get(1, TimeUnit.SECONDS);
            }
        } finally {
            pool.shutdownNow();
        }

        assertTrue(history.size() == THREADS * OPS_PER_THREAD, "每个操作都应记录到历史");
        // 初值false：一开始该实例并未注册
        boolean linearizable = LinearizabilityChecker.isLinearizable(history, Boolean.FALSE);
        assertTrue(linearizable, () -> "InMemoryRegistry的成员可见性应线性一致，实际历史:\n"
                + String.join("\n", history.stream().map(Object::toString).toList()));
    }
}
