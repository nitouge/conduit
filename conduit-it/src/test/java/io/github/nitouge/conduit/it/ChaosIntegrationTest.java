package io.github.nitouge.conduit.it;

import io.github.nitouge.conduit.api.ServiceMeta;
import io.github.nitouge.conduit.it.provider.CrossProcessProviderMain;
import io.github.nitouge.conduit.remoting.PendingRequests;
import io.github.nitouge.conduit.testkit.Await;
import io.github.nitouge.conduit.testkit.EchoService;
import io.github.nitouge.conduit.testkit.EchoServiceImpl;
import io.github.nitouge.conduit.testkit.Ports;
import io.github.nitouge.conduit.testkit.TestReset;
import io.github.nitouge.conduit.testkit.TestRpc;
import io.github.nitouge.conduit.testkit.TestServer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 阶段三 · Chaos基础场景：故障注入后系统要"快速失败且不泄漏"，恢复后能自愈。
 *
 * <p>三个场景对应分布式系统最常见的三类故障：</p>
 * <ol>
 *   <li><b>进程崩溃</b>：provider是独立JVM，调用在途时被强杀 → 请求必须立刻失败，
 *       而不是干等到调用超时（注意：优雅停机允许在途请求完成，只有真崩溃才会立即断链）；</li>
 *   <li><b>服务端缓慢</b>：客户端超时生效，且不得残留pending请求；</li>
 *   <li><b>服务端重启</b>：客户端重连后自动恢复调用。</li>
 * </ol>
 */
class ChaosIntegrationTest {

    @BeforeEach
    void reset() {
        TestReset.all();
    }

    /** 场景1：调用在途时provider进程被强杀（真崩溃），请求应快速失败 */
    @Test
    void inFlightCallFailsFastWhenProviderProcessIsKilled() throws Exception {
        int port = Ports.freePort();
        Process provider = startProvider(port);
        try {
            Await.until("provider未在30s内就绪", 30_000, () -> Ports.connectable(port));

            try (TestRpc rpc = TestRpc.create()) {
                ServiceMeta target = new ServiceMeta(EchoService.class.getName(), "127.0.0.1", port);
                EchoService echo = rpc.proxy(EchoService.class, target, 5_000);
                assertEquals("ok:warm", echo.echo("warm"));

                CompletableFuture<String> inFlight = CompletableFuture.supplyAsync(() -> echo.slow(3_000));
                Await.sleepQuietly(300); // 让慢调用真正进入在途状态

                long start = System.nanoTime();
                provider.destroyForcibly(); // 模拟进程崩溃
                assertThrows(ExecutionException.class,
                        () -> inFlight.get(5, TimeUnit.SECONDS), "provider崩溃后调用应异常完成");

                long elapsedMillis = (System.nanoTime() - start) / 1_000_000;
                assertTrue(elapsedMillis < 2_000,
                        "进程崩溃应在途请求快速失败（而不是等3s慢调用超时），实际 " + elapsedMillis + "ms");
                assertEquals(0, PendingRequests.pendingCount(), "连接断开后pending请求必须清理干净");
            }
        } finally {
            provider.destroyForcibly();
            provider.waitFor(10, TimeUnit.SECONDS);
        }
    }

    /** 场景2：服务端缓慢，客户端超时生效且不残留pending */
    @Test
    void timeoutLeavesNoPendingRequests() {
        TestServer server = TestServer.start(EchoService.class, new EchoServiceImpl("slow"));
        try (TestRpc rpc = TestRpc.create()) {
            EchoService echo = rpc.proxy(EchoService.class, server.meta(EchoService.class), 300);

            assertThrows(RuntimeException.class, () -> echo.slow(2_000), "超过调用超时应失败");
            assertEquals(0, PendingRequests.pendingCount(), "超时清理后不应残留在途请求");
        } finally {
            server.close();
        }
    }

    /** 场景3：服务端重启（同端口），客户端重连后调用恢复 */
    @Test
    void clientRecoversAfterServerRestart() {
        int port = Ports.freePort();
        ServiceMeta target = new ServiceMeta(EchoService.class.getName(), "127.0.0.1", port);

        TestServer first = TestServer.onPort(port)
                .export(EchoService.class, new EchoServiceImpl("first"))
                .start();
        try (TestRpc rpc = TestRpc.create()) {
            EchoService echo = rpc.proxy(EchoService.class, target, 2_000);
            assertEquals("ok:x", echo.echo("x"));

            first.close();

            TestServer second = restartOn(port, 10_000);
            try {
                Await.until("服务端重启后客户端应重连成功", 20_000, () -> {
                    try {
                        return "ok:again".equals(echo.echo("again"));
                    } catch (RuntimeException e) {
                        return false; // 重连窗口内的失败属预期
                    }
                });
            } finally {
                second.close();
            }
        } finally {
            first.close();
        }
    }

    /** 端口释放有TIME_WAIT窗口，重启时允许短暂重试 */
    private static TestServer restartOn(int port, long timeoutMillis) {
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
        RuntimeException last = null;
        while (System.nanoTime() < deadline) {
            try {
                return TestServer.onPort(port)
                        .export(EchoService.class, new EchoServiceImpl("second"))
                        .start();
            } catch (RuntimeException e) {
                last = e;
                Await.sleepQuietly(100);
            }
        }
        throw new IllegalStateException("端口：" + port + "在" + timeoutMillis + "ms内未能重新监听", last);
    }

    private static Process startProvider(int port) throws Exception {
        String javaBin = Path.of(System.getProperty("java.home"), "bin",
                System.getProperty("os.name").toLowerCase().contains("win") ? "java.exe" : "java").toString();
        return new ProcessBuilder(
                javaBin,
                "-cp", System.getProperty("java.class.path"),
                CrossProcessProviderMain.class.getName(),
                String.valueOf(port))
                .redirectErrorStream(true)
                .start();
    }
}
