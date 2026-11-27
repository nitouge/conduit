package io.github.nitouge.conduit.it;

import io.github.nitouge.conduit.api.ServiceMeta;
import io.github.nitouge.conduit.it.provider.CrossProcessProviderMain;
import io.github.nitouge.conduit.testkit.Await;
import io.github.nitouge.conduit.testkit.EchoService;
import io.github.nitouge.conduit.testkit.Ports;
import io.github.nitouge.conduit.testkit.TestReset;
import io.github.nitouge.conduit.testkit.TestRpc;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 真正的跨进程验证：provider跑在<b>另一个JVM</b>（独立main进程），consumer在测试JVM里通过
 * 真实TCP + 完整RPC链路调用。节点标识里带子进程pid，用来证明没有被"同进程优化"掉。
 *
 * <p>依赖surefire的 {@code useManifestOnlyJar=false}：子JVM复用测试 classpath。</p>
 */
class CrossProcessJvmIntegrationTest {

    private static final String HOST = "127.0.0.1";

    @BeforeEach
    void reset() {
        TestReset.all();
    }

    @Test
    void callsProviderRunningInSeparateJvm() throws Exception {
        int port = Ports.freePort();
        Process child = startProvider(port);
        StringBuffer childOutput = captureOutput(child);
        try {
            Await.until("子进程provider未在30s内监听 " + port, 30_000, () -> Ports.connectable(port));

            long currentPid = ProcessHandle.current().pid();
            try (TestRpc rpc = TestRpc.create()) {
                EchoService echo = rpc.proxy(EchoService.class,
                        new ServiceMeta(EchoService.class.getName(), HOST, port), 5_000);

                String nodeId = echo.nodeId();
                assertTrue(nodeId.startsWith("child-"), "节点标识应来自子进程: " + nodeId);
                long childPid = Long.parseLong(nodeId.substring("child-".length()));
                assertNotEquals(currentPid, childPid, "必须真的是另一个进程");

                assertEquals("ok:cross-process", echo.echo("cross-process"));
            }
        } catch (AssertionError e) {
            throw new AssertionError(e.getMessage() + "\n子进程输出:\n" + childOutput, e);
        } finally {
            child.destroyForcibly();
            assertTrue(child.waitFor(10, TimeUnit.SECONDS), "子进程应被结束");
        }

        // 子进程结束后，再调用应失败（证明之前确实连的是它）
        try (TestRpc rpc = TestRpc.create()) {
            EchoService echo = rpc.proxy(EchoService.class,
                    new ServiceMeta(EchoService.class.getName(), HOST, port), 1_500);
            assertThrows(RuntimeException.class, () -> echo.echo("gone"));
        }
    }

    private static Process startProvider(int port) throws Exception {
        String javaBin = Path.of(System.getProperty("java.home"), "bin",
                System.getProperty("os.name").toLowerCase().contains("win") ? "java.exe" : "java").toString();
        String classpath = System.getProperty("java.class.path");
        return new ProcessBuilder(
                javaBin,
                "-cp", classpath,
                CrossProcessProviderMain.class.getName(),
                String.valueOf(port))
                .redirectErrorStream(true)
                .start();
    }

    /** 后台线程持续读子进程输出，失败时拼进断言消息里便于定位 */
    private static StringBuffer captureOutput(Process process) {
        StringBuffer buffer = new StringBuffer();
        Thread reader = new Thread(() -> {
            try (BufferedReader in = new BufferedReader(
                    new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8))) {
                String line;
                while ((line = in.readLine()) != null) {
                    buffer.append(line).append('\n');
                }
            } catch (Exception ignored) {
                // 进程被销毁时读流会抛异常，属预期
            }
        }, "it-child-output-reader");
        reader.setDaemon(true);
        reader.start();
        return buffer;
    }
}
