package io.github.nitouge.conduit.it;

import io.github.nitouge.conduit.it.multi.ConsumerNodeMain;
import io.github.nitouge.conduit.it.multi.NodeService;
import io.github.nitouge.conduit.it.multi.ProviderNodeMain;
import io.github.nitouge.conduit.registry.Registries;
import io.github.nitouge.conduit.registry.Registry;
import io.github.nitouge.conduit.testkit.Await;
import io.github.nitouge.conduit.testkit.Ports;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 分布式正确性（阶段二）：<b>多个真实JVM</b> + 真实Nacos + 真实网络。
 *
 * <p>拓扑：测试进程（编排）+ 2个provider进程 + 每次用例新起的consumer进程。
 * 覆盖：注册发现、跨进程RPC、roundrobin负载均衡、节点优雅下线后的流量收敛、节点重启后的恢复。</p>
 *
 * <p>需要本机Nacos（127.0.0.1:8848）；不可用时整类跳过，不阻塞无外部依赖的回归。
 * 默认不随 {@code mvn test} 运行（要起5个JVM），显式开启：</p>
 *
 * <pre>{@code
 * mvn -pl conduit-it -am test -Dit.distributed=true
 * }</pre>
 */
@EnabledIfSystemProperty(named = "it.distributed", matches = "true")
class MultiJvmRegistryIntegrationTest {

    private static final String NACOS = "127.0.0.1:8848";
    private static final String SERVICE = NodeService.class.getName();
    private static final List<Process> CHILDREN = new CopyOnWriteArrayList<>();

    private static Registry adminRegistry;

    @BeforeAll
    static void requireNacos() {
        try {
            adminRegistry = Registries.getRegistry("nacos", NACOS);
            adminRegistry.discover("__conduit_probe__");
        } catch (Throwable t) {
            Assumptions.abort("Nacos不可用(" + NACOS + ")，跳过多JVM分布式测试: " + t);
        }
    }

    @AfterAll
    static void cleanup() {
        for (Process process : CHILDREN) {
            destroyQuietly(process);
        }
        if (adminRegistry != null) {
            adminRegistry.destroy();
        }
    }

    @Test
    void discoveryLoadBalanceAndRecoveryAcrossJvms() throws Exception {
        int portA = Ports.freePort();
        int portB = Ports.freePort();

        Process nodeA = startProvider(portA, "node-a");
        Process nodeB = startProvider(portB, "node-b");
        awaitInstances(2, 60_000, "两个provider应注册到Nacos");

        // 1) 负载均衡：roundrobin的6次调用应覆盖两个不同进程
        List<String> firstRound = runConsumer(6);
        assertEquals(6, firstRound.size(), "consumer应完成6次调用: " + firstRound);
        assertEquals(Set.of("node-a", "node-b"), new HashSet<>(firstRound),
                "roundrobin应把请求分到两个进程: " + firstRound);

        // 2) 故障收敛：node-a优雅下线（先注销）后，后续调用只能命中node-b
        stopGracefully(nodeA);
        awaitInstances(1, 20_000, "node-a下线后应只剩1个实例");

        List<String> afterLeave = runConsumer(4);
        assertEquals(Set.of("node-b"), new HashSet<>(afterLeave),
                "node-a下线后只应命中node-b: " + afterLeave);

        // 3) 故障恢复：node-a重新启动并注册，流量重新覆盖两个进程
        Process restarted = startProvider(portA, "node-a");
        awaitInstances(2, 60_000, "node-a重启后应恢复2个实例");

        List<String> recovered = runConsumer(4);
        assertTrue(recovered.contains("node-a"), "重启的node-a应重新参与分流: " + recovered);
        assertTrue(recovered.contains("node-b"), "node-b应仍在服务: " + recovered);

        destroyQuietly(restarted);
    }

    // ---------------------------------------------------------------- 进程编排

    private static Process startProvider(int port, String nodeId) throws Exception {
        Process process = launch(ProviderNodeMain.class.getName(),
                String.valueOf(port), nodeId, NACOS);
        CHILDREN.add(process);
        Await.until("provider " + nodeId + " 未在30s内监听端口 " + port, 30_000, () -> Ports.connectable(port));
        return process;
    }

    /** 起一个consumer进程并收集它打印的节点标识 */
    private static List<String> runConsumer(int calls) throws Exception {
        Process process = launch(ConsumerNodeMain.class.getName(),
                NACOS, String.valueOf(calls), "failover", "roundrobin");
        CHILDREN.add(process);

        ExecutorService pool = Executors.newSingleThreadExecutor();
        try {
            Future<List<String>> lines = pool.submit(() -> {
                List<String> collected = new ArrayList<>();
                try (BufferedReader reader = new BufferedReader(
                        new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8))) {
                    String line;
                    while ((line = reader.readLine()) != null) {
                        if (line.startsWith("CALL ")) {
                            collected.add(line);
                        }
                    }
                }
                return collected;
            });
            List<String> rawCalls = lines.get(90, TimeUnit.SECONDS);
            assertTrue(process.waitFor(10, TimeUnit.SECONDS), "consumer进程应自行退出");

            List<String> nodes = new ArrayList<>();
            for (String raw : rawCalls) {
                String[] parts = raw.split(" ");
                // CALL <i> <nodeId> 或 CALL <i> ERROR <Type>
                nodes.add(parts.length >= 3 && !"ERROR".equals(parts[2]) ? parts[2] : "ERROR");
            }
            return nodes;
        } finally {
            pool.shutdownNow();
            destroyQuietly(process);
        }
    }

    private static Process launch(String mainClass, String... args) throws Exception {
        String javaBin = Path.of(System.getProperty("java.home"), "bin",
                System.getProperty("os.name").toLowerCase().contains("win") ? "java.exe" : "java").toString();
        String[] command = new String[4 + args.length];
        command[0] = javaBin;
        command[1] = "-cp";
        command[2] = System.getProperty("java.class.path");
        command[3] = mainClass;
        System.arraycopy(args, 0, command, 4, args.length);
        return new ProcessBuilder(command).redirectErrorStream(true).start();
    }

    private static void stopGracefully(Process provider) throws Exception {
        Writer writer = new OutputStreamWriter(provider.getOutputStream(), StandardCharsets.UTF_8);
        writer.write("STOP\n");
        writer.flush();
        assertTrue(provider.waitFor(15, TimeUnit.SECONDS), "provider应优雅停机并退出");
    }

    private static void awaitInstances(int expected, long timeoutMillis, String description) {
        Await.until(description, timeoutMillis,
                () -> adminRegistry.discover(SERVICE).size() == expected);
    }

    private static void destroyQuietly(Process process) {
        if (process == null || !process.isAlive()) {
            return;
        }
        process.destroyForcibly();
        try {
            process.waitFor(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
