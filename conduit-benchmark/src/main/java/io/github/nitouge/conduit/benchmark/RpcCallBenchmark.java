package io.github.nitouge.conduit.benchmark;

import io.github.nitouge.conduit.client.RpcClient;
import io.github.nitouge.conduit.client.proxy.RpcClientProxy;
import io.github.nitouge.conduit.registry.Registries;
import io.github.nitouge.conduit.registry.Registry;
import io.github.nitouge.conduit.server.RpcServer;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.util.concurrent.TimeUnit;

/**
 * 端到端RPC基准（对应BACKLOG "QPS吞吐量基准测试" 与 "延迟分布基准测试"）。
 *
 * <p>同一个JVM内起真实 {@link RpcServer}（127.0.0.1 + 随机端口），经 {@link RpcClientProxy}
 * 走完整的 注册中心发现 → 负载均衡 → 集群容错 → 连接池 → 握手 → 协议编解码 → 服务端业务线程池
 * 调用链。测的是框架整体开销，而不是某个组件。</p>
 *
 * <p>两个视角：</p>
 * <ul>
 *   <li>{@link #callLatency()} —— {@link Mode#SampleTime}，JMH会输出
 *       p0/p50/p90/p95/p99/p99.9/p100分位，即延迟分布；</li>
 *   <li>{@link #callThroughput()} —— {@link Mode#Throughput} + 4线程并发，
 *       观察单连接多路复用下的QPS上限。</li>
 * </ul>
 *
 * <p>setup阶段先做一次warmup调用，把建链/握手/订阅的开销排除在测量之外。</p>
 */
@BenchmarkMode(Mode.SampleTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(1)
@State(Scope.Benchmark)
public class RpcCallBenchmark {

    /** 客户端序列化：jdk（默认）与kryo（生产推荐）对比 */
    @Param({"jdk", "kryo"})
    public String serializer;

    private Registry registry;
    private RpcServer server;
    private RpcClient client;
    private BenchService service;

    @Setup(Level.Trial)
    public void setUp() throws IOException {
        registry = Registries.getRegistry("local", "benchmark");
        int port = freePort();

        server = new RpcServer("127.0.0.1", port, registry);
        server.export(BenchService.class, new BenchServiceImpl());
        server.start();

        client = new RpcClient(serializer);
        service = RpcClientProxy.create(BenchService.class, registry, client, 3000);

        String warmup = service.echo("warmup");
        System.out.printf("[bench] serializer=%s port=%d warmup=%s%n", serializer, port, warmup);
    }

    @TearDown(Level.Trial)
    public void tearDown() {
        if (client != null) {
            client.close();
        }
        if (server != null) {
            server.close();
        }
        if (registry != null) {
            registry.destroy();
        }
    }

    /** 单调用延迟（JMH输出分位，即延迟分布） */
    @Benchmark
    public String callLatency() {
        return service.echo("hello");
    }

    /** 4线程并发吞吐（单连接多路复用） */
    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    @OutputTimeUnit(TimeUnit.SECONDS)
    @Threads(4)
    public String callThroughput() {
        return service.echo("hello");
    }

    /** 取一个空闲端口（ServerSocket关闭后由RpcServer绑定，存在极小的抢占窗口） */
    private static int freePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0, 1, InetAddress.getLoopbackAddress())) {
            return socket.getLocalPort();
        }
    }
}
