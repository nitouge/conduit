package io.github.nitouge.conduit.benchmark;

import io.github.nitouge.conduit.api.RpcRequest;
import io.github.nitouge.conduit.serialization.Serializer;
import io.github.nitouge.conduit.serialization.SerializerFactory;
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
import org.openjdk.jmh.annotations.Warmup;

import java.util.concurrent.TimeUnit;

/**
 * 序列化性能基准（对应BACKLOG "序列化性能基准测试"）。
 *
 * <p>对同一个 {@link RpcRequest}（small/large两种报文规模）分别测四种序列化实现的
 * 序列化/反序列化吞吐（ops/s）。走的是框架真实入口 {@link SerializerFactory}，
 * 因此也覆盖了SPI查找后取到实例的真实路径。</p>
 *
 * <p>观测点：</p>
 * <ul>
 *   <li>setup阶段打印每种组合的编码后字节数，直接体现"体积"差异；</li>
 *   <li>JSON/Protostuff与JDK/Kryo的量级差异，用于验证"生产推荐kryo"的结论。</li>
 * </ul>
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(1)
@State(Scope.Thread)
public class SerializationBenchmark {

    /** 报文规模：small≈百字节、large≈数千字节 */
    @Param({"small", "large"})
    public String payloadSize;

    /** 四种内置序列化实现 */
    @Param({"jdk", "json", "kryo", "protostuff"})
    public String serializerName;

    private Serializer serializer;
    private RpcRequest request;
    private byte[] encoded;

    @Setup(Level.Trial)
    public void setUp() {
        serializer = SerializerFactory.getByName(serializerName);
        request = BenchData.request(payloadSize);
        encoded = serializer.serialize(request);
        System.out.printf("[bench] serializer=%-10s payload=%-5s encodedBytes=%d%n",
                serializerName, payloadSize, encoded.length);
    }

    @Benchmark
    public byte[] serialize() {
        return serializer.serialize(request);
    }

    @Benchmark
    public RpcRequest deserialize() {
        return serializer.deserialize(encoded, RpcRequest.class);
    }
}
