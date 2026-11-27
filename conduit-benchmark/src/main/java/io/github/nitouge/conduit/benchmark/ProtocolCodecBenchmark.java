package io.github.nitouge.conduit.benchmark;

import io.github.nitouge.conduit.protocol.ProtocolConstants;
import io.github.nitouge.conduit.protocol.ProtocolOptions;
import io.github.nitouge.conduit.protocol.RpcMessage;
import io.github.nitouge.conduit.protocol.codec.RpcDecoder;
import io.github.nitouge.conduit.protocol.codec.RpcEncoder;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
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
import org.openjdk.jmh.infra.Blackhole;

import java.util.concurrent.TimeUnit;

/**
 * 协议编解码基准：一帧 {@code RpcMessage} 经 {@link RpcEncoder}/{@link RpcDecoder}
 * 编成字节 / 从字节还原的吞吐（ops/s），含"不压缩 vs gzip"。
 *
 * <p>用Netty的 {@link EmbeddedChannel} 驱动真实编解码器（不经过网络），
 * 因此测的是"协议头 + 序列化 + 压缩"这段纯CPU路径；压缩阈值设为0，
 * 保证gzip组确实发生压缩（便于观察压缩的CPU代价与体积收益）。</p>
 *
 * <p>解码基准每次会有一段帧缓冲的合并拷贝（Netty解码器持有累积缓冲所致），
 * 属测量框架自身开销，看相对量级/压缩对比即可。</p>
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(1)
@State(Scope.Thread)
public class ProtocolCodecBenchmark {

    /** 压缩算法：none / gzip */
    @Param({"none", "gzip"})
    public String compress;

    private RpcMessage message;
    private EmbeddedChannel encoder;
    private EmbeddedChannel decoder;
    private byte[] frame;

    @Setup(Level.Trial)
    public void setUp() {
        byte compressId = "gzip".equalsIgnoreCase(compress)
                ? ProtocolConstants.COMPRESS_GZIP : ProtocolConstants.COMPRESS_NONE;
        ProtocolOptions options = ProtocolOptions.builder()
                .codec(ProtocolConstants.CODEC_KRYO)
                .compress(compressId)
                .compressThreshold(0)
                .build();

        message = RpcMessage.request(1L, ProtocolConstants.CODEC_KRYO, BenchData.request("large"));
        message.setCompress(compressId);

        // 先用编码器产出一帧字节，供解码基准复用
        encoder = new EmbeddedChannel(new RpcEncoder(options));
        encoder.writeOutbound(message);
        ByteBuf out = encoder.readOutbound();
        frame = new byte[out.readableBytes()];
        out.getBytes(out.readerIndex(), frame);
        out.release();

        decoder = new EmbeddedChannel(new RpcDecoder());
        System.out.printf("[bench] compress=%-5s frameBytes=%d%n", compress, frame.length);
    }

    @Benchmark
    public void encode(Blackhole bh) {
        encoder.writeOutbound(message);
        ByteBuf out = encoder.readOutbound();
        bh.consume(out.readableBytes());
        out.release();
    }

    @Benchmark
    public void decode(Blackhole bh) {
        decoder.writeInbound(Unpooled.wrappedBuffer(frame));
        bh.consume(decoder.readInbound());
    }
}
