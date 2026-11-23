package io.github.nitouge.conduit.protocol;

import io.github.nitouge.conduit.protocol.codec.Compressor;

/**
 * 协议层可配置项：业务帧默认使用的序列化codec、压缩算法与压缩阈值。
 *
 * <p>编解码器只依赖本配置与 {@code Serializer} 接口，不出现任何具体序列化实现——换序列化只改
 * codec取值与SPI配置，编解码代码一行不动。</p>
 *
 * <p>压缩阈值是性能与流量的折中：小报文（如一个短字符串）gzip 后的体积往往比原文更大，
 * 且多一次CPU开销，因此默认只对 {@value #DEFAULT_COMPRESS_THRESHOLD} 字节及以上的body压缩。
 * 阈值为0表示只要配置了压缩就无条件压缩，便于压测对比。</p>
 */
public final class ProtocolOptions {

    /** 默认压缩阈值（字节）：body达到该大小才真正压缩 */
    public static final int DEFAULT_COMPRESS_THRESHOLD = 1024;

    private final byte codec;
    private final byte compress;
    private final int compressThreshold;

    private ProtocolOptions(Builder builder) {
        this.codec = builder.codec;
        this.compress = builder.compress;
        this.compressThreshold = builder.compressThreshold;
    }

    public static ProtocolOptions defaults() {
        return builder().build();
    }

    /** 只指定codec，其余取默认值 */
    public static ProtocolOptions of(byte codec) {
        return builder().codec(codec).build();
    }

    public static Builder builder() {
        return new Builder();
    }

    public Builder toBuilder() {
        return new Builder()
                .codec(codec)
                .compress(compress)
                .compressThreshold(compressThreshold);
    }

    public byte getCodec() {
        return codec;
    }

    public byte getCompress() {
        return compress;
    }

    public int getCompressThreshold() {
        return compressThreshold;
    }

    /**
     * 该body是否应真正压缩：配置了压缩且body长度达到阈值。
     *
     * @param compress   期望的压缩算法（通常来自协议头/连接协商结果）
     * @param bodyLength 序列化后的原始body长度
     */
    public boolean shouldCompress(byte compress, int bodyLength) {
        return compress != ProtocolConstants.COMPRESS_NONE && bodyLength >= compressThreshold;
    }

    @Override
    public String toString() {
        return "ProtocolOptions{" +
                "codec=" + codec +
                ", compress=" + compress +
                ", compressThreshold=" + compressThreshold +
                '}';
    }

    /** {@link ProtocolOptions} 构建器 */
    public static final class Builder {

        private byte codec = ProtocolConstants.CODEC_JDK;
        private byte compress = ProtocolConstants.COMPRESS_NONE;
        private int compressThreshold = DEFAULT_COMPRESS_THRESHOLD;

        private Builder() {
        }

        /** 默认序列化codec（见 {@link ProtocolConstants#CODEC_JDK} 等常量） */
        public Builder codec(byte codec) {
            this.codec = codec;
            return this;
        }

        /** 默认压缩算法（{@link ProtocolConstants#COMPRESS_NONE}/{@link ProtocolConstants#COMPRESS_GZIP}） */
        public Builder compress(byte compress) {
            if (!Compressor.isSupported(compress)) {
                throw new IllegalArgumentException("不支持的压缩算法: " + compress
                        + "（可选: COMPRESS_NONE=" + ProtocolConstants.COMPRESS_NONE
                        + ", COMPRESS_GZIP=" + ProtocolConstants.COMPRESS_GZIP + "）");
            }
            this.compress = compress;
            return this;
        }

        /** 压缩阈值（字节）；0表示无条件压缩 */
        public Builder compressThreshold(int compressThreshold) {
            if (compressThreshold < 0) {
                throw new IllegalArgumentException("compressThreshold不能为负数: " + compressThreshold);
            }
            this.compressThreshold = compressThreshold;
            return this;
        }

        public ProtocolOptions build() {
            return new ProtocolOptions(this);
        }
    }
}
