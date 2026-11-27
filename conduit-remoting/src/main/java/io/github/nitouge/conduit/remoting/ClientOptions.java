package io.github.nitouge.conduit.remoting;

import io.github.nitouge.conduit.protocol.ProtocolConstants;
import io.github.nitouge.conduit.protocol.ProtocolOptions;
import io.github.nitouge.conduit.protocol.codec.Compressor;
import io.netty.handler.ssl.SslContext;

/**
 * 客户端传输层配置（不可变，用 {@link #builder()} 构建）。
 *
 * <p>集中管理一条RPC连接生命周期上的所有可控参数：</p>
 * <ul>
 *   <li><b>协议</b>：默认serialization codec、压缩算法与压缩阈值；</li>
 *   <li><b>建链</b>：TCP connect超时、握手超时；</li>
 *   <li><b>保活</b>：心跳间隔、读空闲超时（超时判定对端失联并断开）；</li>
 *   <li><b>连接池</b>：每个地址的最大连接数（默认1，即长连接复用）、闲置连接回收时长；</li>
 *   <li><b>背压</b>：每连接在途请求上限、写缓冲高低水位（超限快速失败而不是无限堆积）；</li>
 *   <li><b>重连</b>：断线后是否后台自动重连、最大重试次数与指数退避区间；</li>
 *   <li><b>加密</b>：可选的 {@link SslContext}，非null即对该客户端的全部连接启用TLS。</li>
 * </ul>
 *
 * <p>所有超时/间隔单位为毫秒；心跳间隔为0表示关闭心跳，读空闲为0表示不做读空闲探活。</p>
 */
public final class ClientOptions {

    /** 默认TCP建链超时 */
    public static final int DEFAULT_CONNECT_TIMEOUT_MILLIS = 3_000;
    /** 默认协议握手超时 */
    public static final int DEFAULT_HANDSHAKE_TIMEOUT_MILLIS = 3_000;
    /** 默认心跳间隔（写空闲多久发一次PING） */
    public static final int DEFAULT_HEARTBEAT_INTERVAL_MILLIS = 30_000;
    /** 默认读空闲超时（期间没收到任何帧则判定对端失联并断开），应大于心跳间隔 */
    public static final int DEFAULT_READ_IDLE_TIMEOUT_MILLIS = 90_000;
    /** 默认每地址最大连接数：1条长连接复用 */
    public static final int DEFAULT_MAX_CONNECTIONS_PER_ADDRESS = 1;
    /** 默认闲置连接回收时长（无在途请求且超过该时长未使用即关闭） */
    public static final int DEFAULT_IDLE_CONNECTION_TIMEOUT_MILLIS = 600_000;
    /** 默认断线重连最大尝试次数 */
    public static final int DEFAULT_RECONNECT_MAX_ATTEMPTS = 5;
    /** 默认重连首次退避时长 */
    public static final int DEFAULT_RECONNECT_BASE_DELAY_MILLIS = 1_000;
    /** 默认重连最大退避时长 */
    public static final int DEFAULT_RECONNECT_MAX_DELAY_MILLIS = 30_000;
    /** 默认每连接在途请求上限：超过即快速失败，防止服务端变慢时客户端无限堆积 */
    public static final int DEFAULT_MAX_IN_FLIGHT_PER_CONNECTION = 1024;
    /** 默认写缓冲高水位（字节）：超过后channel不再writable，客户端开始拒绝新请求 */
    public static final int DEFAULT_WRITE_BUFFER_HIGH_WATER_MARK = 64 * 1024;
    /** 默认写缓冲低水位（字节）：回落到该值以下后channel恢复writable */
    public static final int DEFAULT_WRITE_BUFFER_LOW_WATER_MARK = 32 * 1024;

    private final byte codec;
    private final byte compress;
    private final int compressThreshold;
    private final long connectTimeoutMillis;
    private final long handshakeTimeoutMillis;
    private final long heartbeatIntervalMillis;
    private final long readIdleTimeoutMillis;
    private final int maxConnectionsPerAddress;
    private final long idleConnectionTimeoutMillis;
    private final boolean reconnectEnabled;
    private final int reconnectMaxAttempts;
    private final long reconnectBaseDelayMillis;
    private final long reconnectMaxDelayMillis;
    private final int maxInFlightPerConnection;
    private final int writeBufferHighWaterMark;
    private final int writeBufferLowWaterMark;
    private final SslContext sslContext;

    private ClientOptions(Builder builder) {
        this.codec = builder.codec;
        this.compress = builder.compress;
        this.compressThreshold = builder.compressThreshold;
        this.connectTimeoutMillis = builder.connectTimeoutMillis;
        this.handshakeTimeoutMillis = builder.handshakeTimeoutMillis;
        this.heartbeatIntervalMillis = builder.heartbeatIntervalMillis;
        this.readIdleTimeoutMillis = builder.readIdleTimeoutMillis;
        this.maxConnectionsPerAddress = builder.maxConnectionsPerAddress;
        this.idleConnectionTimeoutMillis = builder.idleConnectionTimeoutMillis;
        this.reconnectEnabled = builder.reconnectEnabled;
        this.reconnectMaxAttempts = builder.reconnectMaxAttempts;
        this.reconnectBaseDelayMillis = builder.reconnectBaseDelayMillis;
        this.reconnectMaxDelayMillis = builder.reconnectMaxDelayMillis;
        this.maxInFlightPerConnection = builder.maxInFlightPerConnection;
        this.writeBufferHighWaterMark = builder.writeBufferHighWaterMark;
        this.writeBufferLowWaterMark = builder.writeBufferLowWaterMark;
        this.sslContext = builder.sslContext;
    }

    public static ClientOptions defaults() {
        return builder().build();
    }

    /** 只指定默认序列化codec，其余取默认值 */
    public static ClientOptions of(byte codec) {
        return builder().codec(codec).build();
    }

    public static Builder builder() {
        return new Builder();
    }

    /** 供编解码器使用的协议配置（codec/compress/threshold） */
    public ProtocolOptions protocolOptions() {
        return ProtocolOptions.builder()
                .codec(codec)
                .compress(compress)
                .compressThreshold(compressThreshold)
                .build();
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

    public long getConnectTimeoutMillis() {
        return connectTimeoutMillis;
    }

    public long getHandshakeTimeoutMillis() {
        return handshakeTimeoutMillis;
    }

    public long getHeartbeatIntervalMillis() {
        return heartbeatIntervalMillis;
    }

    public long getReadIdleTimeoutMillis() {
        return readIdleTimeoutMillis;
    }

    public int getMaxConnectionsPerAddress() {
        return maxConnectionsPerAddress;
    }

    public long getIdleConnectionTimeoutMillis() {
        return idleConnectionTimeoutMillis;
    }

    public boolean isReconnectEnabled() {
        return reconnectEnabled;
    }

    public int getReconnectMaxAttempts() {
        return reconnectMaxAttempts;
    }

    public long getReconnectBaseDelayMillis() {
        return reconnectBaseDelayMillis;
    }

    public long getReconnectMaxDelayMillis() {
        return reconnectMaxDelayMillis;
    }

    /** 每连接允许的在途请求数上限（超过即快速失败） */
    public int getMaxInFlightPerConnection() {
        return maxInFlightPerConnection;
    }

    /** 写缓冲高水位（字节）：channel 不可写后客户端拒绝新请求 */
    public int getWriteBufferHighWaterMark() {
        return writeBufferHighWaterMark;
    }

    /** 写缓冲低水位（字节）：回落到该值以下后 channel 恢复可写 */
    public int getWriteBufferLowWaterMark() {
        return writeBufferLowWaterMark;
    }

    public SslContext getSslContext() {
        return sslContext;
    }

    @Override
    public String toString() {
        return "ClientOptions{" +
                "codec=" + codec +
                ", compress=" + compress +
                ", compressThreshold=" + compressThreshold +
                ", connectTimeoutMillis=" + connectTimeoutMillis +
                ", handshakeTimeoutMillis=" + handshakeTimeoutMillis +
                ", heartbeatIntervalMillis=" + heartbeatIntervalMillis +
                ", readIdleTimeoutMillis=" + readIdleTimeoutMillis +
                ", maxConnectionsPerAddress=" + maxConnectionsPerAddress +
                ", idleConnectionTimeoutMillis=" + idleConnectionTimeoutMillis +
                ", reconnectEnabled=" + reconnectEnabled +
                ", reconnectMaxAttempts=" + reconnectMaxAttempts +
                ", reconnectBaseDelayMillis=" + reconnectBaseDelayMillis +
                ", reconnectMaxDelayMillis=" + reconnectMaxDelayMillis +
                ", maxInFlightPerConnection=" + maxInFlightPerConnection +
                ", writeBufferHighWaterMark=" + writeBufferHighWaterMark +
                ", writeBufferLowWaterMark=" + writeBufferLowWaterMark +
                ", tls=" + (sslContext != null) +
                '}';
    }

    /** {@link ClientOptions} 构建器 */
    public static final class Builder {

        private byte codec = ProtocolConstants.CODEC_JDK;
        private byte compress = ProtocolConstants.COMPRESS_NONE;
        private int compressThreshold = ProtocolOptions.DEFAULT_COMPRESS_THRESHOLD;
        private long connectTimeoutMillis = DEFAULT_CONNECT_TIMEOUT_MILLIS;
        private long handshakeTimeoutMillis = DEFAULT_HANDSHAKE_TIMEOUT_MILLIS;
        private long heartbeatIntervalMillis = DEFAULT_HEARTBEAT_INTERVAL_MILLIS;
        private long readIdleTimeoutMillis = DEFAULT_READ_IDLE_TIMEOUT_MILLIS;
        private int maxConnectionsPerAddress = DEFAULT_MAX_CONNECTIONS_PER_ADDRESS;
        private long idleConnectionTimeoutMillis = DEFAULT_IDLE_CONNECTION_TIMEOUT_MILLIS;
        private boolean reconnectEnabled = true;
        private int reconnectMaxAttempts = DEFAULT_RECONNECT_MAX_ATTEMPTS;
        private long reconnectBaseDelayMillis = DEFAULT_RECONNECT_BASE_DELAY_MILLIS;
        private long reconnectMaxDelayMillis = DEFAULT_RECONNECT_MAX_DELAY_MILLIS;
        private int maxInFlightPerConnection = DEFAULT_MAX_IN_FLIGHT_PER_CONNECTION;
        private int writeBufferHighWaterMark = DEFAULT_WRITE_BUFFER_HIGH_WATER_MARK;
        private int writeBufferLowWaterMark = DEFAULT_WRITE_BUFFER_LOW_WATER_MARK;
        private SslContext sslContext;

        private Builder() {
        }

        /** 默认序列化codec（见 {@link ProtocolConstants#CODEC_JDK} 等常量） */
        public Builder codec(byte codec) {
            this.codec = codec;
            return this;
        }

        /** 期望使用的压缩算法；服务端不支持时握手下调为NONE */
        public Builder compress(byte compress) {
            if (!Compressor.isSupported(compress)) {
                throw new IllegalArgumentException("不支持的压缩算法: " + compress
                        + "（可选: COMPRESS_NONE, COMPRESS_GZIP）");
            }
            this.compress = compress;
            return this;
        }

        /** 压缩阈值（字节）；0表示无条件压缩 */
        public Builder compressThreshold(int compressThreshold) {
            this.compressThreshold = compressThreshold;
            return this;
        }

        public Builder connectTimeoutMillis(long connectTimeoutMillis) {
            this.connectTimeoutMillis = connectTimeoutMillis;
            return this;
        }

        public Builder handshakeTimeoutMillis(long handshakeTimeoutMillis) {
            this.handshakeTimeoutMillis = handshakeTimeoutMillis;
            return this;
        }

        /** 心跳间隔（毫秒）；0表示关闭心跳 */
        public Builder heartbeatIntervalMillis(long heartbeatIntervalMillis) {
            this.heartbeatIntervalMillis = heartbeatIntervalMillis;
            return this;
        }

        /** 读空闲超时（毫秒）：期间收不到任何帧即判定对端失联；0表示关闭探活 */
        public Builder readIdleTimeoutMillis(long readIdleTimeoutMillis) {
            this.readIdleTimeoutMillis = readIdleTimeoutMillis;
            return this;
        }

        /** 每个地址最多维持的连接数（连接池容量） */
        public Builder maxConnectionsPerAddress(int maxConnectionsPerAddress) {
            this.maxConnectionsPerAddress = maxConnectionsPerAddress;
            return this;
        }

        /** 闲置连接回收时长（毫秒） */
        public Builder idleConnectionTimeoutMillis(long idleConnectionTimeoutMillis) {
            this.idleConnectionTimeoutMillis = idleConnectionTimeoutMillis;
            return this;
        }

        public Builder reconnectEnabled(boolean reconnectEnabled) {
            this.reconnectEnabled = reconnectEnabled;
            return this;
        }

        public Builder reconnectMaxAttempts(int reconnectMaxAttempts) {
            this.reconnectMaxAttempts = reconnectMaxAttempts;
            return this;
        }

        public Builder reconnectBaseDelayMillis(long reconnectBaseDelayMillis) {
            this.reconnectBaseDelayMillis = reconnectBaseDelayMillis;
            return this;
        }

        public Builder reconnectMaxDelayMillis(long reconnectMaxDelayMillis) {
            this.reconnectMaxDelayMillis = reconnectMaxDelayMillis;
            return this;
        }

        /** 非null即启用TLS（可用 {@code Tls} 工具类从证书构建） */
        public Builder sslContext(SslContext sslContext) {
            this.sslContext = sslContext;
            return this;
        }

        /**
         * 每连接允许的在途请求数上限：达到上限后新请求<b>快速失败</b>而不是继续堆积。
         *
         * <p>这是客户端侧的背压阀：服务端变慢时，请求会先堆在写缓冲、再堆在在途表，最后把客户端
         * 自己的内存打满。提前拒绝比 OOM/超时雪崩好得多。</p>
         */
        public Builder maxInFlightPerConnection(int maxInFlightPerConnection) {
            this.maxInFlightPerConnection = maxInFlightPerConnection;
            return this;
        }

        /**
         * 写缓冲高/低水位（字节）：出站缓冲超过高水位后 channel 变为不可写，客户端开始拒绝新请求；
         * 回落到低水位以下恢复可写。
         */
        public Builder writeBufferWaterMark(int lowWaterMark, int highWaterMark) {
            this.writeBufferLowWaterMark = lowWaterMark;
            this.writeBufferHighWaterMark = highWaterMark;
            return this;
        }

        public ClientOptions build() {
            if (connectTimeoutMillis <= 0) {
                throw new IllegalArgumentException("connectTimeoutMillis必须为正数: " + connectTimeoutMillis);
            }
            if (handshakeTimeoutMillis <= 0) {
                throw new IllegalArgumentException("handshakeTimeoutMillis必须为正数: " + handshakeTimeoutMillis);
            }
            if (heartbeatIntervalMillis < 0) {
                throw new IllegalArgumentException("heartbeatIntervalMillis不能为负数: " + heartbeatIntervalMillis);
            }
            if (readIdleTimeoutMillis < 0) {
                throw new IllegalArgumentException("readIdleTimeoutMillis不能为负数: " + readIdleTimeoutMillis);
            }
            if (heartbeatIntervalMillis > 0 && readIdleTimeoutMillis > 0
                    && readIdleTimeoutMillis <= heartbeatIntervalMillis) {
                throw new IllegalArgumentException("readIdleTimeoutMillis(" + readIdleTimeoutMillis
                        + ")必须大于heartbeatIntervalMillis(" + heartbeatIntervalMillis
                        + ")，否则连接会在正常心跳间隙被判失联");
            }
            if (maxConnectionsPerAddress < 1) {
                throw new IllegalArgumentException("maxConnectionsPerAddress必须>=1: " + maxConnectionsPerAddress);
            }
            if (idleConnectionTimeoutMillis < 0) {
                throw new IllegalArgumentException("idleConnectionTimeoutMillis不能为负数: " + idleConnectionTimeoutMillis);
            }
            if (reconnectMaxAttempts < 0) {
                throw new IllegalArgumentException("reconnectMaxAttempts不能为负数: " + reconnectMaxAttempts);
            }
            if (reconnectBaseDelayMillis < 0 || reconnectMaxDelayMillis < 0) {
                throw new IllegalArgumentException("重连退避时长不能为负数");
            }
            if (reconnectMaxDelayMillis < reconnectBaseDelayMillis) {
                throw new IllegalArgumentException("reconnectMaxDelayMillis(" + reconnectMaxDelayMillis
                        + ")不能小于reconnectBaseDelayMillis(" + reconnectBaseDelayMillis + ")");
            }
            if (compressThreshold < 0) {
                throw new IllegalArgumentException("compressThreshold不能为负数: " + compressThreshold);
            }
            if (maxInFlightPerConnection < 1) {
                throw new IllegalArgumentException("maxInFlightPerConnection必须>=1: " + maxInFlightPerConnection);
            }
            if (writeBufferLowWaterMark <= 0 || writeBufferHighWaterMark <= 0) {
                throw new IllegalArgumentException("写缓冲水位必须为正数: low=" + writeBufferLowWaterMark
                        + ", high=" + writeBufferHighWaterMark);
            }
            if (writeBufferHighWaterMark <= writeBufferLowWaterMark) {
                throw new IllegalArgumentException("writeBufferHighWaterMark(" + writeBufferHighWaterMark
                        + ")必须大于lowWaterMark(" + writeBufferLowWaterMark + ")，否则channel会永远不可写");
            }
            return new ClientOptions(this);
        }
    }
}
