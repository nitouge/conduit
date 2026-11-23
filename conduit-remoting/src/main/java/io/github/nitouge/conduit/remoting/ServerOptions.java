package io.github.nitouge.conduit.remoting;

import io.github.nitouge.conduit.protocol.ProtocolOptions;
import io.netty.handler.ssl.SslContext;

/**
 * 服务端传输层配置（不可变，用 {@link #builder()} 构建）。
 *
 * <p>包含业务线程池规模、读空闲探活、握手超时、优雅停机等待时长、响应压缩阈值与可选TLS。
 * 协议版本/序列化codec/压缩算法由每个连接握手协商决定，服务端不需要预先固定。</p>
 */
public final class ServerOptions {

    /** 默认业务线程数 */
    public static final int DEFAULT_BUSINESS_THREADS = 200;
    /** 默认业务队列容量（有界队列，过载时快速拒绝而非无限堆积） */
    public static final int DEFAULT_QUEUE_CAPACITY = 10_000;
    /** 默认读空闲超时：客户端会按心跳间隔发PING，超过该时长没有任何帧即判定连接失联 */
    public static final int DEFAULT_READER_IDLE_TIMEOUT_MILLIS = 60_000;
    /** 默认握手超时：连接建立后迟迟不握手则断开，避免僵尸连接占资源 */
    public static final int DEFAULT_HANDSHAKE_TIMEOUT_MILLIS = 3_000;
    /** 默认优雅停机等待业务在途执行完成的时长 */
    public static final int DEFAULT_SHUTDOWN_TIMEOUT_MILLIS = 10_000;

    private final int businessThreads;
    private final int queueCapacity;
    private final long readerIdleTimeoutMillis;
    private final long handshakeTimeoutMillis;
    private final long shutdownTimeoutMillis;
    private final int compressThreshold;
    private final SslContext sslContext;

    private ServerOptions(Builder builder) {
        this.businessThreads = builder.businessThreads;
        this.queueCapacity = builder.queueCapacity;
        this.readerIdleTimeoutMillis = builder.readerIdleTimeoutMillis;
        this.handshakeTimeoutMillis = builder.handshakeTimeoutMillis;
        this.shutdownTimeoutMillis = builder.shutdownTimeoutMillis;
        this.compressThreshold = builder.compressThreshold;
        this.sslContext = builder.sslContext;
    }

    public static ServerOptions defaults() {
        return builder().build();
    }

    public static Builder builder() {
        return new Builder();
    }

    /** 供出站编码器使用的协议配置（阈值决定响应是否压缩；codec/compress按连接协商结果逐帧给出） */
    public ProtocolOptions protocolOptions() {
        return ProtocolOptions.builder()
                .compressThreshold(compressThreshold)
                .build();
    }

    public int getBusinessThreads() {
        return businessThreads;
    }

    public int getQueueCapacity() {
        return queueCapacity;
    }

    public long getReaderIdleTimeoutMillis() {
        return readerIdleTimeoutMillis;
    }

    public long getHandshakeTimeoutMillis() {
        return handshakeTimeoutMillis;
    }

    public long getShutdownTimeoutMillis() {
        return shutdownTimeoutMillis;
    }

    public int getCompressThreshold() {
        return compressThreshold;
    }

    public SslContext getSslContext() {
        return sslContext;
    }

    @Override
    public String toString() {
        return "ServerOptions{" +
                "businessThreads=" + businessThreads +
                ", queueCapacity=" + queueCapacity +
                ", readerIdleTimeoutMillis=" + readerIdleTimeoutMillis +
                ", handshakeTimeoutMillis=" + handshakeTimeoutMillis +
                ", shutdownTimeoutMillis=" + shutdownTimeoutMillis +
                ", compressThreshold=" + compressThreshold +
                ", tls=" + (sslContext != null) +
                '}';
    }

    /** {@link ServerOptions} 构建器 */
    public static final class Builder {

        private int businessThreads = DEFAULT_BUSINESS_THREADS;
        private int queueCapacity = DEFAULT_QUEUE_CAPACITY;
        private long readerIdleTimeoutMillis = DEFAULT_READER_IDLE_TIMEOUT_MILLIS;
        private long handshakeTimeoutMillis = DEFAULT_HANDSHAKE_TIMEOUT_MILLIS;
        private long shutdownTimeoutMillis = DEFAULT_SHUTDOWN_TIMEOUT_MILLIS;
        private int compressThreshold = ProtocolOptions.DEFAULT_COMPRESS_THRESHOLD;
        private SslContext sslContext;

        private Builder() {
        }

        public Builder businessThreads(int businessThreads) {
            this.businessThreads = businessThreads;
            return this;
        }

        public Builder queueCapacity(int queueCapacity) {
            this.queueCapacity = queueCapacity;
            return this;
        }

        /** 读空闲超时（毫秒）；0表示关闭 */
        public Builder readerIdleTimeoutMillis(long readerIdleTimeoutMillis) {
            this.readerIdleTimeoutMillis = readerIdleTimeoutMillis;
            return this;
        }

        public Builder handshakeTimeoutMillis(long handshakeTimeoutMillis) {
            this.handshakeTimeoutMillis = handshakeTimeoutMillis;
            return this;
        }

        public Builder shutdownTimeoutMillis(long shutdownTimeoutMillis) {
            this.shutdownTimeoutMillis = shutdownTimeoutMillis;
            return this;
        }

        /** 响应压缩阈值（字节）；0表示只要对端协商了压缩就无条件压缩 */
        public Builder compressThreshold(int compressThreshold) {
            this.compressThreshold = compressThreshold;
            return this;
        }

        /** 非null即对全部入站连接启用TLS（可用 {@code Tls} 工具类从证书构建） */
        public Builder sslContext(SslContext sslContext) {
            this.sslContext = sslContext;
            return this;
        }

        public ServerOptions build() {
            if (businessThreads < 1) {
                throw new IllegalArgumentException("businessThreads必须>=1: " + businessThreads);
            }
            if (queueCapacity < 1) {
                throw new IllegalArgumentException("queueCapacity必须>=1: " + queueCapacity);
            }
            if (handshakeTimeoutMillis <= 0) {
                throw new IllegalArgumentException("handshakeTimeoutMillis必须为正数: " + handshakeTimeoutMillis);
            }
            if (readerIdleTimeoutMillis < 0) {
                throw new IllegalArgumentException("readerIdleTimeoutMillis不能为负数: " + readerIdleTimeoutMillis);
            }
            if (shutdownTimeoutMillis <= 0) {
                throw new IllegalArgumentException("shutdownTimeoutMillis必须为正数: " + shutdownTimeoutMillis);
            }
            if (compressThreshold < 0) {
                throw new IllegalArgumentException("compressThreshold不能为负数: " + compressThreshold);
            }
            return new ServerOptions(this);
        }
    }
}
