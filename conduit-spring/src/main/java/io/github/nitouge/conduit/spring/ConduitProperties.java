package io.github.nitouge.conduit.spring;

import io.github.nitouge.conduit.cluster.CircuitBreakerOptions;
import io.github.nitouge.conduit.cluster.ClusterOptions;
import io.github.nitouge.conduit.protocol.ProtocolConstants;
import io.github.nitouge.conduit.remoting.ClientOptions;
import io.github.nitouge.conduit.remoting.ServerOptions;
import io.github.nitouge.conduit.remoting.tls.Tls;
import io.github.nitouge.conduit.serialization.SerializerType;
import io.netty.handler.ssl.SslContext;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.core.io.ClassPathResource;
import org.springframework.util.StringUtils;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * Conduit配置项，绑定application.yml中 {@code conduit.*}。
 *
 * <pre>
 * conduit:
 *   application:
 *     name: greeting-provider
 *   registry:
 *     type: nacos   # nacos/local
 *     address: 127.0.0.1:8848
 *   protocol:
 *     host: 127.0.0.1
 *     port: 20880
 *     serialization: jdk     # jdk/json/kryo/protostuff
 *     compress: none         # none/gzip
 *     compress-threshold: 1024
 *   transport:
 *     connect-timeout-ms: 3000
 *     handshake-timeout-ms: 3000
 *     heartbeat-interval-ms: 30000
 *     read-idle-timeout-ms: 90000
 *     max-connections-per-address: 1
 *     idle-connection-timeout-ms: 600000
 *     reconnect:
 *       enabled: true
 *       max-attempts: 5
 *       base-delay-ms: 1000
 *       max-delay-ms: 30000
 *     server:
 *       business-threads: 200
 *       queue-capacity: 10000
 *       reader-idle-timeout-ms: 60000
 *       shutdown-timeout-ms: 10000
 *     tls:
 *       enabled: false
 *       cert: ""             # 服务端证书链PEM（支持classpath:/path）
 *       key: ""              # 服务端私钥PEM
 *       key-password: ""
 *       trust-cert: ""       # 客户端信任的CA/证书PEM；留空用JVM默认信任库
 *       insecure: false      # 客户端跳过证书校验（仅测试）
 *   cluster:
 *     retries: 2             # failover额外重试次数（总尝试=retries+1）
 *     retry-backoff-ms: 0    # 重试退避
 *     breaker:
 *       failure-threshold: 5 # 连续失败多少次熔断
 *       open-duration-ms: 5000
 * </pre>
 */
@ConfigurationProperties(prefix = "conduit")
public class ConduitProperties {

    /** 是否启用Conduit自动装配（默认true，设false可关闭整个starter） */
    private boolean enabled = true;

    private final ApplicationConfig application = new ApplicationConfig();
    private final RegistryConfig registry = new RegistryConfig();
    private final ProtocolConfig protocol = new ProtocolConfig();
    private final TransportConfig transport = new TransportConfig();
    private final ClusterConfig cluster = new ClusterConfig();

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public ApplicationConfig getApplication() {
        return application;
    }

    public RegistryConfig getRegistry() {
        return registry;
    }

    public ProtocolConfig getProtocol() {
        return protocol;
    }

    public TransportConfig getTransport() {
        return transport;
    }

    public ClusterConfig getCluster() {
        return cluster;
    }

    // ---------------------------------------------------------------- 构建传输选项

    /** 依据 {@code conduit.*} 构建客户端传输配置（序列化/压缩/连接池/心跳/重连/TLS） */
    public ClientOptions toClientOptions() {
        TransportConfig t = transport;
        ReconnectConfig reconnect = t.getReconnect();
        ClientOptions.Builder builder = ClientOptions.builder()
                .codec(SerializerType.idOf(protocol.getSerialization()))
                .compress(compressId(protocol.getCompress()))
                .compressThreshold(protocol.getCompressThreshold())
                .connectTimeoutMillis(t.getConnectTimeoutMs())
                .handshakeTimeoutMillis(t.getHandshakeTimeoutMs())
                .heartbeatIntervalMillis(t.getHeartbeatIntervalMs())
                .readIdleTimeoutMillis(t.getReadIdleTimeoutMs())
                .maxConnectionsPerAddress(t.getMaxConnectionsPerAddress())
                .idleConnectionTimeoutMillis(t.getIdleConnectionTimeoutMs())
                .reconnectEnabled(reconnect.isEnabled())
                .reconnectMaxAttempts(reconnect.getMaxAttempts())
                .reconnectBaseDelayMillis(reconnect.getBaseDelayMs())
                .reconnectMaxDelayMillis(reconnect.getMaxDelayMs());
        SslContext sslContext = buildClientSslContext();
        if (sslContext != null) {
            builder.sslContext(sslContext);
        }
        return builder.build();
    }

    /** 依据 {@code conduit.*} 构建服务端传输配置（业务线程池/读空闲/握手超时/TLS） */
    public ServerOptions toServerOptions() {
        ServerConfig server = transport.getServer();
        ServerOptions.Builder builder = ServerOptions.builder()
                .businessThreads(server.getBusinessThreads())
                .queueCapacity(server.getQueueCapacity())
                .readerIdleTimeoutMillis(server.getReaderIdleTimeoutMs())
                .handshakeTimeoutMillis(transport.getHandshakeTimeoutMs())
                .shutdownTimeoutMillis(server.getShutdownTimeoutMs())
                .compressThreshold(protocol.getCompressThreshold());
        SslContext sslContext = buildServerSslContext();
        if (sslContext != null) {
            builder.sslContext(sslContext);
        }
        return builder.build();
    }

    /** 依据 {@code conduit.cluster.*} 构建重试/退避/熔断配置 */
    public ClusterOptions toClusterOptions() {
        return ClusterOptions.builder()
                .retries(cluster.getRetries())
                .retryBackoffMs(cluster.getRetryBackoffMs())
                .circuitBreaker(CircuitBreakerOptions.builder()
                        .failureThreshold(cluster.getBreaker().getFailureThreshold())
                        .openDurationMs(cluster.getBreaker().getOpenDurationMs())
                        .build())
                .build();
    }

    private SslContext buildClientSslContext() {
        TlsConfig tls = transport.getTls();
        if (!tls.isEnabled()) {
            return null;
        }
        try {
            if (tls.isInsecure()) {
                return Tls.insecureClient();
            }
            if (!StringUtils.hasText(tls.getTrustCert())) {
                return Tls.defaultClient();
            }
            try (InputStream trust = openResource(tls.getTrustCert())) {
                return Tls.client(trust);
            }
        } catch (IOException e) {
            throw new IllegalStateException("初始化客户端TLS失败: " + e.getMessage(), e);
        }
    }

    private SslContext buildServerSslContext() {
        TlsConfig tls = transport.getTls();
        if (!tls.isEnabled()) {
            return null;
        }
        if (!StringUtils.hasText(tls.getCert()) || !StringUtils.hasText(tls.getKey())) {
            throw new IllegalStateException("开启TLS时conduit.transport.tls.cert / key不能为空");
        }
        try (InputStream cert = openResource(tls.getCert());
             InputStream key = openResource(tls.getKey())) {
            return Tls.server(cert, key, tls.getKeyPassword());
        } catch (IOException e) {
            throw new IllegalStateException("初始化服务端TLS失败: " + e.getMessage(), e);
        }
    }

    /** 支持 {@code classpath:} 前缀与普通文件路径 */
    private static InputStream openResource(String location) throws IOException {
        if (location.startsWith("classpath:")) {
            return new ClassPathResource(location.substring("classpath:".length())).getInputStream();
        }
        return new FileInputStream(location);
    }

    private static byte compressId(String name) {
        if (!StringUtils.hasText(name) || "none".equalsIgnoreCase(name)) {
            return ProtocolConstants.COMPRESS_NONE;
        }
        if ("gzip".equalsIgnoreCase(name)) {
            return ProtocolConstants.COMPRESS_GZIP;
        }
        throw new IllegalArgumentException("不支持的压缩算法: " + name + "（可选: none/gzip）");
    }

    // ---------------------------------------------------------------- 配置结构

    public static class ApplicationConfig {
        private String name = "conduit-app";

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }
    }

    public static class RegistryConfig {
        private String type = "nacos";
        private String address = "127.0.0.1:8848";

        public String getType() {
            return type;
        }

        public void setType(String type) {
            this.type = type;
        }

        public String getAddress() {
            return address;
        }

        public void setAddress(String address) {
            this.address = address;
        }
    }

    public static class ProtocolConfig {
        private String host = "127.0.0.1";
        private int port = 20880;
        private String serialization = "jdk";
        /** 压缩算法：none/gzip */
        private String compress = "none";
        /** 压缩阈值（字节）：body达到该大小才压缩 */
        private int compressThreshold = 1024;

        public String getHost() {
            return host;
        }

        public void setHost(String host) {
            this.host = host;
        }

        public int getPort() {
            return port;
        }

        public void setPort(int port) {
            this.port = port;
        }

        public String getSerialization() {
            return serialization;
        }

        public void setSerialization(String serialization) {
            this.serialization = serialization;
        }

        public String getCompress() {
            return compress;
        }

        public void setCompress(String compress) {
            this.compress = compress;
        }

        public int getCompressThreshold() {
            return compressThreshold;
        }

        public void setCompressThreshold(int compressThreshold) {
            this.compressThreshold = compressThreshold;
        }
    }

    /** 传输层配置：客户端公共项 + 服务端 + 重连 + TLS */
    public static class TransportConfig {
        private long connectTimeoutMs = ClientOptions.DEFAULT_CONNECT_TIMEOUT_MILLIS;
        private long handshakeTimeoutMs = ClientOptions.DEFAULT_HANDSHAKE_TIMEOUT_MILLIS;
        private long heartbeatIntervalMs = ClientOptions.DEFAULT_HEARTBEAT_INTERVAL_MILLIS;
        private long readIdleTimeoutMs = ClientOptions.DEFAULT_READ_IDLE_TIMEOUT_MILLIS;
        private int maxConnectionsPerAddress = ClientOptions.DEFAULT_MAX_CONNECTIONS_PER_ADDRESS;
        private long idleConnectionTimeoutMs = ClientOptions.DEFAULT_IDLE_CONNECTION_TIMEOUT_MILLIS;
        private final ReconnectConfig reconnect = new ReconnectConfig();
        private final ServerConfig server = new ServerConfig();
        private final TlsConfig tls = new TlsConfig();

        public long getConnectTimeoutMs() {
            return connectTimeoutMs;
        }

        public void setConnectTimeoutMs(long connectTimeoutMs) {
            this.connectTimeoutMs = connectTimeoutMs;
        }

        public long getHandshakeTimeoutMs() {
            return handshakeTimeoutMs;
        }

        public void setHandshakeTimeoutMs(long handshakeTimeoutMs) {
            this.handshakeTimeoutMs = handshakeTimeoutMs;
        }

        public long getHeartbeatIntervalMs() {
            return heartbeatIntervalMs;
        }

        public void setHeartbeatIntervalMs(long heartbeatIntervalMs) {
            this.heartbeatIntervalMs = heartbeatIntervalMs;
        }

        public long getReadIdleTimeoutMs() {
            return readIdleTimeoutMs;
        }

        public void setReadIdleTimeoutMs(long readIdleTimeoutMs) {
            this.readIdleTimeoutMs = readIdleTimeoutMs;
        }

        public int getMaxConnectionsPerAddress() {
            return maxConnectionsPerAddress;
        }

        public void setMaxConnectionsPerAddress(int maxConnectionsPerAddress) {
            this.maxConnectionsPerAddress = maxConnectionsPerAddress;
        }

        public long getIdleConnectionTimeoutMs() {
            return idleConnectionTimeoutMs;
        }

        public void setIdleConnectionTimeoutMs(long idleConnectionTimeoutMs) {
            this.idleConnectionTimeoutMs = idleConnectionTimeoutMs;
        }

        public ReconnectConfig getReconnect() {
            return reconnect;
        }

        public ServerConfig getServer() {
            return server;
        }

        public TlsConfig getTls() {
            return tls;
        }
    }

    public static class ReconnectConfig {
        private boolean enabled = true;
        private int maxAttempts = ClientOptions.DEFAULT_RECONNECT_MAX_ATTEMPTS;
        private long baseDelayMs = ClientOptions.DEFAULT_RECONNECT_BASE_DELAY_MILLIS;
        private long maxDelayMs = ClientOptions.DEFAULT_RECONNECT_MAX_DELAY_MILLIS;

        public boolean isEnabled() {
            return enabled;
        }

        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }

        public int getMaxAttempts() {
            return maxAttempts;
        }

        public void setMaxAttempts(int maxAttempts) {
            this.maxAttempts = maxAttempts;
        }

        public long getBaseDelayMs() {
            return baseDelayMs;
        }

        public void setBaseDelayMs(long baseDelayMs) {
            this.baseDelayMs = baseDelayMs;
        }

        public long getMaxDelayMs() {
            return maxDelayMs;
        }

        public void setMaxDelayMs(long maxDelayMs) {
            this.maxDelayMs = maxDelayMs;
        }
    }

    public static class ServerConfig {
        private int businessThreads = ServerOptions.DEFAULT_BUSINESS_THREADS;
        private int queueCapacity = ServerOptions.DEFAULT_QUEUE_CAPACITY;
        private long readerIdleTimeoutMs = ServerOptions.DEFAULT_READER_IDLE_TIMEOUT_MILLIS;
        private long shutdownTimeoutMs = ServerOptions.DEFAULT_SHUTDOWN_TIMEOUT_MILLIS;

        public int getBusinessThreads() {
            return businessThreads;
        }

        public void setBusinessThreads(int businessThreads) {
            this.businessThreads = businessThreads;
        }

        public int getQueueCapacity() {
            return queueCapacity;
        }

        public void setQueueCapacity(int queueCapacity) {
            this.queueCapacity = queueCapacity;
        }

        public long getReaderIdleTimeoutMs() {
            return readerIdleTimeoutMs;
        }

        public void setReaderIdleTimeoutMs(long readerIdleTimeoutMs) {
            this.readerIdleTimeoutMs = readerIdleTimeoutMs;
        }

        public long getShutdownTimeoutMs() {
            return shutdownTimeoutMs;
        }

        public void setShutdownTimeoutMs(long shutdownTimeoutMs) {
            this.shutdownTimeoutMs = shutdownTimeoutMs;
        }
    }

    /** 集群容错配置：重试次数、退避与熔断器参数 */
    public static class ClusterConfig {
        private int retries = ClusterOptions.DEFAULT_RETRIES;
        private long retryBackoffMs = ClusterOptions.DEFAULT_RETRY_BACKOFF_MS;
        private final BreakerConfig breaker = new BreakerConfig();

        public int getRetries() {
            return retries;
        }

        public void setRetries(int retries) {
            this.retries = retries;
        }

        public long getRetryBackoffMs() {
            return retryBackoffMs;
        }

        public void setRetryBackoffMs(long retryBackoffMs) {
            this.retryBackoffMs = retryBackoffMs;
        }

        public BreakerConfig getBreaker() {
            return breaker;
        }
    }

    /** 熔断器配置 */
    public static class BreakerConfig {
        private int failureThreshold = CircuitBreakerOptions.DEFAULT_FAILURE_THRESHOLD;
        private long openDurationMs = CircuitBreakerOptions.DEFAULT_OPEN_DURATION_MS;

        public int getFailureThreshold() {
            return failureThreshold;
        }

        public void setFailureThreshold(int failureThreshold) {
            this.failureThreshold = failureThreshold;
        }

        public long getOpenDurationMs() {
            return openDurationMs;
        }

        public void setOpenDurationMs(long openDurationMs) {
            this.openDurationMs = openDurationMs;
        }
    }

    /** TLS配置：服务端证书/私钥 + 客户端信任证书 */
    public static class TlsConfig {
        private boolean enabled = false;
        private String cert;
        private String key;
        private String keyPassword;
        private String trustCert;
        private boolean insecure = false;

        public boolean isEnabled() {
            return enabled;
        }

        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }

        public String getCert() {
            return cert;
        }

        public void setCert(String cert) {
            this.cert = cert;
        }

        public String getKey() {
            return key;
        }

        public void setKey(String key) {
            this.key = key;
        }

        public String getKeyPassword() {
            return keyPassword;
        }

        public void setKeyPassword(String keyPassword) {
            this.keyPassword = keyPassword;
        }

        public String getTrustCert() {
            return trustCert;
        }

        public void setTrustCert(String trustCert) {
            this.trustCert = trustCert;
        }

        public boolean isInsecure() {
            return insecure;
        }

        public void setInsecure(boolean insecure) {
            this.insecure = insecure;
        }
    }
}
