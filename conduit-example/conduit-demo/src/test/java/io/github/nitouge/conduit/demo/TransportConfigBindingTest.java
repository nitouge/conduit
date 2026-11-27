package io.github.nitouge.conduit.demo;

import io.github.nitouge.conduit.client.RpcClient;
import io.github.nitouge.conduit.protocol.ProtocolConstants;
import io.github.nitouge.conduit.remoting.ClientOptions;
import io.github.nitouge.conduit.remoting.ServerOptions;
import io.github.nitouge.conduit.serialization.SerializerType;
import io.github.nitouge.conduit.spring.ConduitAutoConfiguration;
import io.github.nitouge.conduit.spring.ConduitProperties;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 传输层配置绑定验收：验证 {@code conduit.transport.*} 的 kebab-case 属性真的绑定进了
 * {@link ClientOptions}/{@link ServerOptions}，并作用到自动装配出的 {@link RpcClient}。
 *
 * <p>为什么需要这个测试：Spring 的宽松绑定对写错的属性名<b>不会报错</b>，只会静默使用默认值
 * （例如把 {@code max-connections-per-address} 拼错，运行时仍是单连接，很难发现）。
 * 这里用 {@link ApplicationContextRunner} 把每个配置项都断言一遍。</p>
 */
class TransportConfigBindingTest {

    private ApplicationContextRunner runner() {
        return new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(ConduitAutoConfiguration.class))
                .withPropertyValues(
                        "conduit.registry.type=local",
                        "conduit.registry.address=transport-binding-test",
                        "conduit.protocol.serialization=kryo",
                        "conduit.protocol.compress=gzip",
                        "conduit.protocol.compress-threshold=2048",
                        // 客户端传输项
                        "conduit.transport.connect-timeout-ms=1111",
                        "conduit.transport.handshake-timeout-ms=2222",
                        "conduit.transport.heartbeat-interval-ms=3333",
                        "conduit.transport.read-idle-timeout-ms=9999",
                        "conduit.transport.max-connections-per-address=3",
                        "conduit.transport.idle-connection-timeout-ms=4444",
                        "conduit.transport.max-in-flight-per-connection=55",
                        "conduit.transport.write-buffer-high-water-mark=131072",
                        "conduit.transport.write-buffer-low-water-mark=65536",
                        "conduit.transport.reconnect.enabled=false",
                        "conduit.transport.reconnect.max-attempts=7",
                        "conduit.transport.reconnect.base-delay-ms=555",
                        "conduit.transport.reconnect.max-delay-ms=6666",
                        // 服务端传输项
                        "conduit.transport.server.business-threads=9",
                        "conduit.transport.server.queue-capacity=77",
                        "conduit.transport.server.reader-idle-timeout-ms=8888",
                        "conduit.transport.server.shutdown-timeout-ms=1234",
                        "conduit.transport.tls.enabled=false");
    }

    @Test
    void kebabCasePropertiesBindToClientOptions() {
        runner().run(context -> {
            assertNullStartupFailure(context.getStartupFailure());
            ConduitProperties props = context.getBean(ConduitProperties.class);
            ClientOptions options = props.toClientOptions();

            assertEquals(SerializerType.idOf("kryo"), options.getCodec());
            assertEquals(ProtocolConstants.COMPRESS_GZIP, options.getCompress());
            assertEquals(2048, options.getCompressThreshold());
            assertEquals(1111, options.getConnectTimeoutMillis());
            assertEquals(2222, options.getHandshakeTimeoutMillis());
            assertEquals(3333, options.getHeartbeatIntervalMillis());
            assertEquals(9999, options.getReadIdleTimeoutMillis());
            assertEquals(3, options.getMaxConnectionsPerAddress());
            assertEquals(4444, options.getIdleConnectionTimeoutMillis());
            assertEquals(55, options.getMaxInFlightPerConnection());
            assertEquals(131072, options.getWriteBufferHighWaterMark());
            assertEquals(65536, options.getWriteBufferLowWaterMark());
            assertFalse(options.isReconnectEnabled());
            assertEquals(7, options.getReconnectMaxAttempts());
            assertEquals(555, options.getReconnectBaseDelayMillis());
            assertEquals(6666, options.getReconnectMaxDelayMillis());

            // 自动装配出的 RpcClient 必须真正使用这套配置
            RpcClient rpcClient = context.getBean(RpcClient.class);
            ClientOptions beanOptions = rpcClient.nettyClient().getOptions();
            assertEquals(options.getCodec(), beanOptions.getCodec());
            assertEquals(options.getMaxConnectionsPerAddress(), beanOptions.getMaxConnectionsPerAddress());
            assertEquals(options.getHeartbeatIntervalMillis(), beanOptions.getHeartbeatIntervalMillis());
        });
    }

    @Test
    void kebabCasePropertiesBindToServerOptions() {
        runner().run(context -> {
            assertNullStartupFailure(context.getStartupFailure());
            ConduitProperties props = context.getBean(ConduitProperties.class);
            ServerOptions options = props.toServerOptions();

            assertEquals(9, options.getBusinessThreads());
            assertEquals(77, options.getQueueCapacity());
            assertEquals(8888, options.getReaderIdleTimeoutMillis());
            assertEquals(1234, options.getShutdownTimeoutMillis());
            // 握手超时是两端公共项；响应压缩阈值复用 protocol.compress-threshold
            assertEquals(2222, options.getHandshakeTimeoutMillis());
            assertEquals(2048, options.getCompressThreshold());
        });
    }

    @Test
    void invalidCompressFailsFastAtStartup() {
        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(ConduitAutoConfiguration.class))
                .withPropertyValues(
                        "conduit.registry.type=local",
                        "conduit.protocol.compress=snappy")
                .run(context -> {
                    Throwable failure = context.getStartupFailure();
                    assertNotNull(failure, "不支持的压缩算法应在启动时快速失败");
                    assertTrue(hasMessage(failure, "不支持的压缩算法"),
                            "失败原因应明确指出压缩算法不合法: " + failure);
                });
    }

    @Test
    void readIdleNotGreaterThanHeartbeatFailsFast() {
        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(ConduitAutoConfiguration.class))
                .withPropertyValues(
                        "conduit.registry.type=local",
                        "conduit.transport.heartbeat-interval-ms=5000",
                        "conduit.transport.read-idle-timeout-ms=5000")
                .run(context -> {
                    Throwable failure = context.getStartupFailure();
                    assertNotNull(failure, "读空闲<=心跳间隔属于非法配置，应在启动时快速失败");
                    assertTrue(hasMessage(failure, "readIdleTimeoutMillis"),
                            "失败原因应指出读空闲配置问题: " + failure);
                });
    }

    private static void assertNullStartupFailure(Throwable failure) {
        if (failure != null) {
            throw new AssertionError("上下文启动失败: " + failure, failure);
        }
    }

    private static boolean hasMessage(Throwable failure, String expected) {
        Throwable current = failure;
        while (current != null) {
            if (current.getMessage() != null && current.getMessage().contains(expected)) {
                return true;
            }
            current = current.getCause();
        }
        return false;
    }
}
