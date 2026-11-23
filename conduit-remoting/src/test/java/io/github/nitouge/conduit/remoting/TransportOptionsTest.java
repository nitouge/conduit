package io.github.nitouge.conduit.remoting;

import io.github.nitouge.conduit.protocol.ProtocolConstants;
import io.github.nitouge.conduit.protocol.ProtocolOptions;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link ClientOptions} / {@link ServerOptions} 单元测试：默认值与非法配置快速失败。
 */
class TransportOptionsTest {

    @Test
    void clientDefaultsAreProductionSane() {
        ClientOptions options = ClientOptions.defaults();

        assertEquals(ProtocolConstants.CODEC_JDK, options.getCodec());
        assertEquals(ProtocolConstants.COMPRESS_NONE, options.getCompress());
        assertEquals(1, options.getMaxConnectionsPerAddress());
        assertEquals(30_000, options.getHeartbeatIntervalMillis());
        assertEquals(90_000, options.getReadIdleTimeoutMillis());
        assertTrue(options.isReconnectEnabled());
        assertNull(options.getSslContext());
    }

    @Test
    void readIdleMustBeGreaterThanHeartbeat() {
        assertThrows(IllegalArgumentException.class, () -> ClientOptions.builder()
                .heartbeatIntervalMillis(1_000)
                .readIdleTimeoutMillis(1_000)
                .build());
    }

    @Test
    void rejectsUnknownCompressAlgorithm() {
        assertThrows(IllegalArgumentException.class,
                () -> ClientOptions.builder().compress((byte) 42));
    }

    @Test
    void rejectsInvalidClientTimeoutsAndPoolSize() {
        assertThrows(IllegalArgumentException.class, () -> ClientOptions.builder().connectTimeoutMillis(0).build());
        assertThrows(IllegalArgumentException.class, () -> ClientOptions.builder().handshakeTimeoutMillis(0).build());
        assertThrows(IllegalArgumentException.class, () -> ClientOptions.builder().maxConnectionsPerAddress(0).build());
        assertThrows(IllegalArgumentException.class, () -> ClientOptions.builder().compressThreshold(-1).build());
    }

    @Test
    void protocolOptionsCarryCodecCompressAndThreshold() {
        ClientOptions options = ClientOptions.builder()
                .codec(ProtocolConstants.CODEC_KRYO)
                .compress(ProtocolConstants.COMPRESS_GZIP)
                .compressThreshold(64)
                .build();

        ProtocolOptions protocolOptions = options.protocolOptions();
        assertEquals(ProtocolConstants.CODEC_KRYO, protocolOptions.getCodec());
        assertEquals(ProtocolConstants.COMPRESS_GZIP, protocolOptions.getCompress());
        assertEquals(64, protocolOptions.getCompressThreshold());
    }

    @Test
    void serverDefaultsAndValidation() {
        ServerOptions defaults = ServerOptions.defaults();
        assertEquals(200, defaults.getBusinessThreads());
        assertEquals(10_000, defaults.getQueueCapacity());
        assertEquals(60_000, defaults.getReaderIdleTimeoutMillis());
        assertEquals(10_000, defaults.getShutdownTimeoutMillis());

        assertThrows(IllegalArgumentException.class, () -> ServerOptions.builder().businessThreads(0).build());
        assertThrows(IllegalArgumentException.class, () -> ServerOptions.builder().queueCapacity(0).build());
        assertThrows(IllegalArgumentException.class, () -> ServerOptions.builder().handshakeTimeoutMillis(0).build());
        assertThrows(IllegalArgumentException.class, () -> ServerOptions.builder().readerIdleTimeoutMillis(-1).build());
        assertThrows(IllegalArgumentException.class, () -> ServerOptions.builder().shutdownTimeoutMillis(0).build());
        assertThrows(IllegalArgumentException.class, () -> ServerOptions.builder().compressThreshold(-1).build());
    }
}
