package io.github.nitouge.conduit.protocol;

import io.github.nitouge.conduit.common.exception.RpcException;
import io.github.nitouge.conduit.protocol.codec.Compressor;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 压缩器单元测试：往返一致性、能力判定与解压炸弹上限。
 */
class CompressorTest {

    @Test
    void noneIsIdentity() {
        byte[] data = "conduit".getBytes(StandardCharsets.UTF_8);
        assertArrayEquals(data, Compressor.compress(ProtocolConstants.COMPRESS_NONE, data));
        assertArrayEquals(data, Compressor.decompress(ProtocolConstants.COMPRESS_NONE, data));
    }

    @Test
    void gzipRoundTrip() {
        byte[] data = "hello conduit ".repeat(200).getBytes(StandardCharsets.UTF_8);
        byte[] compressed = Compressor.compress(ProtocolConstants.COMPRESS_GZIP, data);

        assertTrue(compressed.length < data.length, "重复文本应被显著压缩");
        assertArrayEquals(data, Compressor.decompress(ProtocolConstants.COMPRESS_GZIP, compressed));
    }

    @Test
    void supportsKnownAlgorithmsOnly() {
        assertTrue(Compressor.isSupported(ProtocolConstants.COMPRESS_NONE));
        assertTrue(Compressor.isSupported(ProtocolConstants.COMPRESS_GZIP));
        assertFalse(Compressor.isSupported((byte) 42));
    }

    @Test
    void unknownAlgorithmIsRejected() {
        byte[] data = new byte[]{1, 2, 3};
        assertThrows(RpcException.class, () -> Compressor.compress((byte) 42, data));
        assertThrows(RpcException.class, () -> Compressor.decompress((byte) 42, data));
    }

    @Test
    void decompressionBombIsRejected() {
        // 高压缩比报文：解压后超过上限(MAX_FRAME_LENGTH*4)必须中止，而不是把堆撑爆
        long oversize = (long) ProtocolConstants.MAX_FRAME_LENGTH * 4 + 1024 * 1024;
        byte[] zeros = new byte[(int) oversize];
        byte[] compressed = Compressor.compress(ProtocolConstants.COMPRESS_GZIP, zeros);

        assertTrue(compressed.length < zeros.length / 100);
        assertThrows(RpcException.class,
                () -> Compressor.decompress(ProtocolConstants.COMPRESS_GZIP, compressed));
        // 保留引用，避免JIT在断言前回收（无实际作用，仅表达测试意图）
        assertTrue(Arrays.equals(compressed, compressed));
    }
}
