package io.github.nitouge.conduit.protocol;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link ProtocolOptions} 单元测试：压缩阈值判定与非法配置快速失败。
 */
class ProtocolOptionsTest {

    @Test
    void compressionOnlyWhenThresholdReached() {
        ProtocolOptions options = ProtocolOptions.builder()
                .compress(ProtocolConstants.COMPRESS_GZIP)
                .compressThreshold(100)
                .build();

        assertFalse(options.shouldCompress(ProtocolConstants.COMPRESS_GZIP, 99));
        assertTrue(options.shouldCompress(ProtocolConstants.COMPRESS_GZIP, 100));
        assertTrue(options.shouldCompress(ProtocolConstants.COMPRESS_GZIP, 10_000));
        assertFalse(options.shouldCompress(ProtocolConstants.COMPRESS_NONE, 10_000),
                "未配置压缩算法时不应压缩");
    }

    @Test
    void rejectsUnknownCompressAlgorithm() {
        assertThrows(IllegalArgumentException.class,
                () -> ProtocolOptions.builder().compress((byte) 42));
    }

    @Test
    void rejectsNegativeThreshold() {
        assertThrows(IllegalArgumentException.class,
                () -> ProtocolOptions.builder().compressThreshold(-1));
    }

    @Test
    void defaultsAreUncompressedWithDocumentedThreshold() {
        ProtocolOptions options = ProtocolOptions.defaults();
        assertFalse(options.shouldCompress(options.getCompress(), Integer.MAX_VALUE));
        assertFalse(options.shouldCompress(ProtocolConstants.COMPRESS_GZIP,
                ProtocolOptions.DEFAULT_COMPRESS_THRESHOLD - 1));
        assertTrue(options.shouldCompress(ProtocolConstants.COMPRESS_GZIP,
                ProtocolOptions.DEFAULT_COMPRESS_THRESHOLD));
    }
}
