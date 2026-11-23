package io.github.nitouge.conduit.protocol.codec;

import io.github.nitouge.conduit.common.exception.RpcException;
import io.github.nitouge.conduit.protocol.ProtocolConstants;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * body压缩/解压。压缩发生在序列化之后、写socket之前；解压发生在反序列化之前。
 *
 * <p>M1默认NONE（不压缩）；同时内置GZIP以便大报文场景启用。</p>
 */
public final class Compressor {

    private Compressor() {
    }

    /** 本端点是否支持该压缩算法（用于握手能力协商与解码校验） */
    public static boolean isSupported(byte type) {
        return type == ProtocolConstants.COMPRESS_NONE || type == ProtocolConstants.COMPRESS_GZIP;
    }

    public static byte[] compress(byte type, byte[] data) {
        return switch (type) {
            case ProtocolConstants.COMPRESS_NONE -> data;
            case ProtocolConstants.COMPRESS_GZIP -> gzip(data);
            default -> throw new RpcException("未知的压缩类型: " + type);
        };
    }

    public static byte[] decompress(byte type, byte[] data) {
        return switch (type) {
            case ProtocolConstants.COMPRESS_NONE -> data;
            case ProtocolConstants.COMPRESS_GZIP -> ungzip(data);
            default -> throw new RpcException("未知的压缩类型: " + type);
        };
    }

    private static byte[] gzip(byte[] data) {
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
             GZIPOutputStream gzip = new GZIPOutputStream(bos)) {
            gzip.write(data);
            gzip.finish();
            return bos.toByteArray();
        } catch (IOException e) {
            throw new RpcException("gzip compress failed", e);
        }
    }

    /**
     * 解压上限：防解压炸弹。上游LengthFieldBasedFrameDecoder的MAX_FRAME_LENGTH只约束<b>压缩后</b>
     * 的帧大小，高压缩比报文（16MB可膨胀到数GB）若不设上限会在解码线程耗尽堆内存。这里把解压后的
     * 总字节数限制在原帧上限的若干倍，超过即判为异常帧并中止。
     */
    private static final long MAX_DECOMPRESSED_BYTES = ProtocolConstants.MAX_FRAME_LENGTH * 4L;

    private static byte[] ungzip(byte[] data) {
        try (ByteArrayInputStream bis = new ByteArrayInputStream(data);
             GZIPInputStream gzip = new GZIPInputStream(bis);
             ByteArrayOutputStream bos = new ByteArrayOutputStream()) {
            byte[] buf = new byte[1024];
            int n;
            long total = 0;
            while ((n = gzip.read(buf)) != -1) {
                total += n;
                if (total > MAX_DECOMPRESSED_BYTES) {
                    throw new RpcException("gzip解压超出上限(" + MAX_DECOMPRESSED_BYTES + "字节)，疑似解压炸弹");
                }
                bos.write(buf, 0, n);
            }
            return bos.toByteArray();
        } catch (IOException e) {
            throw new RpcException("gzip decompress failed", e);
        }
    }
}
