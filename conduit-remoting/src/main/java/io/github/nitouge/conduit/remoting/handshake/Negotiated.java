package io.github.nitouge.conduit.remoting.handshake;

/**
 * 一次连接握手协商出的结果：协议版本、序列化codec与压缩算法。
 *
 * <p>业务帧必须使用协商结果（版本/codec/compress）编码，保证两端对同一帧的解读完全一致。</p>
 */
public final class Negotiated {

    private final byte version;
    private final byte codec;
    private final byte compress;

    public Negotiated(byte version, byte codec, byte compress) {
        this.version = version;
        this.codec = codec;
        this.compress = compress;
    }

    public byte getVersion() {
        return version;
    }

    public byte getCodec() {
        return codec;
    }

    public byte getCompress() {
        return compress;
    }

    @Override
    public String toString() {
        return "Negotiated{" +
                "version=" + version +
                ", codec=" + codec +
                ", compress=" + compress +
                '}';
    }
}
