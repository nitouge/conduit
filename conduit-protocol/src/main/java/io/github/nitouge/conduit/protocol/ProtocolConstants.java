package io.github.nitouge.conduit.protocol;

/**
 * 协议常量：消息头各字段的取值与长度，客户端/服务端必须严格一致。
 *
 * <p>18字节定长头：magic(2) version(1) type(1) codec(1) compress(1) requestId(8) length(4)。</p>
 */
public final class ProtocolConstants {

    private ProtocolConstants() {
    }

    /** 魔数，快速识别Conduit协议帧（0xCAFE） */
    public static final short MAGIC = (short) 0xCAFE;

    /** 协议版本 */
    public static final byte VERSION = 1;

    /** 定长消息头长度（字节） */
    public static final int HEADER_LENGTH = 18;

    /** length字段在帧内的偏移：magic2+version1+type1+codec1+compress1+requestId8 = 14 */
    public static final int LENGTH_FIELD_OFFSET = 14;

    /** length字段长度（int） */
    public static final int LENGTH_FIELD_LENGTH = 4;

    /** 单帧最大长度（16MB），防御异常超大帧 */
    public static final int MAX_FRAME_LENGTH = 16 * 1024 * 1024;

    // ---- type：消息类型 ----
    public static final byte TYPE_REQUEST = 1;
    public static final byte TYPE_RESPONSE = 2;
    public static final byte TYPE_HEARTBEAT = 3;

    // ---- codec：序列化类型 id（与SerializerType一致）----
    public static final byte CODEC_JDK = 1;
    public static final byte CODEC_JSON = 2;
    public static final byte CODEC_KRYO = 3;
    public static final byte CODEC_PROTOSTUFF = 4;

    // ---- compress：压缩类型 ----
    public static final byte COMPRESS_NONE = 0;
    public static final byte COMPRESS_GZIP = 1;
}
