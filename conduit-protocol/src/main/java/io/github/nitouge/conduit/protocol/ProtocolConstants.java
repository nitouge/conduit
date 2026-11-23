package io.github.nitouge.conduit.protocol;

/**
 * 协议常量：消息头各字段的取值与长度，客户端/服务端必须严格一致。
 *
 * <p>18字节定长头：magic(2) version(1) type(1) codec(1) compress(1) requestId(8) length(4)。</p>
 *
 * <h2>协议版本与协商</h2>
 * <p>头里的 {@code version} 是「帧格式版本」。为了让未来升级（改头布局、加字段语义）时两端能
 * 互相识别而不是静默错乱，连接建立后先做一次<b>版本协商</b>（见 {@link #TYPE_HANDSHAKE}）：</p>
 * <ol>
 *   <li>客户端在头里携带自己支持的<b>最高</b>版本发起握手；</li>
 *   <li>服务端在 {@code [MIN_SUPPORTED_VERSION, MAX_SUPPORTED_VERSION]} 与客户端版本之间取交集，
 *       取交集最高版本作为协商结果，回 {@link #TYPE_HANDSHAKE_ACK}（头里 version/codec/compress 即协商结果）；</li>
 *   <li>无交集则以 {@link #TYPE_HANDSHAKE_REJECT} 明确告知原因后断开。</li>
 * </ol>
 * <p>协商完成后，业务帧头里的 version 必须落在双方协商结果上；解码器对超出本端点支持区间的
 * 业务帧直接判为协议违规并断开。</p>
 */
public final class ProtocolConstants {

    private ProtocolConstants() {
    }

    /** 魔数，快速识别Conduit协议帧（0xCAFE） */
    public static final short MAGIC = (short) 0xCAFE;

    /** 默认协议版本 */
    public static final byte VERSION = 1;

    /** 本端点支持的最低协议版本（版本协商下界） */
    public static final byte MIN_SUPPORTED_VERSION = 1;

    /** 本端点支持的最高协议版本（版本协商上界） */
    public static final byte MAX_SUPPORTED_VERSION = 1;

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

    /** 心跳帧：body为空（length=0）；服务端收到后原样回一帧，用于保活与半开连接探测 */
    public static final byte TYPE_HEARTBEAT = 3;

    /** 握手请求（客户端 -> 服务端）。body为1字节status（恒为 {@link #HANDSHAKE_OK}） */
    public static final byte TYPE_HANDSHAKE = 4;

    /** 握手成功应答（服务端 -> 客户端）。头里version/codec/compress即协商结果，body为1字节status */
    public static final byte TYPE_HANDSHAKE_ACK = 5;

    /** 握手拒绝（服务端 -> 客户端）。body为1字节 {@code HANDSHAKE_*} 拒绝原因 */
    public static final byte TYPE_HANDSHAKE_REJECT = 6;

    // ---- handshake status：握手结果/拒绝原因 ----
    /** 成功 */
    public static final byte HANDSHAKE_OK = 0;
    /** 双方没有共同支持的协议版本 */
    public static final byte HANDSHAKE_NO_COMMON_VERSION = 1;
    /** 服务端不支持客户端提议的序列化codec */
    public static final byte HANDSHAKE_UNSUPPORTED_CODEC = 2;
    /** 服务端不支持客户端提议的压缩算法 */
    public static final byte HANDSHAKE_UNSUPPORTED_COMPRESS = 3;
    /** 客户端本地状态：等待握手应答超时（不会出现在线上帧里） */
    public static final byte HANDSHAKE_TIMEOUT = 4;

    // ---- codec：序列化类型 id（与SerializerType一致）----
    public static final byte CODEC_JDK = 1;
    public static final byte CODEC_JSON = 2;
    public static final byte CODEC_KRYO = 3;
    public static final byte CODEC_PROTOSTUFF = 4;

    // ---- compress：压缩类型 ----
    public static final byte COMPRESS_NONE = 0;
    public static final byte COMPRESS_GZIP = 1;

    /** 是否为握手类帧（请求/应答/拒绝） */
    public static boolean isHandshakeType(byte type) {
        return type == TYPE_HANDSHAKE || type == TYPE_HANDSHAKE_ACK || type == TYPE_HANDSHAKE_REJECT;
    }

    /** 是否为控制帧（不承载业务payload） */
    public static boolean isControlType(byte type) {
        return type == TYPE_HEARTBEAT || isHandshakeType(type);
    }

    /** 给定版本是否在本端点支持区间内 */
    public static boolean isSupportedVersion(int version) {
        return version >= MIN_SUPPORTED_VERSION && version <= MAX_SUPPORTED_VERSION;
    }

    /**
     * 版本协商：在 [本端点下界, 本端点上界] 与客户端提议版本之间取交集最高版本。
     *
     * <p>客户端总是在握手里提议自己支持的<b>最高</b>版本（隐含它也支持更低版本），因此：</p>
     * <ul>
     *   <li>提议版本高于本端点上界（如未来客户端 v9 连老服务端 v1）→ 平滑降级到本端点上界，
     *       而不是拒绝——这正是向后兼容的价值；</li>
     *   <li>提议版本低于本端点下界（如远古客户端 v0 连 v1 服务端）→ 无交集，返回 {@code -1}，
     *       由握手拒绝并明确告知原因。</li>
     * </ul>
     *
     * @return 协商出的版本；无交集返回 {@code -1}
     */
    public static int negotiateVersion(int offered) {
        int negotiated = Math.min(offered, MAX_SUPPORTED_VERSION);
        return negotiated >= MIN_SUPPORTED_VERSION ? negotiated : -1;
    }

    /** 握手状态/拒绝原因的可读文本，便于日志排障 */
    public static String handshakeStatusText(byte status) {
        return switch (status) {
            case HANDSHAKE_OK -> "OK";
            case HANDSHAKE_NO_COMMON_VERSION -> "NO_COMMON_VERSION";
            case HANDSHAKE_UNSUPPORTED_CODEC -> "UNSUPPORTED_CODEC";
            case HANDSHAKE_UNSUPPORTED_COMPRESS -> "UNSUPPORTED_COMPRESS";
            case HANDSHAKE_TIMEOUT -> "TIMEOUT";
            default -> "UNKNOWN(" + status + ")";
        };
    }

    /** 消息类型可读文本，便于日志排障 */
    public static String typeName(byte type) {
        return switch (type) {
            case TYPE_REQUEST -> "REQUEST";
            case TYPE_RESPONSE -> "RESPONSE";
            case TYPE_HEARTBEAT -> "HEARTBEAT";
            case TYPE_HANDSHAKE -> "HANDSHAKE";
            case TYPE_HANDSHAKE_ACK -> "HANDSHAKE_ACK";
            case TYPE_HANDSHAKE_REJECT -> "HANDSHAKE_REJECT";
            default -> "UNKNOWN(" + type + ")";
        };
    }
}
