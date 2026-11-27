package io.github.nitouge.conduit.protocol;

/**
 * 一帧消息的内存表示：把协议头关键字段与payload放在一起，便于编解码与在pipeline中流转。
 *
 * <p>payload为RpcRequest（type=请求）或RpcResponse（type=响应）；心跳/握手/取消等控制帧payload为null，
 * 握手帧用 {@link #getStatus()} 表达结果（成功/拒绝原因），取消帧用它表达取消原因。</p>
 */
public class RpcMessage {

    /** 帧格式版本，默认当前版本；握手后为双方协商出的版本 */
    private byte version = ProtocolConstants.VERSION;

    /** 消息类型：请求/响应/心跳/握手 */
    private byte type;

    /** 序列化类型id */
    private byte codec;

    /** 压缩类型 */
    private byte compress;

    /** 控制帧状态：握手成功/拒绝原因；业务帧不使用 */
    private byte status;

    /** 协议头里的请求ID（请求-响应关联的唯一依据；控制帧通常为0） */
    private long requestId;

    /** RpcRequest或RpcResponse；控制帧为null */
    private Object payload;

    public RpcMessage() {
    }

    /** 构造一个请求帧（默认codec，compress=NONE，version=当前版本） */
    public static RpcMessage request(long requestId, byte codec, Object payload) {
        return build(ProtocolConstants.TYPE_REQUEST, codec, ProtocolConstants.VERSION,
                ProtocolConstants.COMPRESS_NONE, ProtocolConstants.HANDSHAKE_OK, requestId, payload);
    }

    /** 构造一个响应帧 */
    public static RpcMessage response(long requestId, byte codec, Object payload) {
        return build(ProtocolConstants.TYPE_RESPONSE, codec, ProtocolConstants.VERSION,
                ProtocolConstants.COMPRESS_NONE, ProtocolConstants.HANDSHAKE_OK, requestId, payload);
    }

    /** 构造一个心跳帧（无body） */
    public static RpcMessage heartbeat(long requestId, byte codec) {
        return build(ProtocolConstants.TYPE_HEARTBEAT, codec, ProtocolConstants.VERSION,
                ProtocolConstants.COMPRESS_NONE, ProtocolConstants.HANDSHAKE_OK, requestId, null);
    }

    /**
     * 构造一个握手请求帧：头里携带客户端提议的版本、codec与压缩算法。
     *
     * @param version  客户端支持的最高协议版本
     * @param codec    客户端希望使用的序列化codec
     * @param compress 客户端希望使用的压缩算法
     */
    public static RpcMessage handshake(byte version, byte codec, byte compress) {
        return build(ProtocolConstants.TYPE_HANDSHAKE, codec, version, compress,
                ProtocolConstants.HANDSHAKE_OK, 0L, null);
    }

    /**
     * 构造一个取消帧：requestId 指向要取消的请求，{@code reason} 说明为什么不等了。
     *
     * <p>无 body payload（原因放在 {@link #getStatus()}，与握手帧同构：控制帧的原因/结果都放
     * status，不依赖任何序列化器）。</p>
     *
     * @param requestId 要取消的请求id（必须是本连接上已发出、尚未响应的请求）
     * @param reason    {@code ProtocolConstants.CANCEL_*} 之一
     */
    public static RpcMessage cancel(long requestId, byte codec, byte version, byte reason) {
        return build(ProtocolConstants.TYPE_CANCEL, codec, version,
                ProtocolConstants.COMPRESS_NONE, reason, requestId, null);
    }

    /** 构造一个握手成功应答帧：头里version/codec/compress为服务端接受（协商）的结果 */
    public static RpcMessage handshakeAck(byte version, byte codec, byte compress) {
        return build(ProtocolConstants.TYPE_HANDSHAKE_ACK, codec, version, compress,
                ProtocolConstants.HANDSHAKE_OK, 0L, null);
    }

    /** 构造一个握手拒绝帧：携带拒绝原因与服务端支持的版本区间上界（便于对端排障） */
    public static RpcMessage handshakeReject(byte status, byte serverVersion) {
        return build(ProtocolConstants.TYPE_HANDSHAKE_REJECT, ProtocolConstants.CODEC_JDK, serverVersion,
                ProtocolConstants.COMPRESS_NONE, status, 0L, null);
    }

    private static RpcMessage build(byte type, byte codec, byte version, byte compress,
                                    byte status, long requestId, Object payload) {
        RpcMessage m = new RpcMessage();
        m.type = type;
        m.codec = codec;
        m.version = version;
        m.compress = compress;
        m.status = status;
        m.requestId = requestId;
        m.payload = payload;
        return m;
    }

    public boolean isHeartbeat() {
        return type == ProtocolConstants.TYPE_HEARTBEAT;
    }

    public boolean isHandshake() {
        return ProtocolConstants.isHandshakeType(type);
    }

    public boolean isHandshakeRequest() {
        return type == ProtocolConstants.TYPE_HANDSHAKE;
    }

    public boolean isHandshakeAck() {
        return type == ProtocolConstants.TYPE_HANDSHAKE_ACK;
    }

    public boolean isHandshakeReject() {
        return type == ProtocolConstants.TYPE_HANDSHAKE_REJECT;
    }

    /** 是否为不承载业务payload的控制帧（心跳/握手/取消） */
    public boolean isControlFrame() {
        return ProtocolConstants.isControlType(type);
    }

    /** 是否为取消帧 */
    public boolean isCancel() {
        return ProtocolConstants.isCancelType(type);
    }

    public byte getVersion() {
        return version;
    }

    public void setVersion(byte version) {
        this.version = version;
    }

    public byte getType() {
        return type;
    }

    public void setType(byte type) {
        this.type = type;
    }

    public byte getCodec() {
        return codec;
    }

    public void setCodec(byte codec) {
        this.codec = codec;
    }

    public byte getCompress() {
        return compress;
    }

    public void setCompress(byte compress) {
        this.compress = compress;
    }

    public byte getStatus() {
        return status;
    }

    public void setStatus(byte status) {
        this.status = status;
    }

    public long getRequestId() {
        return requestId;
    }

    public void setRequestId(long requestId) {
        this.requestId = requestId;
    }

    public Object getPayload() {
        return payload;
    }

    public void setPayload(Object payload) {
        this.payload = payload;
    }

    @Override
    public String toString() {
        return "RpcMessage{" +
                "version=" + version +
                ", type=" + ProtocolConstants.typeName(type) +
                ", codec=" + codec +
                ", compress=" + compress +
                ", status=" + status +
                ", requestId=" + requestId +
                ", payload=" + payload +
                '}';
    }
}
