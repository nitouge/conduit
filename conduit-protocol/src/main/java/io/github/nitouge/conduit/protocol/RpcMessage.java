package io.github.nitouge.conduit.protocol;

/**
 * 一帧消息的内存表示：把协议头关键字段与payload放在一起，便于编解码与在pipeline中流转。
 *
 * <p>payload为RpcRequest（type=请求）或RpcResponse（type=响应）；心跳帧payload为null。</p>
 */
public class RpcMessage {

    /** 消息类型：请求/响应/心跳 */
    private byte type;

    /** 序列化类型id */
    private byte codec;

    /** 压缩类型 */
    private byte compress;

    /** 协议头里的请求ID（请求-响应关联的唯一依据） */
    private long requestId;

    /** RpcRequest或RpcResponse；心跳为null */
    private Object payload;

    public RpcMessage() {
    }

    /** 构造一个请求帧（codec默认JDK，compress默认NONE） */
    public static RpcMessage request(long requestId, byte codec, Object payload) {
        return build(ProtocolConstants.TYPE_REQUEST, codec, requestId, payload);
    }

    /** 构造一个响应帧 */
    public static RpcMessage response(long requestId, byte codec, Object payload) {
        return build(ProtocolConstants.TYPE_RESPONSE, codec, requestId, payload);
    }

    /** 构造一个心跳帧（无body） */
    public static RpcMessage heartbeat(long requestId, byte codec) {
        return build(ProtocolConstants.TYPE_HEARTBEAT, codec, requestId, null);
    }

    private static RpcMessage build(byte type, byte codec, long requestId, Object payload) {
        RpcMessage m = new RpcMessage();
        m.type = type;
        m.codec = codec;
        m.compress = ProtocolConstants.COMPRESS_NONE;
        m.requestId = requestId;
        m.payload = payload;
        return m;
    }

    public boolean isHeartbeat() {
        return type == ProtocolConstants.TYPE_HEARTBEAT;
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
                "type=" + type +
                ", codec=" + codec +
                ", compress=" + compress +
                ", requestId=" + requestId +
                ", payload=" + payload +
                '}';
    }
}
