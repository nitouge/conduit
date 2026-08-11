package io.github.nitouge.conduit.serialization;

/**
 * 序列化类型的id ↔ name映射。
 *
 * <p>id会写进协议头的codec字段（1字节），与conduit-protocol的 {@code ProtocolConstants.CODEC_*}
 * 一一对应：1=jdk, 2=json, 3=kryo, 4=protostuff。</p>
 */
public enum SerializerType {

    JDK((byte) 1, "jdk"),
    JSON((byte) 2, "json"),
    KRYO((byte) 3, "kryo"),
    PROTOSTUFF((byte) 4, "protostuff");

    private final byte id;
    private final String name;

    SerializerType(byte id, String name) {
        this.id = id;
        this.name = name;
    }

    public byte getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    /** 按id查类型 */
    public static SerializerType of(byte id) {
        for (SerializerType t : values()) {
            if (t.id == id) {
                return t;
            }
        }
        throw new SerializationException("未知的序列化codec id: " + id);
    }

    /** 按name查类型 */
    public static SerializerType of(String name) {
        for (SerializerType t : values()) {
            if (t.name.equals(name)) {
                return t;
            }
        }
        throw new SerializationException("未知的序列化名称: " + name);
    }

    /** name -> id */
    public static byte idOf(String name) {
        return of(name).id;
    }

    /** id -> name */
    public static String nameOf(byte id) {
        return of(id).name;
    }
}
