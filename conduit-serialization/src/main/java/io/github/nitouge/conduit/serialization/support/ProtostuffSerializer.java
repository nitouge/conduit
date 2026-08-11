package io.github.nitouge.conduit.serialization.support;

import io.github.nitouge.conduit.api.RpcRequest;
import io.github.nitouge.conduit.serialization.SerializationException;
import io.github.nitouge.conduit.serialization.Serializer;
import io.protostuff.LinkedBuffer;
import io.protostuff.ProtostuffIOUtil;
import io.protostuff.Schema;
import io.protostuff.runtime.RuntimeSchema;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 基于Protostuff的序列化器：protobuf编码、免写.proto（运行时按POJO反射生成schema）。
 *
 * <p>Schema按类缓存（线程安全）；{@link LinkedBuffer} 不可共享，用 {@link ThreadLocal} 复用并在用后clear。</p>
 *
 * <p>注意：Protostuff最适合具体POJO。对携带 {@code Class<?>[]} / 复杂 {@code Object[]} 的通用
 * {@link RpcRequest}，jdk/kryo/json 更稳妥；本实现主要用于具体业务对象。</p>
 */
public class ProtostuffSerializer implements Serializer {

    private static final Map<Class<?>, Schema<?>> SCHEMA_CACHE = new ConcurrentHashMap<>();
    private static final ThreadLocal<LinkedBuffer> BUFFER_LOCAL =
            ThreadLocal.withInitial(() -> LinkedBuffer.allocate(512));

    @SuppressWarnings("unchecked")
    private <T> Schema<T> schemaOf(Class<T> clazz) {
        return (Schema<T>) SCHEMA_CACHE.computeIfAbsent(clazz, RuntimeSchema::getSchema);
    }

    @SuppressWarnings("unchecked")
    @Override
    public byte[] serialize(Object obj) {
        if (obj == null) {
            throw new SerializationException("Protostuff不支持序列化null（需具体POJO以反射生成schema）");
        }
        Class<Object> clazz = (Class<Object>) obj.getClass();
        Schema<Object> schema = schemaOf(clazz);
        LinkedBuffer buffer = BUFFER_LOCAL.get();
        try {
            return ProtostuffIOUtil.toByteArray(obj, schema, buffer);
        } catch (Exception e) {
            throw new SerializationException("Protostuff serialize failed", e);
        } finally {
            buffer.clear();
        }
    }

    @Override
    public <T> T deserialize(byte[] bytes, Class<T> clazz) {
        try {
            Schema<T> schema = schemaOf(clazz);
            T message = schema.newMessage();
            ProtostuffIOUtil.mergeFrom(bytes, message, schema);
            return message;
        } catch (Exception e) {
            throw new SerializationException("Protostuff deserialize failed", e);
        }
    }
}
