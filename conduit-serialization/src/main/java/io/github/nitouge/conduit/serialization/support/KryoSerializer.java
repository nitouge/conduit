package io.github.nitouge.conduit.serialization.support;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import io.github.nitouge.conduit.serialization.SerializationException;
import io.github.nitouge.conduit.serialization.Serializer;

import java.io.ByteArrayOutputStream;

/**
 * 基于Kryo的高性能二进制序列化器（生产推荐）。速度快、产物小。
 *
 * <p>Kryo实例非线程安全，用 {@link ThreadLocal} 每线程一个实例规避并发问题。</p>
 */
public class KryoSerializer implements Serializer {

    private static final ThreadLocal<Kryo> KRYO_LOCAL = ThreadLocal.withInitial(() -> {
        Kryo kryo = new Kryo();
        kryo.setRegistrationRequired(false); // 简单起见不强制预注册类
        kryo.setReferences(true);            // 支持循环引用
        return kryo;
    });

    @Override
    public byte[] serialize(Object obj) {
        Kryo kryo = KRYO_LOCAL.get();
        // 用new Output(bos)包一个ByteArrayOutputStream而非new Output(初始容量)：
        // 后者写满会按固定上限抛BufferOverflow，前者底层流可自动扩容，对大小未知的对象更稳妥。
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
             Output output = new Output(bos)) {
            kryo.writeObject(output, obj);
            output.flush();
            return bos.toByteArray();
        } catch (Exception e) {
            throw new SerializationException("Kryo serialize failed", e);
        }
    }

    @Override
    public <T> T deserialize(byte[] bytes, Class<T> clazz) {
        Kryo kryo = KRYO_LOCAL.get();
        try (Input input = new Input(bytes)) {
            return kryo.readObject(input, clazz);
        } catch (Exception e) {
            throw new SerializationException("Kryo deserialize failed", e);
        }
    }
}
