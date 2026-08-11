package io.github.nitouge.conduit.serialization.support;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.nitouge.conduit.api.RpcRequest;
import io.github.nitouge.conduit.serialization.SerializationException;
import io.github.nitouge.conduit.serialization.Serializer;

import java.io.IOException;

/**
 * 基于Jackson的JSON序列化器：可读、跨语言、免schema。
 *
 * <p><b>安全：刻意不开启Jackson default typing。</b> default typing（按JSON里的 {@code @class}
 * 反射实例化任意类型）在反序列化网络不可信字节时是经典的多态反序列化gadget RCE风险。
 * 这里改为：反序列化得到的 {@code Object[] args} 元素先按其声明的 {@code paramTypes} 用
 * {@code convertValue} 精确转型——既消除了任意类型实例化（只实例化方法声明的参数类型），又顺带
 * 修正了JSON number还原成Integer而非Long之类的类型失真。</p>
 *
 * <p>已知限制（留待后续硬化）：{@code RpcResponse.data} 不带返回类型信息，非String的返回值在JSON
 * 下可能类型失真；{@link Throwable} 难以经Jackson可靠往返。需要这些场景请用jdk/kryo序列化。
 * {@link ObjectMapper} 线程安全，单例复用。</p>
 */
public class JsonSerializer implements Serializer {

    private static final ObjectMapper MAPPER = new ObjectMapper()
            // 不认识的字段不报错，提升跨版本健壮性
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    @Override
    public byte[] serialize(Object obj) {
        try {
            return MAPPER.writeValueAsBytes(obj);
        } catch (IOException e) {
            throw new SerializationException("JSON serialize failed", e);
        }
    }

    @Override
    public <T> T deserialize(byte[] bytes, Class<T> clazz) {
        try {
            T value = MAPPER.readValue(bytes, clazz);
            // 请求体：用paramTypes把args元素精确转型（既安全又保真）
            if (value instanceof RpcRequest) {
                coerceArgs((RpcRequest) value);
            }
            return value;
        } catch (IOException e) {
            throw new SerializationException("JSON deserialize failed", e);
        }
    }

    /** 按paramTypes把每个arg转成声明类型；只实例化方法声明的参数类型，杜绝任意类型反射实例化。 */
    private void coerceArgs(RpcRequest request) {
        Object[] args = request.getArgs();
        Class<?>[] types = request.getParamTypes();
        if (args == null || types == null) {
            return;
        }
        int n = Math.min(args.length, types.length);
        for (int i = 0; i < n; i++) {
            if (args[i] != null) {
                args[i] = MAPPER.convertValue(args[i], types[i]);
            }
        }
    }
}
