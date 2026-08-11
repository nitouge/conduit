package io.github.nitouge.conduit.serialization;

import io.github.nitouge.conduit.common.spi.ExtensionLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 序列化器工厂：按codec id/名称取 {@link Serializer}，统一委托给 {@link ExtensionLoader}（SPI）。
 *
 * <p>编解码层（conduit-protocol的RpcEncoder/RpcDecoder）通过本工厂取序列化器，<b>绝不new具体实现</b>
 * ——这是序列化可替换在字节层的落点：换序列化只改 {@code META-INF/rpc/...Serializer} 配置与依赖。</p>
 */
public final class SerializerFactory {

    private static final Logger log = LoggerFactory.getLogger(SerializerFactory.class);

    private SerializerFactory() {
    }

    /** 按协议头codec id取序列化器 */
    public static Serializer getByCodec(byte codec) {
        String name = SerializerType.nameOf(codec);
        log.debug("[SerializerFactory] codec={} -> name={}", codec, name);
        return getByName(name);
    }

    /** 按扩展名（jdk/json/kryo/protostuff）取序列化器 */
    public static Serializer getByName(String name) {
        Serializer serializer = ExtensionLoader.getExtensionLoader(Serializer.class).getExtension(name);
        log.debug("[SerializerFactory] 加载序列化器 name={} -> {}", name, serializer.getClass().getSimpleName());
        return serializer;
    }

    /** 默认序列化器（@SPI 指定，当前为jdk） */
    public static Serializer getDefault() {
        Serializer serializer = ExtensionLoader.getExtensionLoader(Serializer.class).getDefaultExtension();
        log.debug("[SerializerFactory] 加载默认序列化器 -> {}", serializer.getClass().getSimpleName());
        return serializer;
    }
}
