package io.github.nitouge.conduit.serialization;

import io.github.nitouge.conduit.api.RpcRequest;
import io.github.nitouge.conduit.api.RpcResponse;
import io.github.nitouge.conduit.common.spi.SPI;

/**
 * 序列化器：负责 {@link RpcRequest} 和 {{@link RpcResponse} 的body编解码。
 *
 * <p>默认扩展为"jdk"：M1阶段用JDK序列化最快跑通端到端；
 * 生产推荐改为"kryo"（速度快、体积小）。</p>
 *
 * <p>实现要求：必须线程安全（框架会被多线程并发调用）。若底层库本身非线程安全
 * （如Kryo），实现内部用ThreadLocal或对象池保证安全。</p>
 */
@SPI("jdk")
public interface Serializer {

    /**
     * 把对象序列化为字节数组。
     *
     * @param obj 通常是RpcRequest或RpcResponse
     */
    byte[] serialize(Object obj);

    /**
     * 把字节数组反序列化为指定类型的对象。
     *
     * @param bytes 字节数组（来自协议体）
     * @param clazz 目标类型（接收端根据协议头type字段决定传RpcRequest.class还是RpcResponse.class）
     */
    <T> T deserialize(byte[] bytes, Class<T> clazz);
}
