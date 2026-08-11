package io.github.nitouge.conduit.serialization;
import io.github.nitouge.conduit.serialization.support.JdkSerializer;
import io.github.nitouge.conduit.serialization.support.JsonSerializer;
import io.github.nitouge.conduit.serialization.support.KryoSerializer;
import io.github.nitouge.conduit.serialization.support.ProtostuffSerializer;

import io.github.nitouge.conduit.api.RpcRequest;
import io.github.nitouge.conduit.common.spi.ExtensionLoader;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

class SerializerSpiTest {

    @Test
    void allFourSerializersLoadViaSpi() {
        ExtensionLoader<Serializer> loader = ExtensionLoader.getExtensionLoader(Serializer.class);
        assertInstanceOf(JdkSerializer.class, loader.getExtension("jdk"));
        assertInstanceOf(JsonSerializer.class, loader.getExtension("json"));
        assertInstanceOf(KryoSerializer.class, loader.getExtension("kryo"));
        assertInstanceOf(ProtostuffSerializer.class, loader.getExtension("protostuff"));
        // @SPI("jdk") -> 默认是jdk
        assertInstanceOf(JdkSerializer.class, loader.getDefaultExtension());
    }

    /** jdk/kryo/json都能完整往返携带Class[]/Object[]的RpcRequest */
    @ParameterizedTest
    @ValueSource(strings = {"jdk", "kryo", "json"})
    void roundTripRpcRequest(String name) {
        Serializer s = ExtensionLoader.getExtensionLoader(Serializer.class).getExtension(name);

        RpcRequest req = new RpcRequest();
        req.setInterfaceName("com.demo.HelloService");
        req.setMethodName("sayHello");
        req.setParamTypes(new Class<?>[]{String.class});
        req.setArgs(new Object[]{"Conduit"});
        req.setVersion("1.0.0");

        byte[] bytes = s.serialize(req);
        RpcRequest back = s.deserialize(bytes, RpcRequest.class);

        assertEquals("com.demo.HelloService", back.getInterfaceName());
        assertEquals("sayHello", back.getMethodName());
        assertEquals(String.class, back.getParamTypes()[0]);
        assertEquals("Conduit", back.getArgs()[0]);
        assertEquals("1.0.0", back.getVersion());
    }

    /**
     * 对抗性审查修复回归：JSON序列化器 (1) 不再开启default typing —— 不写 {@code @class} 类型标签，
     * 杜绝多态反序列化gadget RCE；(2) 按paramTypes精确转型args —— long参数还原为Long而非Integer。
     */
    @Test
    void jsonNoDefaultTypingAndCoercesArgsByParamTypes() {
        Serializer json = ExtensionLoader.getExtensionLoader(Serializer.class).getExtension("json");

        RpcRequest req = new RpcRequest();
        req.setInterfaceName("com.demo.Calc");
        req.setMethodName("add");
        req.setParamTypes(new Class<?>[]{long.class, String.class});
        req.setArgs(new Object[]{5L, "x"});

        byte[] bytes = json.serialize(req);
        // 安全：不再写入default-typing的类型标签（@class）
        assertFalse(new String(bytes, StandardCharsets.UTF_8).contains("@class"),
                "JSON不应再使用default typing写入@class类型标签");

        RpcRequest back = json.deserialize(bytes, RpcRequest.class);
        // 保真：long参数经paramTypes转型应为Long（而非JSON默认的Integer）
        assertInstanceOf(Long.class, back.getArgs()[0]);
        assertEquals(5L, back.getArgs()[0]);
        assertEquals("x", back.getArgs()[1]);
    }

    /** Protostuff最适合具体POJO（无Class[]/Object[]）；单独用一个普通对象验证其可用 */
    @Test
    void protostuffRoundTripsConcretePojo() {
        Serializer s = ExtensionLoader.getExtensionLoader(Serializer.class).getExtension("protostuff");

        UserDto u = new UserDto(7L, "Neo");
        byte[] bytes = s.serialize(u);
        UserDto back = s.deserialize(bytes, UserDto.class);

        assertEquals(7L, back.getId());
        assertEquals("Neo", back.getName());
    }

    public static class UserDto {
        private long id;
        private String name;

        public UserDto() {
        }

        public UserDto(long id, String name) {
            this.id = id;
            this.name = name;
        }

        public long getId() {
            return id;
        }

        public String getName() {
            return name;
        }
    }
}
