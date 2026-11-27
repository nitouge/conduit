package io.github.nitouge.conduit.serialization;

import io.github.nitouge.conduit.api.RpcRequest;
import io.github.nitouge.conduit.api.RpcResponse;
import io.github.nitouge.conduit.common.spi.ExtensionLoader;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * 序列化边界用例（协议编解码与集群重试都会放大这些问题）：
 * 空参数、null实参、数组实参、attachments，以及大报文。
 *
 * <p>Protostuff 对 {@code RpcRequest}（含 {@code Class[]}/{@code Object[]}）不做保证，
 * 因此这里只覆盖 jdk/json/kryo；Protostuff 的具体POJO场景见 {@link SerializerSpiTest}。</p>
 */
class SerializerEdgeCaseTest {

    private static Serializer serializer(String name) {
        return ExtensionLoader.getExtensionLoader(Serializer.class).getExtension(name);
    }

    /** 空参数数组：方法无参调用 */
    @ParameterizedTest
    @ValueSource(strings = {"jdk", "json", "kryo"})
    void roundTripsEmptyArguments(String name) {
        Serializer s = serializer(name);
        RpcRequest request = request(new Class<?>[0], new Object[0]);

        RpcRequest back = s.deserialize(s.serialize(request), RpcRequest.class);

        assertEquals("noArgs", back.getMethodName());
        assertEquals(0, back.getArgs().length);
        assertEquals(0, back.getParamTypes().length);
    }

    /** null实参：常见于"可选参数"调用 */
    @ParameterizedTest
    @ValueSource(strings = {"jdk", "json", "kryo"})
    void roundTripsNullArgument(String name) {
        Serializer s = serializer(name);
        RpcRequest request = request(new Class<?>[]{String.class}, new Object[]{null});

        RpcRequest back = s.deserialize(s.serialize(request), RpcRequest.class);

        assertEquals(1, back.getArgs().length);
        assertNull(back.getArgs()[0]);
    }

    /** 数组 + 多参数：验证参数类型/顺序不错位 */
    @ParameterizedTest
    @ValueSource(strings = {"jdk", "json", "kryo"})
    void roundTripsArrayAndMultipleArguments(String name) {
        Serializer s = serializer(name);
        String[] tags = {"a", "b", "c"};
        RpcRequest request = request(new Class<?>[]{String[].class, int.class}, new Object[]{tags, 7});

        RpcRequest back = s.deserialize(s.serialize(request), RpcRequest.class);

        assertArrayEquals(tags, (String[]) back.getArgs()[0], name + " 应保真数组实参");
        assertEquals(7, ((Number) back.getArgs()[1]).intValue());
    }

    /** attachments（traceId等）必须跨序列化保留 */
    @ParameterizedTest
    @ValueSource(strings = {"jdk", "json", "kryo"})
    void roundTripsAttachments(String name) {
        Serializer s = serializer(name);
        RpcRequest request = request(new Class<?>[]{String.class}, new Object[]{"x"});
        Map<String, String> attachments = new HashMap<>();
        attachments.put("traceId", "0123456789abcdef0123456789abcdef");
        attachments.put("tenant", "t-1");
        request.setAttachments(attachments);

        RpcRequest back = s.deserialize(s.serialize(request), RpcRequest.class);

        assertEquals("0123456789abcdef0123456789abcdef", back.getAttachment("traceId"));
        assertEquals("t-1", back.getAttachment("tenant"));
    }

    /** 大报文（64KB）：序列化不应截断/丢字节 */
    @ParameterizedTest
    @ValueSource(strings = {"jdk", "json", "kryo"})
    void roundTripsLargePayload(String name) {
        Serializer s = serializer(name);
        char[] chars = new char[64 * 1024];
        Arrays.fill(chars, 'x');
        String large = new String(chars);

        RpcResponse response = RpcResponse.success(large);
        byte[] bytes = s.serialize(response);

        RpcResponse back = s.deserialize(bytes, RpcResponse.class);
        assertEquals(large, back.getData());
        assertEquals(large.length(), ((String) back.getData()).length());
    }

    private static RpcRequest request(Class<?>[] paramTypes, Object[] args) {
        RpcRequest request = new RpcRequest();
        request.setInterfaceName("io.github.nitouge.conduit.test.EchoService");
        request.setMethodName(paramTypes.length == 0 ? "noArgs" : "someMethod");
        request.setParamTypes(paramTypes);
        request.setArgs(args);
        return request;
    }
}
