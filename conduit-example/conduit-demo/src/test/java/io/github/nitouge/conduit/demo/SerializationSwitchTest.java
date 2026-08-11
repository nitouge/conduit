package io.github.nitouge.conduit.demo;

import io.github.nitouge.conduit.api.ServiceMeta;
import io.github.nitouge.conduit.client.RpcClient;
import io.github.nitouge.conduit.client.proxy.RpcClientProxy;
import io.github.nitouge.conduit.demo.api.HelloService;
import io.github.nitouge.conduit.demo.provider.HelloServiceImpl;
import io.github.nitouge.conduit.server.RpcServer;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * 仅更换客户端序列化方式（jdk/kryo/json），业务代码与服务端零改动，端到端调用照常成功。
 *
 * <p>这证明序列化可替换真正落地：序列化经协议头codec + SPI（ExtensionLoader）选定，
 * 收发两端各自按codec取对应Serializer，调用链对具体实现一无所知。</p>
 */
class SerializationSwitchTest {

    private static final String HOST = "127.0.0.1";

    static Stream<Arguments> serializers() {
        return Stream.of(
                Arguments.of("jdk", 20894),
                Arguments.of("kryo", 20895),
                Arguments.of("json", 20896));
    }

    @ParameterizedTest(name = "serialization={0}")
    @MethodSource("serializers")
    void callWorksWithSerializer(String serialization, int port) {
        RpcServer server = new RpcServer(HOST, port);
        server.export(HelloService.class, new HelloServiceImpl());
        server.start();

        // 仅这一行决定序列化方式（由application.yml注入）；其余代码与默认jdk完全一致
        RpcClient client = new RpcClient(serialization);
        try {
            ServiceMeta target = new ServiceMeta(HelloService.class.getName(), HOST, port);
            HelloService hello = RpcClientProxy.create(HelloService.class, target, client, 3000);

            assertEquals("Hello, Conduit from Conduit!", hello.sayHello("Conduit"));
        } finally {
            client.close();
            server.close();
        }
    }
}
