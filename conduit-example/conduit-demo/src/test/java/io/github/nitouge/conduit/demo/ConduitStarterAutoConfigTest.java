package io.github.nitouge.conduit.demo;

import io.github.nitouge.conduit.api.annotation.RpcReference;
import io.github.nitouge.conduit.api.annotation.RpcService;
import io.github.nitouge.conduit.demo.api.HelloService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;

import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * 验证真正的starter：应用<b>没有</b> {@code @EnableConduit}，仅靠classpath上conduit-spring的
 * {@code META-INF/spring/...AutoConfiguration.imports}，被 {@code @EnableAutoConfiguration} 自动装配。
 *
 * <p>顺带把序列化设为kryo，证明配置确实经application.yml/properties注入并生效。</p>
 */
@SpringBootTest(
        classes = ConduitStarterAutoConfigTest.StarterApp.class,
        webEnvironment = SpringBootTest.WebEnvironment.NONE,
        properties = {
                "conduit.registry.type=local",
                "conduit.registry.address=starter-jvm",
                "conduit.protocol.host=127.0.0.1",
                "conduit.protocol.port=20906",
                "conduit.protocol.serialization=kryo"
        })
class ConduitStarterAutoConfigTest {

    @Autowired
    private Caller caller;

    @Test
    void autoConfiguredWithoutEnableConduit() {
        assertEquals("Hi, Starter!", caller.call("Starter"));
    }

    /** 注意：没有@EnableConduit，仅@EnableAutoConfiguration触发starter自动装配 */
    @SpringBootConfiguration
    @EnableAutoConfiguration
    static class StarterApp {
        @Bean
        Impl impl() {
            return new Impl();
        }

        @Bean
        Caller caller() {
            return new Caller();
        }
    }

    @RpcService
    public static class Impl implements HelloService {
        @Override
        public String sayHello(String name) {
            return "Hi, " + name + "!";
        }

        @Override
        public CompletableFuture<String> sayHelloAsync(String name) {
            return CompletableFuture.completedFuture(sayHello(name));
        }
    }

    public static class Caller {
        @RpcReference
        private HelloService helloService;

        public String call(String name) {
            return helloService.sayHello(name);
        }
    }
}
