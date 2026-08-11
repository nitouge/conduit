package io.github.nitouge.conduit.demo;

import io.github.nitouge.conduit.api.annotation.RpcReference;
import io.github.nitouge.conduit.api.annotation.RpcService;
import io.github.nitouge.conduit.demo.api.HelloService;
import io.github.nitouge.conduit.registry.Registry;
import io.github.nitouge.conduit.spring.EnableConduit;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.Serial;
import java.io.Serializable;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 端到端：纯注解驱动。@RpcService自动暴露、@RpcReference自动注入，在同一个Spring容器内
 * provider + consumer走真实Netty跑通——用in-JVM的local注册中心，无需Nacos。
 *
 * <p>同时修复：1.@RpcReference定义在父类仍被注入；2.实现类附带的Serializable等非业务接口不被暴露。</p>
 */
@SpringBootTest(
        classes = ConduitSpringTest.TestApp.class,
        webEnvironment = SpringBootTest.WebEnvironment.NONE,
        properties = {
                "conduit.registry.type=local",
                "conduit.registry.address=test-jvm",
                "conduit.protocol.host=127.0.0.1",
                "conduit.protocol.port=20905",
                "conduit.protocol.serialization=jdk"
        })
class ConduitSpringTest {

    @Autowired
    private GreetingClient greetingClient;

    @Autowired
    private Registry registry;

    @Test
    void annotationDrivenExportAndInjectWorks() {
        // greetingClient的@RpcReference字段定义在父类BaseConsumer上，验证继承字段也被注入
        assertEquals("Hello, Spring from Conduit!", greetingClient.greet("Spring"));
    }

    @Test
    void onlyBusinessInterfaceIsExported() {
        // GreetingProvider还实现了Serializable，但不应被暴露/注册到java.io.Serializable名下
        assertTrue(registry.discover("java.io.Serializable").isEmpty(), "标记接口不应被暴露");
        assertEquals(1, registry.discover(HelloService.class.getName()).size(), "业务接口应已注册");
    }

    @EnableConduit
    @Configuration
    static class TestApp {
        @Bean
        GreetingProvider greetingProvider() {
            return new GreetingProvider();
        }

        @Bean
        GreetingClient greetingClient() {
            return new GreetingClient();
        }
    }

    /** {@link @RpcService} 自动暴露的服务实现（额外实现Serializable以验证只暴露业务接口） */
    @RpcService
    public static class GreetingProvider implements HelloService, Serializable {
        @Serial
        private static final long serialVersionUID = 1L;

        @Override
        public String sayHello(String name) {
            return "Hello, " + name + " from Conduit!";
        }

        @Override
        public CompletableFuture<String> sayHelloAsync(String name) {
            return CompletableFuture.completedFuture(sayHello(name));
        }
    }

    /** {@link @RpcReference} 定义在父类，验证继承字段注入 */
    public static class BaseConsumer {
        @RpcReference
        protected HelloService helloService;

        public String greet(String name) {
            return helloService.sayHello(name);
        }
    }

    public static class GreetingClient extends BaseConsumer {
    }
}
