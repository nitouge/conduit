package io.github.nitouge.conduit.starter.demo.provider;

import io.github.nitouge.conduit.api.annotation.RpcReference;
import io.github.nitouge.conduit.api.annotation.RpcService;
import io.github.nitouge.conduit.registry.Registry;
import io.github.nitouge.conduit.starter.demo.api.GreetService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * 快速、<b>无需Nacos</b> 的自动装配验证（内存注册中心local，provider+consumer同进程）：
 * 证明只引入starter（无 {@code @EnableConduit}）即把 {@code @RpcService} 自动暴露、
 * {@code @RpcReference} 自动注入，并走真实Netty完成一次调用。
 *
 * <p>这条用例进CI跑（不依赖外部组件）；两个真实进程经Nacos的联调见模块README，用curl验证。</p>
 */
@SpringBootTest(
        classes = StarterAutoConfigInProcessTest.InProcApp.class,
        webEnvironment = SpringBootTest.WebEnvironment.NONE,
        properties = {
                "conduit.registry.type=local",
                "conduit.registry.address=provider-inproc-jvm",
                "conduit.protocol.host=127.0.0.1",
                "conduit.protocol.port=20899",
                "conduit.protocol.serialization=kryo"
        })
class StarterAutoConfigInProcessTest {

    @Autowired
    private Caller caller;

    @Autowired
    private Registry registry;

    @Test
    void autoConfiguredWithoutEnableConduit() {
        assertEquals("Hi, InProc!", caller.call("InProc"));
    }

    @Test
    void serviceIsExportedThroughAutoConfiguredRegistry() {
        assertEquals(1, registry.discover(GreetService.class.getName()).size(),
                "业务接口应已由starter自动注册");
    }

    /** 注意：用@SpringBootConfiguration + @EnableAutoConfiguration触发starter自动装配，且不扫描生产包。 */
    @SpringBootConfiguration
    @EnableAutoConfiguration
    static class InProcApp {
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
    static class Impl implements GreetService {
        @Override
        public String greet(String name) {
            return "Hi, " + name + "!";
        }
    }

    static class Caller {
        @RpcReference
        private GreetService greetService;

        String call(String name) {
            return greetService.greet(name);
        }
    }
}
