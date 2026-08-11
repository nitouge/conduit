package io.github.nitouge.conduit.starter.demo.consumer;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * 服务消费方进程入口（Web应用）。
 *
 * <p>只引入conduit-spring-boot-starter，没有 {@code @EnableConduit}：starter自动装配后，
 * {@link GreetConsumer} 里的 {@code @RpcReference} 字段会被注入一个经Nacos发现provider的远程代理。
 * 对外用 {@link GreetController} 暴露HTTP接口，方便curl验证跨进程调用。</p>
 */
@SpringBootApplication
public class ConsumerApplication {

    public static void main(String[] args) {
        SpringApplication.run(ConsumerApplication.class, args);
    }
}
