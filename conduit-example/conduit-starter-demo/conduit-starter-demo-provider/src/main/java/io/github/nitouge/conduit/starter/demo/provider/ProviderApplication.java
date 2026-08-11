package io.github.nitouge.conduit.starter.demo.provider;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * 服务提供方进程入口。
 *
 * <p>只引入conduit-spring-boot-starter，没有 {@code @EnableConduit}：starter自动装配后，
 * {@link GreetServiceImpl} 上的 {@code @RpcService} 会被自动暴露并注册到Nacos（见application.yml），
 * 同时启动Netty监听RPC端口。Netty线程为非守护线程，进程会持续存活直到优雅停机。</p>
 */
@SpringBootApplication
public class ProviderApplication {

    public static void main(String[] args) {
        SpringApplication.run(ProviderApplication.class, args);
    }
}
