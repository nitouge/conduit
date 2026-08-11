package io.github.nitouge.conduit.demo.provider;

import io.github.nitouge.conduit.demo.api.HelloService;

import java.util.concurrent.CompletableFuture;

/**
 * 示例服务实现。
 */
public class HelloServiceImpl implements HelloService {

    @Override
    public String sayHello(String name) {
        System.out.println("收到远程调用: " + name);
        return "Hello, " + name + " from Conduit!";
    }

    @Override
    public CompletableFuture<String> sayHelloAsync(String name) {
        // 服务端返回future；RpcRequestHandler会在服务端解开后回传真实结果
        return CompletableFuture.completedFuture(sayHello(name));
    }
}
