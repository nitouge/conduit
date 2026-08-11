package io.github.nitouge.conduit.demo.api;

import java.util.concurrent.CompletableFuture;

/**
 * 示例服务接口：provider与consumer共享。
 */
public interface HelloService {

    /** 同步方法 */
    String sayHello(String name);

    /** 异步方法：返回CompletableFuture，消费者不阻塞（M4异步演示） */
    CompletableFuture<String> sayHelloAsync(String name);
}
