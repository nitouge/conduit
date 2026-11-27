package io.github.nitouge.conduit.testkit;

import java.util.concurrent.CompletableFuture;

/**
 * 测试用服务接口：覆盖同步、异步、慢调用、异常、节点标识五种典型场景。
 *
 * <p>接口定义在testkit里，各模块的测试共享同一份契约，避免每个测试各造一个EchoService。</p>
 */
public interface EchoService {

    /** 同步回声：返回"ok:" + message */
    String echo(String message);

    /** 异步回声：provider端返回CompletableFuture，消费端拿到future */
    CompletableFuture<String> echoAsync(String message);

    /** 慢调用：睡指定毫秒后返回，用于超时/异步验证 */
    String slow(long millis);

    /** 始终抛异常，用于错误传播/异常规整验证 */
    String fail(String message);

    /** 返回本节点的标识，用于验证负载均衡分流到哪个节点 */
    String nodeId();
}
