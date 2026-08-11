package io.github.nitouge.conduit.starter.demo.consumer;

import io.github.nitouge.conduit.api.annotation.RpcReference;
import io.github.nitouge.conduit.api.enums.ClusterPolicyEnum;
import io.github.nitouge.conduit.api.enums.LoadbalancePolicyEnum;
import io.github.nitouge.conduit.starter.demo.api.GreetService;
import org.springframework.stereotype.Component;

/**
 * 消费方。{@code @RpcReference} 由starter自动注入远程代理——调用它就像调本地方法，
 * 底层实际是Nacos发现provider地址 → Netty连接 → 序列化请求 → 远程执行 → 回传。
 */
@Component
public class GreetConsumer {

    @RpcReference(
            version = "1.0.0",
            group = "",
            timeout = 1000,
            cluster = ClusterPolicyEnum.FAIL_OVER,
            loadbalance = LoadbalancePolicyEnum.RANDOM
    )
    private GreetService greetService;

    public String greet(String name) {
        return greetService.greet(name);
    }
}
