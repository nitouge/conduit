package io.github.nitouge.conduit.starter.demo.provider;

import io.github.nitouge.conduit.api.annotation.RpcService;
import io.github.nitouge.conduit.starter.demo.api.GreetService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * 服务实现。返回的消息里特意带上<b>本进程的pid与RPC端口</b>——这样consumer拿到的字符串
 * 一定是另一个进程算出来的，跨进程调用一眼可证（consumer的pid与此不同）。
 */
@RpcService(version = "1.0.1", group = "", weight = 10)
@Component
public class GreetServiceImpl implements GreetService {

    private static final Logger log = LoggerFactory.getLogger(GreetServiceImpl.class);

    @Value("${conduit.protocol.port:0}")
    private int rpcPort;

    @Override
    public String greet(String name) {
        long pid = ProcessHandle.current().pid();
        log.info("收到远程调用greet(\"{}\")  ← 本provider进程 pid={}, rpcPort={}", name, pid, rpcPort);
        return String.format("Hi, %s! —— 来自provider进程(pid=%d, rpcPort=%d)", name, pid, rpcPort);
    }
}
