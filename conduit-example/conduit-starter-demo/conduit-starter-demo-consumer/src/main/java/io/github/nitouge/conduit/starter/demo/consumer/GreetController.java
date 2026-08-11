package io.github.nitouge.conduit.starter.demo.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * REST入口：{@code GET /greet?name=Xxx}。返回里同时带上<b>本consumer进程的pid</b> 和
 * <b>provider进程算出的消息（含provider的pid）</b>——两个pid不同，即可直观证明这是跨进程调用。
 */
@RestController
public class GreetController {

    private static final Logger log = LoggerFactory.getLogger(GreetController.class);

    private final GreetConsumer consumer;

    public GreetController(GreetConsumer consumer) {
        this.consumer = consumer;
    }

    @GetMapping("/greet")
    public String greet(@RequestParam(name = "name", defaultValue = "World") String name) {
        long pid = ProcessHandle.current().pid();
        log.info("收到HTTP /greet?name={}，发起远程RPC ...（本consumer进程 pid={}）", name, pid);
        String remote = consumer.greet(name);
        return String.format("[consumer进程pid=%d] 远程返回 → %s%n", pid, remote);
    }
}
