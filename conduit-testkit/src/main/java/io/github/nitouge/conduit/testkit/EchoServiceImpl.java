package io.github.nitouge.conduit.testkit;

import java.util.concurrent.CompletableFuture;

/**
 * {@link EchoService} 的默认实现，带节点标识；每个测试进程/用例可以起多个不同标识的实例来区分节点。
 */
public class EchoServiceImpl implements EchoService {

    private final String nodeId;

    public EchoServiceImpl() {
        this("default");
    }

    public EchoServiceImpl(String nodeId) {
        this.nodeId = nodeId;
    }

    @Override
    public String echo(String message) {
        return "ok:" + message;
    }

    @Override
    public CompletableFuture<String> echoAsync(String message) {
        return CompletableFuture.completedFuture(echo(message));
    }

    @Override
    public String slow(long millis) {
        Await.sleepQuietly(millis);
        return "slow:" + millis;
    }

    @Override
    public String fail(String message) {
        throw new IllegalStateException("expected failure: " + message);
    }

    @Override
    public String nodeId() {
        return nodeId;
    }
}
