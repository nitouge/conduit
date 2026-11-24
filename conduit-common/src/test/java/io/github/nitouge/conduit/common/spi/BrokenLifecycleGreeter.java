package io.github.nitouge.conduit.common.spi;

/** init失败的测试扩展：用于验证失败后不缓存、下次可重试。 */
public class BrokenLifecycleGreeter implements Greeter, Lifecycle {

    @Override
    public void init() {
        throw new IllegalStateException("init boom");
    }

    @Override
    public String greet() {
        return "broken";
    }
}
