package io.github.nitouge.conduit.starter.demo.api;

/**
 * 示例服务接口：provider暴露、consumer引用。
 *
 * <p>RPC按接口全限定名注册与发现，因此provider/consumer两个进程必须依赖同一份api（同名同包）。</p>
 */
public interface GreetService {

    /** 同步问候 */
    String greet(String name);
}
