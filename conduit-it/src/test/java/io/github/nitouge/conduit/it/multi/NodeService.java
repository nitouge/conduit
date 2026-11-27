package io.github.nitouge.conduit.it.multi;

/**
 * 多JVM用例的共享契约：provider/consumer跑在不同进程里，靠同一个接口名在注册中心相遇。
 */
public interface NodeService {

    /** 返回处理本次调用的节点标识，用来验证负载均衡与故障转移落到了哪个进程 */
    String whoAmI();

    /** 普通回声，验证请求-响应配对与序列化 */
    String echo(String message);
}
