package io.github.nitouge.conduit.benchmark;

/**
 * 基准用的服务接口：一个字典型调用（小报文）+ 一个对象型调用（较大报文）。
 */
public interface BenchService {

    /** 小报文往返：测量框架本身的开销（协议头、线程切换、请求响应关联） */
    String echo(String message);

    /** 对象往返：把序列化/反序列化也纳入端到端耗时 */
    BenchPayload roundTrip(BenchPayload payload);
}
