package io.github.nitouge.conduit.benchmark;

/**
 * {@link BenchService} 的实现：只做最小工作，让基准测的是框架开销而不是业务逻辑。
 */
public class BenchServiceImpl implements BenchService {

    @Override
    public String echo(String message) {
        return message;
    }

    @Override
    public BenchPayload roundTrip(BenchPayload payload) {
        return payload;
    }
}
