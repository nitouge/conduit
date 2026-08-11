package io.github.nitouge.conduit.common.filter.builtin;

import io.github.nitouge.conduit.api.RpcRequest;
import io.github.nitouge.conduit.api.RpcResponse;
import io.github.nitouge.conduit.common.filter.Filter;
import io.github.nitouge.conduit.common.filter.FilterChain;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;

/**
 * 访问日志Filter：记录每次调用的接口/方法/耗时/成功与否。
 *
 * <p>双侧生效、默认启用——安全零副作用、安静记录（INFO级别）。
 * 用 {@code -Dconduit.filter.accesslog.enabled=false} 关闭。</p>
 */
public class AccessLogFilter implements Filter {

    private static final Logger log = LoggerFactory.getLogger("CONDUIT.ACCESS");

    @Override
    public CompletableFuture<RpcResponse> invoke(RpcRequest request, FilterChain chain) {
        long start = System.currentTimeMillis();
        String method = request.getInterfaceName() + "#" + request.getMethodName();

        return chain.invoke(request).whenComplete((resp, err) -> {
            long elapsed = System.currentTimeMillis() - start;
            boolean success = (err == null && (resp == null || !resp.hasException()));
            log.info("[AccessLog] {} - {}ms - {}", method, elapsed, success ? "OK" : "FAIL");
        });
    }

    @Override
    public int order() {
        return -100; // 外层：最先进、最后出，记全程耗时
    }

    @Override
    public String name() {
        return "accesslog";
    }
}
