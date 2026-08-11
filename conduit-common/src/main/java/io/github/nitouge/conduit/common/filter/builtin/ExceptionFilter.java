package io.github.nitouge.conduit.common.filter.builtin;

import io.github.nitouge.conduit.api.RpcRequest;
import io.github.nitouge.conduit.api.RpcResponse;
import io.github.nitouge.conduit.common.exception.RpcException;
import io.github.nitouge.conduit.common.filter.Filter;
import io.github.nitouge.conduit.common.filter.FilterChain;
import io.github.nitouge.conduit.common.filter.Side;

import java.util.concurrent.CompletableFuture;

/**
 * 异常规整Filter（提供侧）：把消费侧可能反序列化失败的业务异常降级成一个可安全传输的
 * {@link RpcException}，避免提供侧特有的异常类型让消费侧解不开。
 *
 * <p>规则参照Dubbo的 {@link ExceptionFilter}——只对<b>可能有问题</b>的异常降级，其余原样透传：</p>
 * <ul>
 *   <li>{@link RpcException}（框架异常）→ 原样：本就在框架公共依赖里，消费侧一定能反序列化。</li>
 *   <li>JDK自带异常（{@code java.*}/{@code javax.*}）→ 原样：消费侧一定有这些类。</li>
 *   <li>异常类与接口在同一jar（同包前缀）→ 原样：消费侧引了接口jar就有这个异常。</li>
 *   <li>其余（provider私有异常类）→ 降级成 {@code RpcException(类名 + ": " + message)}，
 *       保留这是一次失败的语义与可读信息，但不再依赖消费侧持有该异常类。</li>
 * </ul>
 *
 * <p>默认启用；用 {@code -Dconduit.filter.exception.enabled=false} 关闭。
 * order=100（内层，最靠近反射终端）：在业务异常刚产生、还没经过其它filter时就规整。</p>
 */
public class ExceptionFilter implements Filter {

    @Override
    public CompletableFuture<RpcResponse> invoke(RpcRequest request, FilterChain chain) {
        return chain.invoke(request).thenApply(resp -> sanitize(request, resp));
    }

    @Override
    public int order() {
        return 100; // 内层：最靠近终端，业务异常刚产生就规整
    }

    @Override
    public boolean appliesTo(Side side) {
        return side == Side.PROVIDER;
    }

    @Override
    public String name() {
        return "exception";
    }

    private RpcResponse sanitize(RpcRequest request, RpcResponse response) {
        if (response == null || !response.hasException()) {
            return response;
        }
        Throwable e = response.getException();
        if (!needsDegrade(request, e)) {
            return response;
        }
        // 降级：保留异常语义，但换成消费侧一定能反序列化的RpcException
        RpcException safe = new RpcException(e.getClass().getName() + ": " + e.getMessage());
        safe.setStackTrace(e.getStackTrace());
        return RpcResponse.error(safe);
    }

    /** 判断异常是否需要降级（消费侧可能反序列化不了）。 */
    private boolean needsDegrade(RpcRequest request, Throwable e) {
        if (e instanceof RpcException) {
            return false; // 框架异常，公共依赖里有
        }
        String exClassName = e.getClass().getName();
        // JDK/标准库异常：消费侧一定有
        if (exClassName.startsWith("java.") || exClassName.startsWith("javax.")) {
            return false;
        }
        // 与接口同jar（同顶层包前缀）：消费侧引了接口jar就有
        String interfaceName = request.getInterfaceName();
        return interfaceName == null || !samePackagePrefix(exClassName, interfaceName);
    }

    /** 两个全限定名是否共享至少两级包前缀（粗略判定同一jar/同一模块）。 */
    private boolean samePackagePrefix(String a, String b) {
        String pa = topPackages(a, 2);
        String pb = topPackages(b, 2);
        return !pa.isEmpty() && pa.equals(pb);
    }

    private String topPackages(String fqcn, int levels) {
        int idx = 0;
        for (int i = 0; i < levels; i++) {
            int next = fqcn.indexOf('.', idx);
            if (next < 0) {
                return fqcn.substring(0, idx > 0 ? idx - 1 : 0);
            }
            idx = next + 1;
        }
        return fqcn.substring(0, idx - 1);
    }
}
