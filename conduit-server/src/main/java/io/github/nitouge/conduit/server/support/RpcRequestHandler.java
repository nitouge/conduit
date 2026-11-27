package io.github.nitouge.conduit.server.support;

import io.github.nitouge.conduit.api.RpcContext;
import io.github.nitouge.conduit.api.RpcRequest;
import io.github.nitouge.conduit.api.RpcResponse;
import io.github.nitouge.conduit.common.exception.RpcException;
import io.github.nitouge.conduit.common.filter.Filter;
import io.github.nitouge.conduit.common.filter.FilterChain;
import io.github.nitouge.conduit.common.filter.FilterChainBuilder;
import io.github.nitouge.conduit.common.filter.FilterLoader;
import io.github.nitouge.conduit.common.filter.Side;
import io.github.nitouge.conduit.common.trace.TraceContext;
import io.github.nitouge.conduit.remoting.RequestHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

/**
 * 服务端核心请求处理：从服务缓存取实现对象，反射调用目标方法，组装 {@link RpcResponse}。
 *
 * <p>返回 {@link CompletableFuture}：同步方法返回已完成的future；若服务方法本身返回
 * {@link CompletableFuture}（provider异步），则把它<b>映射</b>过来而不是阻塞 {@code get()}——
 * 业务线程提交完即释放，响应在该future完成时由传输层回写（真正的provider端非阻塞异步）。</p>
 *
 * <p>运行在服务端业务线程池中；不接触协议头，requestId的读取与回填全在传输层完成。</p>
 *
 * <p><b>请求边界（Filter链 + 上下文还原）：</b></p>
 * <ol>
 *   <li>进入时把 {@link RpcRequest#getAttachments()}（traceId/spanId/幂等键/灰度标签等）还原到
 *       {@link RpcContext}，并把 trace/span 绑定到 MDC；</li>
 *   <li>经提供侧 {@link Filter} 链，链尾终端才反射调用业务方法；</li>
 *   <li>{@code finally} 清空线程本地状态，避免串到线程池里的下一个请求。</li>
 * </ol>
 *
 * <p>异步provider方法的continuation跑在另一线程，{@code ThreadLocal} 不自动过去——需要跨线程保留
 * 上下文时用 {@code ContextSnapshot} 显式传播（框架不替业务线程池做决定）。</p>
 */
public class RpcRequestHandler implements RequestHandler {

    private static final Logger log = LoggerFactory.getLogger(RpcRequestHandler.class);

    private final ServiceCache serviceCache;
    /** 提供侧filter链（按order排序、已过滤PROVIDER侧）；无filter时为空列表 */
    private final List<Filter> providerFilters;

    public RpcRequestHandler(ServiceCache serviceCache) {
        this.serviceCache = serviceCache;
        this.providerFilters = FilterLoader.load(Side.PROVIDER);
    }

    @Override
    public CompletableFuture<RpcResponse> handle(RpcRequest request) {
        // 故意用局部变量而不是实例字段：本 handler 是线程池共享的单例，实例字段会在并发请求间互相覆盖
        TraceContext.MdcSnapshot mdc = restoreContext(request);
        try {
            FilterChain chain = FilterChainBuilder.build(providerFilters, terminal());
            return chain.invoke(request);
        } catch (Throwable t) {
            // 兜底：链上任何同步抛出都不能让客户端拿不到响应、干等到超时
            log.warn("[RpcRequestHandler] 处理请求同步抛出 {}#{}: {}",
                    request.getInterfaceName(), request.getMethodName(), t.toString());
            return CompletableFuture.completedFuture(RpcResponse.error(t));
        } finally {
            // chain.invoke 的同步部分已结束（业务方法已返回，或已把 async future 交出来）：
            // 此时必须清理线程本地状态——业务线程马上要回到线程池服务下一个请求。
            TraceContext.restoreMdc(mdc);
            RpcContext.removeContext();
        }
    }

    /**
     * 把请求里的 attachments 还原到 {@link RpcContext}，并把 trace 三元组绑定到 MDC。
     *
     * <p>traceId 由消费侧 {@code TraceFilter} 生成/沿用；这里只做还原，不再生成——提供侧如果自己造
     * traceId，整条链就断成两截了。</p>
     *
     * @return 绑定前的 MDC 快照，供 {@code finally} 恢复
     */
    private TraceContext.MdcSnapshot restoreContext(RpcRequest request) {
        Map<String, String> attachments = request.getAttachments();
        if (attachments != null && !attachments.isEmpty()) {
            RpcContext.getContext().setAttachments(attachments);
            log.debug("[RpcRequestHandler] 还原attachments到RpcContext: keys={}", attachments.keySet());
        }
        return TraceContext.bindMdc(
                request.getAttachment(TraceContext.TRACE_ID),
                request.getAttachment(TraceContext.SPAN_ID),
                request.getAttachment(TraceContext.PARENT_SPAN_ID));
    }

    /** 链尾终端：取实现、反射调用、组装响应 */
    private FilterChain terminal() {
        return req -> {
            try {
                Object impl = serviceCache.get(req.getInterfaceName());
                if (impl == null) {
                    log.warn("[RpcRequestHandler] 未找到服务实现: {}", req.getInterfaceName());
                    throw new RpcException("未找到服务实现: " + req.getInterfaceName());
                }

                // 用paramTypes精确定位重载方法；setAccessible兼容非public实现类
                Method method = impl.getClass().getMethod(req.getMethodName(), req.getParamTypes());
                method.setAccessible(true);
                log.debug("[RpcRequestHandler] 反射调用 {}#{} on {}",
                        req.getInterfaceName(), req.getMethodName(), impl.getClass().getSimpleName());
                Object result = method.invoke(impl, req.getArgs());

                // provider异步：方法返回CompletableFuture，映射成RpcResponse的future，不阻塞业务线程
                if (result instanceof CompletableFuture) {
                    log.debug("[RpcRequestHandler] {}#{} 返回CompletableFuture（provider异步），映射待完成",
                            req.getInterfaceName(), req.getMethodName());
                    return ((CompletableFuture<?>) result).handle((data, error) -> {
                        if (error != null) {
                            log.debug("[RpcRequestHandler] 异步方法异常完成 {}#{}: {}",
                                    req.getInterfaceName(), req.getMethodName(), unwrap(error).toString());
                            return RpcResponse.error(unwrap(error));
                        }
                        return RpcResponse.success(data);
                    });
                }
                log.debug("[RpcRequestHandler] {}#{} 同步返回成功", req.getInterfaceName(), req.getMethodName());
                return CompletableFuture.completedFuture(RpcResponse.success(result));
            } catch (InvocationTargetException e) {
                // 业务方法（同步）抛出的异常，透传真实cause
                log.debug("[RpcRequestHandler] 业务方法抛异常 {}#{}: {}",
                        req.getInterfaceName(), req.getMethodName(), String.valueOf(e.getTargetException()));
                return CompletableFuture.completedFuture(RpcResponse.error(e.getTargetException()));
            } catch (Throwable t) {
                // serviceCache.get/getMethod等异常，以及兜底：绝不同步抛出，包成error响应
                log.warn("[RpcRequestHandler] 处理请求失败 {}#{}: {}",
                        req.getInterfaceName(), req.getMethodName(), t.toString());
                return CompletableFuture.completedFuture(RpcResponse.error(t));
            }
        };
    }

    private static Throwable unwrap(Throwable t) {
        return (t instanceof CompletionException && t.getCause() != null) ? t.getCause() : t;
    }
}
