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
 * <p><b>Filter链（§0.A）+ RpcContext（§0.B）：</b>同步与异步provider方法都经提供侧
 * {@link Filter} 链——链尾终端才反射调用业务方法。调用前把 {@link RpcRequest#getAttachments()}
 * 还原到 {@link RpcContext}（业务方法体能读取），{@code finally} 清空防泄漏。
 * 异步方法的continuation跑另一线程读不到上下文——属固有限制，与H12同类，需要时自行拷贝snapshot。</p>
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
        // 链尾终端：取实现、填充RpcContext、反射调用、组装响应、finally清空上下文。
        // 整体包try/catch(Throwable) 兜底——保证terminal绝不同步抛出（无filter时chain==terminal，
        // 若同步抛出会让传输层拿不到响应、客户端干等超时）。
        FilterChain terminal = req -> {
            try {
                Object impl = serviceCache.get(req.getInterfaceName());
                if (impl == null) {
                    log.warn("[RpcRequestHandler] 未找到服务实现: {}", req.getInterfaceName());
                    throw new RpcException("未找到服务实现: " + req.getInterfaceName());
                }

                // §0.B：从请求还原attachments到当前线程RpcContext（业务方法体可读取）
                Map<String, String> attachments = req.getAttachments();
                if (attachments != null && !attachments.isEmpty()) {
                    RpcContext.getContext().setAttachments(attachments);
                    log.debug("[RpcRequestHandler] 还原attachments到RpcContext: keys={}", attachments.keySet());
                }

                try {
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
                    // 同步：立即完成
                    log.debug("[RpcRequestHandler] {}#{} 同步返回成功", req.getInterfaceName(), req.getMethodName());
                    return CompletableFuture.completedFuture(RpcResponse.success(result));
                } catch (InvocationTargetException e) {
                    // 业务方法（同步）抛出的异常，透传真实cause
                    log.debug("[RpcRequestHandler] 业务方法抛异常 {}#{}: {}",
                            req.getInterfaceName(), req.getMethodName(), String.valueOf(e.getTargetException()));
                    return CompletableFuture.completedFuture(RpcResponse.error(e.getTargetException()));
                } finally {
                    // 调用边界清空上下文——同步方法立即清空；异步方法也在这里清（continuation跑另一线程
                    // 反正读不到，这里清掉不影响它，只是防止业务线程池的线程泄漏给下次请求）
                    RpcContext.removeContext();
                }
            } catch (Throwable t) {
                // serviceCache.get/getMethod等异常，以及兜底：绝不同步抛出，包成error响应
                log.warn("[RpcRequestHandler] 处理请求失败 {}#{}: {}",
                        req.getInterfaceName(), req.getMethodName(), t.toString());
                return CompletableFuture.completedFuture(RpcResponse.error(t));
            }
        };

        FilterChain chain = FilterChainBuilder.build(providerFilters, terminal);
        return chain.invoke(request);
    }

    private static Throwable unwrap(Throwable t) {
        return (t instanceof CompletionException && t.getCause() != null) ? t.getCause() : t;
    }
}
