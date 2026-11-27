
package io.github.nitouge.conduit.common.filter.builtin;

import io.github.nitouge.conduit.api.RpcRequest;
import io.github.nitouge.conduit.api.RpcResponse;
import io.github.nitouge.conduit.common.exception.RpcException;
import io.github.nitouge.conduit.common.filter.Filter;
import io.github.nitouge.conduit.common.filter.FilterChain;
import io.github.nitouge.conduit.common.filter.Side;

import java.util.concurrent.CompletableFuture;

/**
 * 异常规整过滤器（Provider侧）。
 *
 * <p>用于处理服务提供者返回的异常，避免Provider私有异常类型在Consumer侧反序列化失败。</p>
 *
 * <p>异常处理规则：</p>
 * <ul>
 *     <li>RpcException：框架公共异常，直接透传。</li>
 *     <li>JDK标准异常：Consumer侧通常具备对应类型，直接透传。</li>
 *     <li>与服务接口具有相同顶层包前缀的异常：认为属于共享API模块，直接透传。</li>
 *     <li>其他异常：转换为RpcException，避免依赖Provider私有异常类。</li>
 * </ul>
 *
 * <p>异常降级仅保留原异常的类名、消息和堆栈信息，不保留原异常对象及其cause。</p>
 *
 * <p>默认启用，可通过系统属性
 * {@code -Dconduit.filter.exception.enabled=false} 关闭。</p>
 *
 * <p>order=100：作为内层过滤器，尽可能靠近服务调用终端，
 * 在业务异常产生后及时完成规整。</p>
 */
public class ExceptionFilter implements Filter {

    /** 系统异常和标准库异常的包名前缀。 */
    private static final String JAVA_PACKAGE_PREFIX = "java.";

    private static final String JAVAX_PACKAGE_PREFIX = "javax.";

    /** 判断是否属于同一顶层包时使用的包名层级。 */
    private static final int PACKAGE_PREFIX_LEVELS = 2;

    /**
     * 执行异常规整。
     *
     * <p>先执行后续过滤器及实际服务调用，再通过thenApply对正常完成的响应进行检查。
     * 仅当响应包含异常时，才执行异常类型判断和必要的降级处理。</p>
     *
     * @param request RPC请求
     * @param chain   过滤器链
     * @return 规整后的 RPC响应
     */
    @Override
    public CompletableFuture<RpcResponse> invoke(
            RpcRequest request, FilterChain chain) {

        return chain.invoke(request)
                .thenApply(response -> sanitize(request, response));
    }

    /**
     * 返回过滤器执行顺序。
     *
     * <p>数值越小越先执行，100表示内层过滤器，
     * 用于尽可能靠近服务调用终端处理业务异常。</p>
     *
     * @return 过滤器优先级
     */
    @Override
    public int order() {
        return 100;
    }

    /**
     * 指定过滤器生效的调用侧。
     *
     * <p>异常规整只在Provider侧执行，
     * 避免Consumer侧重复处理远程返回的异常。</p>
     *
     * @param side 当前调用侧
     * @return 是否适用于当前调用侧
     */
    @Override
    public boolean appliesTo(Side side) {
        return side == Side.PROVIDER;
    }

    /**
     * 返回过滤器名称。
     *
     * @return 过滤器名称
     */
    @Override
    public String name() {
        return "exception";
    }

    /**
     * 对RPC响应中的异常进行规整。
     *
     * <p>如果异常类型属于Consumer侧可以安全识别的范围，则保留原响应；
     * 否则将异常转换为框架公共异常RpcException。</p>
     *
     * @param request  RPC请求
     * @param response RPC响应
     * @return 规整后的响应
     */
    private RpcResponse sanitize(RpcRequest request, RpcResponse response) {
        // 无响应或响应不包含异常时，无需处理
        if (response == null || !response.hasException()) {
            return response;
        }

        Throwable exception = response.getException();

        // 当前异常属于可直接传输的类型，无需降级
        if (!needsDegrade(request, exception)) {
            return response;
        }

        // 使用框架公共异常替换Provider私有异常，避免Consumer反序列化失败
        RpcException safeException = new RpcException(
                exception.getClass().getName() + ": " + exception.getMessage()
        );

        // 保留原始异常的堆栈信息，便于定位Provider侧问题
        safeException.setStackTrace(exception.getStackTrace());

        return RpcResponse.error(safeException);
    }

    /**
     * 判断异常是否需要降级。
     *
     * <p>通过异常类型、标准库包名以及异常类与服务接口的包名前缀，
     * 粗略判断 Consumer 是否具备对应异常类型。</p>
     *
     * @param request   RPC请求
     * @param exception 服务调用产生的异常
     * @return true 表示需要降级，false表示可以直接透传
     */
    private boolean needsDegrade(RpcRequest request, Throwable exception) {
        // 框架公共异常，Consumer已依赖conduit-common，无需降级
        if (exception instanceof RpcException) {
            return false;
        }

        String exceptionClassName = exception.getClass().getName();
        // JDK标准库异常，Consumer侧通常具备对应类型
        if (exceptionClassName.startsWith(JAVA_PACKAGE_PREFIX)
                || exceptionClassName.startsWith(JAVAX_PACKAGE_PREFIX)) {
            return false;
        }

        // 通过服务接口的包名前缀，粗略判断异常是否属于共享API模块
        String interfaceClassName = request.getInterfaceName();

        return interfaceClassName == null || !hasSamePackagePrefix(exceptionClassName, interfaceClassName);
    }

    /**
     * 判断两个类名是否具有相同的顶层包名前缀。
     *
     * <p>例如：</p>
     * <pre>
     * com.example.api.UserService
     * com.example.api.UserException
     *
     * 提取前两级包名后均为：
     * com.example
     * </pre>
     *
     * <p>注意：相同包名前缀只能作为共享模块的启发式判断，
     * 不能准确证明两个类属于同一个JAR或模块。</p>
     *
     * @param exceptionClassName 异常类全限定名
     * @param interfaceClassName 服务接口全限定名
     * @return 是否具有相同的顶层包名前缀
     */
    private boolean hasSamePackagePrefix(String exceptionClassName, String interfaceClassName) {
        String exceptionPackage = extractPackagePrefix(exceptionClassName, PACKAGE_PREFIX_LEVELS);
        String interfacePackage = extractPackagePrefix(interfaceClassName, PACKAGE_PREFIX_LEVELS);
        return !exceptionPackage.isEmpty() && exceptionPackage.equals(interfacePackage);
    }

    /**
     * 提取指定层级的包名前缀。
     *
     * <p>例如，提取 {@code com.example.api.UserService}
     * 的前两级包名前缀，结果为 {@code com.example}。</p>
     *
     * @param className 类全限定名
     * @param levels    需要提取的包名层级
     * @return 提取后的包名前缀；层级不足时返回空字符串
     */
    private String extractPackagePrefix(String className, int levels) {
        if (className == null || className.isEmpty() || levels <= 0) {
            return "";
        }

        int separatorIndex = -1;
        for (int level = 0; level < levels; level++) {
            separatorIndex = className.indexOf('.', separatorIndex + 1);
            // 包名层级不足，无法提取指定前缀
            if (separatorIndex < 0) {
                return "";
            }
        }

        return className.substring(0, separatorIndex);
    }
}
