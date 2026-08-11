package io.github.nitouge.conduit.common.exception;

import java.io.Serial;

/**
 * 框架统一的运行时异常基类。
 *
 * <p>传输、序列化、反射调用、服务未找到等框架层错误都用它（或其子类）表达；
 * 业务方法自身抛出的异常不属于此类，会被原样透传回调用方。</p>
 */
public class RpcException extends RuntimeException {

    @Serial
    private static final long serialVersionUID = 1L;

    public RpcException(String message) {
        super(message);
    }

    public RpcException(String message, Throwable cause) {
        super(message, cause);
    }

    public RpcException(Throwable cause) {
        super(cause);
    }
}
