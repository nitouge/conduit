package io.github.nitouge.conduit.api;

import java.io.Serial;
import java.io.Serializable;

/**
 * 一次调用的响应体，只携带结果或异常。
 *
 * <p>同样<b>不包含requestId</b>——靠协议头的requestId与请求关联。</p>
 */
public class RpcResponse implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    /** 正常返回值；调用失败时为null */
    private Object data;

    /** 调用过程中抛出的异常；正常时为null */
    private Throwable exception;

    public RpcResponse() {
    }

    /** 构造一个成功响应 */
    public static RpcResponse success(Object data) {
        RpcResponse r = new RpcResponse();
        r.setData(data);
        return r;
    }

    /** 构造一个异常响应 */
    public static RpcResponse error(Throwable exception) {
        RpcResponse r = new RpcResponse();
        r.setException(exception);
        return r;
    }

    /** 是否携带异常 */
    public boolean hasException() {
        return exception != null;
    }

    public Object getData() {
        return data;
    }

    public void setData(Object data) {
        this.data = data;
    }

    public Throwable getException() {
        return exception;
    }

    public void setException(Throwable exception) {
        this.exception = exception;
    }

    @Override
    public String toString() {
        return "RpcResponse{" +
                "data=" + data +
                ", exception=" + exception +
                '}';
    }
}
