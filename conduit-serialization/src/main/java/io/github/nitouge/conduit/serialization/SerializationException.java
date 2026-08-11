package io.github.nitouge.conduit.serialization;

import io.github.nitouge.conduit.common.exception.RpcException;

import java.io.Serial;

/**
 * 序列化/反序列化失败时抛出。
 */
public class SerializationException extends RpcException {

    @Serial
    private static final long serialVersionUID = 1L;

    public SerializationException(String message) {
        super(message);
    }

    public SerializationException(String message, Throwable cause) {
        super(message, cause);
    }
}
