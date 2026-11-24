package io.github.nitouge.conduit.common.spi;

import io.github.nitouge.conduit.api.RpcRequest;

import java.util.Map;

/** 自适应扩展测试用@SPI接口：按参数中的lang选择实现。 */
@SPI("en")
public interface Translator {

    @Adaptive({"lang"})
    String translate(Map<String, String> attributes);

    @Adaptive({"lang"})
    String translate(RpcRequest request);
}
