package io.github.nitouge.conduit.common.spi;

import io.github.nitouge.conduit.api.RpcRequest;

import java.util.Map;

public class ChineseTranslator implements Translator {

    @Override
    public String translate(Map<String, String> attributes) {
        return "zh";
    }

    @Override
    public String translate(RpcRequest request) {
        return "zh";
    }
}
