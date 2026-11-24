package io.github.nitouge.conduit.common.spi;

import io.github.nitouge.conduit.api.RpcRequest;

import java.util.Map;

public class EnglishTranslator implements Translator {

    @Override
    public String translate(Map<String, String> attributes) {
        return "en";
    }

    @Override
    public String translate(RpcRequest request) {
        return "en";
    }
}
