package io.github.nitouge.conduit.common.spi;

/** ExtensionLoader测试用@SPI接口 */
@SPI("en")
public interface Greeter {
    String greet();
}
