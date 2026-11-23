package io.github.nitouge.conduit.starter.demo.consumer;

import io.github.nitouge.conduit.protocol.ProtocolConstants;
import io.github.nitouge.conduit.remoting.ClientOptions;
import io.github.nitouge.conduit.serialization.SerializerType;
import io.github.nitouge.conduit.spring.ConduitProperties;
import org.junit.jupiter.api.Test;
import org.springframework.boot.context.properties.bind.BindResult;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.boot.context.properties.source.ConfigurationPropertySources;
import org.springframework.boot.env.YamlPropertySourceLoader;
import org.springframework.core.env.MutablePropertySources;
import org.springframework.core.env.PropertySource;
import org.springframework.core.io.ClassPathResource;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 消费端 {@code application.yml} 配置绑定验收（<b>不启动Spring上下文、不连Nacos</b>）。
 *
 * <p>{@code GreetControllerTest} 是纯 {@code @WebMvcTest}，不会加载Conduit自动配置，
 * 因此消费端的 yml 改动无法被它覆盖；这里直接读取真实的 application.yml 并绑定
 * {@link ConduitProperties}，确保属性名/嵌套结构写错时立刻失败，而不是静默用默认值。</p>
 */
class ConsumerApplicationYmlTest {

    @Test
    void applicationYmlBindsTransportConfig() throws Exception {
        List<PropertySource<?>> sources = new YamlPropertySourceLoader()
                .load("application.yml", new ClassPathResource("application.yml"));
        MutablePropertySources propertySources = new MutablePropertySources();
        sources.forEach(propertySources::addLast);

        BindResult<ConduitProperties> bound = new Binder(ConfigurationPropertySources.from(propertySources))
                .bind("conduit", ConduitProperties.class);
        assertTrue(bound.isBound(), "application.yml 中缺少 conduit 配置");
        ConduitProperties props = bound.get();

        assertEquals("greet-consumer", props.getApplication().getName());
        assertEquals("nacos", props.getRegistry().getType());

        ClientOptions options = props.toClientOptions();
        assertEquals(SerializerType.idOf("kryo"), options.getCodec());
        assertEquals(ProtocolConstants.COMPRESS_GZIP, options.getCompress());
        assertEquals(1024, options.getCompressThreshold());
        assertEquals(30_000, options.getHeartbeatIntervalMillis());
        assertTrue(options.isReconnectEnabled());
        assertEquals(5, options.getReconnectMaxAttempts());
        assertEquals(1_000, options.getReconnectBaseDelayMillis());
        assertEquals(30_000, options.getReconnectMaxDelayMillis());
    }
}
