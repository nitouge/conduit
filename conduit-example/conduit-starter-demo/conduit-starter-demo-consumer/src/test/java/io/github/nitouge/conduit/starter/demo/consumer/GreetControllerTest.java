package io.github.nitouge.conduit.starter.demo.consumer;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.web.servlet.MockMvc;

import static org.hamcrest.Matchers.containsString;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * 仅测Web层（<b>无需Nacos、无需真实RPC</b>）：{@code @WebMvcTest} 不加载Conduit自动配置，
 * 用 {@code @MockBean} 顶替 {@link GreetConsumer}，验证/greet端点把远程结果透传出去。
 *
 * <p>真正的两进程跨服务调用见模块README：启动Nacos + provider + consumer，再curl。</p>
 */
@WebMvcTest(GreetController.class)
class GreetControllerTest {

    @Autowired
    private MockMvc mvc;

    @MockBean
    private GreetConsumer consumer;

    @Test
    void greetEndpointReturnsRemoteResult() throws Exception {
        when(consumer.greet("Tom")).thenReturn("Hi, Tom! (stub provider)");

        mvc.perform(get("/greet").param("name", "Tom"))
                .andExpect(status().isOk())
                .andExpect(content().string(containsString("Hi, Tom!")));
    }
}
