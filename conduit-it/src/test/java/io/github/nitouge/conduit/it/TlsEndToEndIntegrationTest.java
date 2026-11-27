package io.github.nitouge.conduit.it;

import io.github.nitouge.conduit.remoting.ClientOptions;
import io.github.nitouge.conduit.remoting.ServerOptions;
import io.github.nitouge.conduit.testkit.EchoService;
import io.github.nitouge.conduit.testkit.EchoServiceImpl;
import io.github.nitouge.conduit.testkit.TestReset;
import io.github.nitouge.conduit.testkit.TestRpc;
import io.github.nitouge.conduit.testkit.TestServer;
import io.github.nitouge.conduit.testkit.TlsTestCerts;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * 链路加密（TLS）端到端：真实TCP + 真实TLS握手 + 完整RPC调用。
 *
 * <p>证书由testkit在测试期用keytool生成（SAN含127.0.0.1），客户端开启HTTPS主机名校验，
 * 因此这条用例同时验证了"加密"和"证书校验"两个环节。</p>
 */
class TlsEndToEndIntegrationTest {

    @BeforeEach
    void reset() {
        TestReset.all();
    }

    /** 双向TLS：服务端持证书、客户端信任该证书并校验主机名 */
    @Test
    void tlsCallSucceedsWithVerifiedCertificate() {
        ServerOptions serverOptions = ServerOptions.builder()
                .sslContext(TlsTestCerts.serverContext())
                .build();
        ClientOptions clientOptions = ClientOptions.builder()
                .sslContext(TlsTestCerts.clientContext())
                .build();

        try (TestServer server = TestServer.withOptions(serverOptions)
                .export(EchoService.class, new EchoServiceImpl("tls"))
                .start();
             TestRpc rpc = TestRpc.create(clientOptions)) {

            EchoService echo = rpc.proxy(EchoService.class, server.meta(EchoService.class), 5_000);
            assertEquals("ok:secure", echo.echo("secure"));
            assertEquals("tls", echo.nodeId());
        }
    }

    /** 明文客户端打TLS服务端：应在调用阶段快速失败，而不是一直挂住 */
    @Test
    void plainClientCannotTalkToTlsServer() {
        ServerOptions serverOptions = ServerOptions.builder()
                .sslContext(TlsTestCerts.serverContext())
                .build();

        try (TestServer server = TestServer.withOptions(serverOptions)
                .export(EchoService.class, new EchoServiceImpl("tls"))
                .start();
             TestRpc rpc = TestRpc.create()) {

            EchoService echo = rpc.proxy(EchoService.class, server.meta(EchoService.class), 2_000);
            assertThrows(RuntimeException.class, () -> echo.echo("plain"));
        }
    }
}
