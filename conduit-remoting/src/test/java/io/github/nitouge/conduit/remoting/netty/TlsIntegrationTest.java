package io.github.nitouge.conduit.remoting.netty;

import io.github.nitouge.conduit.remoting.ClientOptions;
import io.github.nitouge.conduit.remoting.ServerOptions;
import io.github.nitouge.conduit.remoting.TestSupport;
import io.github.nitouge.conduit.remoting.tls.Tls;
import io.netty.handler.ssl.SslContext;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.TrustManagerFactory;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * 链路加密（TLS）集成测试：自签证书端到端加密调用；明文/密文不匹配时快速失败。
 *
 * <p>证书在测试期用JDK自带keytool生成（含 127.0.0.1 的SAN），不依赖外部证书文件。</p>
 */
class TlsIntegrationTest {

    @TempDir
    static Path tempDir;

    private static SslContext serverSslContext;
    private static SslContext clientSslContext;

    @BeforeAll
    static void setUp() throws Exception {
        char[] password = "changeit".toCharArray();
        Path keyStore = tempDir.resolve("conduit-test.p12");
        generateKeyStore(keyStore);

        KeyStore store = KeyStore.getInstance("PKCS12");
        try (InputStream in = Files.newInputStream(keyStore)) {
            store.load(in, password);
        }
        KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        keyManagerFactory.init(store, password);
        TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(
                TrustManagerFactory.getDefaultAlgorithm());
        trustManagerFactory.init(store);

        serverSslContext = Tls.server(keyManagerFactory);
        // 客户端双向校验：信任该自签证书 + 开启HTTPS主机名校验（证书SAN含127.0.0.1）
        clientSslContext = Tls.client(trustManagerFactory, Tls.DEFAULT_ENDPOINT_IDENTIFICATION);
    }

    private static void generateKeyStore(Path keyStore) throws Exception {
        String executable = System.getProperty("os.name").toLowerCase().contains("win") ? "keytool.exe" : "keytool";
        String keytool = Path.of(System.getProperty("java.home"), "bin", executable).toString();
        Process process = new ProcessBuilder(
                keytool,
                "-genkeypair",
                "-alias", "conduit",
                "-keyalg", "RSA",
                "-keysize", "2048",
                "-validity", "2",
                "-dname", "CN=localhost",
                "-ext", "SAN=ip:127.0.0.1,dns:localhost",
                "-storetype", "PKCS12",
                "-keystore", keyStore.toString(),
                "-storepass", "changeit",
                "-keypass", "changeit",
                "-noprompt")
                .redirectErrorStream(true)
                .start();
        String output = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        assertEquals(0, process.waitFor(), "keytool生成测试证书失败: " + output);
    }

    @Test
    void tlsEndToEndCallWorks() throws Exception {
        int port = TestSupport.freePort();
        NettyServer server = new NettyServer(TestSupport.HOST, port, TestSupport.echoHandler(),
                ServerOptions.builder().sslContext(serverSslContext).build());
        server.start();

        NettyClient client = new NettyClient(ClientOptions.builder().sslContext(clientSslContext).build());
        try {
            assertEquals("ok:secure", TestSupport.call(client, port, "secure").getData());
        } finally {
            client.close();
            server.close();
        }
    }

    @Test
    void insecureClientStillRunsOverTls() throws Exception {
        int port = TestSupport.freePort();
        NettyServer server = new NettyServer(TestSupport.HOST, port, TestSupport.echoHandler(),
                ServerOptions.builder().sslContext(serverSslContext).build());
        server.start();

        NettyClient client = new NettyClient(ClientOptions.builder()
                .sslContext(Tls.insecureClient())
                .build());
        try {
            assertEquals("ok:insecure", TestSupport.call(client, port, "insecure").getData());
        } finally {
            client.close();
            server.close();
        }
    }

    @Test
    void plaintextClientCannotTalkToTlsServer() throws Exception {
        int port = TestSupport.freePort();
        NettyServer server = new NettyServer(TestSupport.HOST, port, TestSupport.echoHandler(),
                ServerOptions.builder().sslContext(serverSslContext).build());
        server.start();

        NettyClient client = new NettyClient(ClientOptions.builder()
                .connectTimeoutMillis(1_000)
                .handshakeTimeoutMillis(1_000)
                .reconnectEnabled(false)
                .build());
        try {
            Throwable cause = TestSupport.callFailing(client, port, "plaintext");
            assertNotNull(cause, "明文客户端访问TLS服务端必须失败");
        } finally {
            client.close();
            server.close();
        }
    }

    @Test
    void tlsClientCannotTalkToPlaintextServer() throws Exception {
        int port = TestSupport.freePort();
        NettyServer server = new NettyServer(TestSupport.HOST, port, TestSupport.echoHandler());
        server.start();

        NettyClient client = new NettyClient(ClientOptions.builder()
                .sslContext(Tls.insecureClient())
                .connectTimeoutMillis(1_000)
                .handshakeTimeoutMillis(1_000)
                .reconnectEnabled(false)
                .build());
        try {
            Throwable cause = TestSupport.callFailing(client, port, "tls");
            assertNotNull(cause, "TLS客户端访问明文服务端必须失败");
        } finally {
            client.close();
            server.close();
        }
    }
}
