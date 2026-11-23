package io.github.nitouge.conduit.demo;

import io.github.nitouge.conduit.api.ServiceMeta;
import io.github.nitouge.conduit.client.RpcClient;
import io.github.nitouge.conduit.client.proxy.RpcClientProxy;
import io.github.nitouge.conduit.demo.api.HelloService;
import io.github.nitouge.conduit.demo.provider.HelloServiceImpl;
import io.github.nitouge.conduit.protocol.ProtocolConstants;
import io.github.nitouge.conduit.remoting.ClientOptions;
import io.github.nitouge.conduit.remoting.ServerOptions;
import io.github.nitouge.conduit.remoting.handshake.Handshake;
import io.github.nitouge.conduit.remoting.handshake.Negotiated;
import io.github.nitouge.conduit.server.RpcServer;
import io.github.nitouge.conduit.spring.ConduitProperties;
import io.netty.channel.Channel;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.util.Base64;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * 传输层配置端到端验收：同一份 {@link ConduitProperties}（等价于 application.yml）同时构建
 * 服务端与客户端，走真实 Netty 完成调用，证明配置不是"只绑定、不生效"。
 *
 * <p>覆盖：Kryo + GZIP（含压缩阈值）、连接池、心跳、重连；以及经 `conduit.transport.tls.*`
 * （PEM 文件）启用 TLS 的端到端调用。证书在测试期用 JDK 自带 keytool 生成，不提交证书文件。</p>
 */
class TransportConfigE2ETest {

    private static final String HOST = "127.0.0.1";

    @Test
    void configuredTransportCallWorksWithKryoAndGzip() throws Exception {
        ConduitProperties props = kryoGzipProperties();
        int port = freePort();
        RpcServer server = new RpcServer(HOST, port, props.toServerOptions());
        server.export(HelloService.class, new HelloServiceImpl());
        server.start();

        RpcClient client = new RpcClient(props.toClientOptions());
        try {
            HelloService hello = proxy(client, port);
            // 大报文：走 GZIP 压缩路径
            String payload = "x".repeat(8192);
            assertEquals("Hello, " + payload + " from Conduit!", hello.sayHello(payload));

            Channel channel = client.nettyClient().getChannel(new InetSocketAddress(HOST, port));
            Negotiated negotiated = Handshake.negotiated(channel);
            assertNotNull(negotiated, "连接必须完成握手");
            assertEquals(ProtocolConstants.VERSION, negotiated.getVersion());
            assertEquals(ProtocolConstants.CODEC_KRYO, negotiated.getCodec());
            assertEquals(ProtocolConstants.COMPRESS_GZIP, negotiated.getCompress());
        } finally {
            client.close();
            server.close();
        }
    }

    @Test
    void tlsConfiguredThroughPropertiesWorks(@TempDir Path dir) throws Exception {
        // 1) 生成自签证书（SAN 含127.0.0.1，满足客户端默认的HTTPS主机名校验）
        Path keyStorePath = dir.resolve("conduit-demo.p12");
        generateKeyStore(keyStorePath);

        // 2) 从keystore导出证书链与私钥为PEM（ConduitProperties的TLS配置读PEM）
        Path certPem = dir.resolve("server.crt");
        Path keyPem = dir.resolve("server.key");
        exportPem(keyStorePath, "changeit".toCharArray(), "conduit", certPem, keyPem);

        // 3) 等价于 application.yml 里的 conduit.transport.tls.*
        ConduitProperties props = new ConduitProperties();
        props.getProtocol().setSerialization("kryo");
        props.getTransport().getTls().setEnabled(true);
        props.getTransport().getTls().setCert(certPem.toString());
        props.getTransport().getTls().setKey(keyPem.toString());
        props.getTransport().getTls().setTrustCert(certPem.toString());

        ClientOptions clientOptions = props.toClientOptions();
        ServerOptions serverOptions = props.toServerOptions();
        assertNotNull(clientOptions.getSslContext(), "客户端应构建出SslContext");
        assertNotNull(serverOptions.getSslContext(), "服务端应构建出SslContext");

        int port = freePort();
        RpcServer server = new RpcServer(HOST, port, serverOptions);
        server.export(HelloService.class, new HelloServiceImpl());
        server.start();

        RpcClient client = new RpcClient(clientOptions);
        try {
            assertEquals("Hello, TLS from Conduit!", proxy(client, port).sayHello("TLS"));
        } finally {
            client.close();
            server.close();
        }
    }

    private static HelloService proxy(RpcClient client, int port) {
        ServiceMeta target = new ServiceMeta(HelloService.class.getName(), HOST, port);
        return RpcClientProxy.create(HelloService.class, target, client, 5_000);
    }

    /** Kryo + GZIP + 连接池/心跳/重连，全部通过配置项设置 */
    private static ConduitProperties kryoGzipProperties() {
        ConduitProperties props = new ConduitProperties();
        props.getProtocol().setSerialization("kryo");
        props.getProtocol().setCompress("gzip");
        props.getProtocol().setCompressThreshold(0); // 测试期强制压缩，确保压缩路径被覆盖
        props.getTransport().setMaxConnectionsPerAddress(2);
        props.getTransport().setHeartbeatIntervalMs(200);
        props.getTransport().setReadIdleTimeoutMs(1_000);
        props.getTransport().getReconnect().setEnabled(true);
        props.getTransport().getReconnect().setMaxAttempts(5);
        props.getTransport().getReconnect().setBaseDelayMs(100);
        return props;
    }

    private static int freePort() throws Exception {
        try (ServerSocket socket = new ServerSocket(0)) {
            socket.setReuseAddress(true);
            return socket.getLocalPort();
        }
    }

    private static void generateKeyStore(Path keyStore) throws Exception {
        String executable = System.getProperty("os.name").toLowerCase().contains("win")
                ? "keytool.exe" : "keytool";
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

    private static void exportPem(Path keyStorePath, char[] password, String alias,
                                  Path certPem, Path keyPem) throws Exception {
        KeyStore keyStore = KeyStore.getInstance("PKCS12");
        try (InputStream in = Files.newInputStream(keyStorePath)) {
            keyStore.load(in, password);
        }
        String certificate = pem("CERTIFICATE", keyStore.getCertificate(alias).getEncoded());
        PrivateKey privateKey = (PrivateKey) keyStore.getKey(alias, password);
        assertNotNull(privateKey, "keystore中应能取到私钥");
        String key = pem("PRIVATE KEY", privateKey.getEncoded());

        Files.writeString(certPem, certificate, StandardCharsets.US_ASCII);
        Files.writeString(keyPem, key, StandardCharsets.US_ASCII);
    }

    /** DER -> PEM（PKCS#8私钥编码；证书为X.509编码） */
    private static String pem(String type, byte[] der) {
        String base64 = Base64.getMimeEncoder(64, new byte[]{'\n'}).encodeToString(der);
        return "-----BEGIN " + type + "-----\n" + base64 + "\n-----END " + type + "-----\n";
    }
}
