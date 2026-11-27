package io.github.nitouge.conduit.testkit;

import io.github.nitouge.conduit.remoting.tls.Tls;
import io.netty.handler.ssl.SslContext;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.TrustManagerFactory;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;

/**
 * TLS自签证书夹具：测试期用JDK自带keytool在临时目录生成一份证书（SAN含127.0.0.1）,
 * 提供"服务端用的"与"客户端信任用"的两份 {@link SslContext}，让TLS端到端测试零外部依赖。
 *
 * <p>首次调用时生成，之后同一JVM内复用；证书有效期2天足以覆盖测试，不会污染仓库。</p>
 */
public final class TlsTestCerts {

    private static final char[] PASSWORD = "changeit".toCharArray();

    private static volatile SslContext serverContext;
    private static volatile SslContext clientContext;

    private TlsTestCerts() {
    }

    /** 服务端SslContext（含私钥，PKCS12由keytool生成） */
    public static SslContext serverContext() {
        ensureGenerated();
        return serverContext;
    }

    /** 客户端SslContext（信任该自签证书 + 开启HTTPS主机名校验） */
    public static SslContext clientContext() {
        ensureGenerated();
        return clientContext;
    }

    private static void ensureGenerated() {
        if (serverContext != null && clientContext != null) {
            return;
        }
        synchronized (TlsTestCerts.class) {
            if (serverContext != null && clientContext != null) {
                return;
            }
            try {
                // 注意：不能先用Files.createTempFile建出空文件——keytool不接受已存在的keystore。
                Path dir = Files.createTempDirectory("conduit-it-tls-");
                Path keyStorePath = dir.resolve("conduit-it.p12");
                generateKeyStore(keyStorePath);

                KeyStore store = KeyStore.getInstance("PKCS12");
                try (InputStream in = Files.newInputStream(keyStorePath)) {
                    store.load(in, PASSWORD);
                }
                KeyManagerFactory keyManagerFactory =
                        KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
                keyManagerFactory.init(store, PASSWORD);
                TrustManagerFactory trustManagerFactory =
                        TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
                trustManagerFactory.init(store);

                serverContext = Tls.server(keyManagerFactory);
                clientContext = Tls.client(trustManagerFactory, Tls.DEFAULT_ENDPOINT_IDENTIFICATION);
            } catch (Exception e) {
                throw new IllegalStateException("生成测试TLS证书失败: " + e.getMessage(), e);
            }
        }
    }

    private static void generateKeyStore(Path keyStore) throws Exception {
        String keytoolName = System.getProperty("os.name").toLowerCase().contains("win") ? "keytool.exe" : "keytool";
        String keytool = Path.of(System.getProperty("java.home"), "bin", keytoolName).toString();
        Process process = new ProcessBuilder(
                keytool,
                "-genkeypair",
                "-alias", "conduit-testkit",
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
        int exit = process.waitFor();
        if (exit != 0) {
            throw new IllegalStateException("keytool生成测试证书失败(exit=" + exit + "): " + output);
        }
    }
}
