package io.github.nitouge.conduit.remoting.tls;

import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslProvider;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLException;
import javax.net.ssl.TrustManagerFactory;
import java.io.File;
import java.io.InputStream;

/**
 * TLS配置工厂：链路加密的便捷入口。
 *
 * <p>Conduit的传输层本身只依赖一个 {@link SslContext}：把它交给
 * {@code ClientOptions.sslContext(...)} 或 {@code ServerOptions.sslContext(...)} 即启用TLS，
 * 其余（建连、握手、心跳、连接池、重连）行为完全不变。</p>
 *
 * <p>客户端默认启用HTTPS主机名校验（{@code endpointIdentificationAlgorithm=HTTPS}），
 * 防止"证书合法但连接被中间人指向了别的主机"。自签名/内网证书可显式传入
 * {@link #client(TrustManagerFactory, String)} 并指定 {@code null} 关闭主机名校验（仅限受控环境）。</p>
 *
 * <p>使用JDK SSL provider（不依赖netty-tcnative/OpenSSL），保证在任何JVM上行为一致。</p>
 */
public final class Tls {

    /** 客户端默认的主机名校验算法 */
    public static final String DEFAULT_ENDPOINT_IDENTIFICATION = "HTTPS";

    private Tls() {
    }

    // ---------------- 服务端 ----------------

    /** 服务端：从PEM证书链 + PKCS8私钥文件构建（无密码） */
    public static SslContext server(File certChain, File privateKey) throws SSLException {
        return server(certChain, privateKey, null);
    }

    /** 服务端：从PEM证书链 + PKCS8私钥文件构建（私钥有密码时传密码） */
    public static SslContext server(File certChain, File privateKey, String privateKeyPassword) throws SSLException {
        return SslContextBuilder.forServer(certChain, privateKey, privateKeyPassword)
                .sslProvider(SslProvider.JDK)
                .build();
    }

    /** 服务端：从PEM证书链 + PKCS8私钥输入流构建（便于从classpath加载） */
    public static SslContext server(InputStream certChain, InputStream privateKey, String privateKeyPassword)
            throws SSLException {
        return SslContextBuilder.forServer(certChain, privateKey, privateKeyPassword)
                .sslProvider(SslProvider.JDK)
                .build();
    }

    /** 服务端：从已初始化的KeyManagerFactory构建（keystore/PKCS12场景） */
    public static SslContext server(KeyManagerFactory keyManagerFactory) throws SSLException {
        return SslContextBuilder.forServer(keyManagerFactory)
                .sslProvider(SslProvider.JDK)
                .build();
    }

    // ---------------- 客户端 ----------------

    /** 客户端：使用JVM默认信任库，并启用HTTPS主机名校验 */
    public static SslContext defaultClient() throws SSLException {
        return clientBuilder(DEFAULT_ENDPOINT_IDENTIFICATION).build();
    }

    /** 客户端：信任给定CA/证书文件，并启用HTTPS主机名校验 */
    public static SslContext client(File trustCertChain) throws SSLException {
        return client(trustCertChain, DEFAULT_ENDPOINT_IDENTIFICATION);
    }

    /**
     * 客户端：信任给定CA/证书文件。
     *
     * @param endpointIdentificationAlgorithm 主机名校验算法（如"HTTPS"）；null/空表示关闭校验（仅限受控环境）
     */
    public static SslContext client(File trustCertChain, String endpointIdentificationAlgorithm)
            throws SSLException {
        return clientBuilder(endpointIdentificationAlgorithm)
                .trustManager(trustCertChain)
                .build();
    }

    /** 客户端：信任给定TrustManagerFactory，并启用HTTPS主机名校验 */
    public static SslContext client(TrustManagerFactory trustManagerFactory) throws SSLException {
        return client(trustManagerFactory, DEFAULT_ENDPOINT_IDENTIFICATION);
    }

    /** 客户端：信任给定证书输入流（便于从classpath加载），并启用HTTPS主机名校验 */
    public static SslContext client(InputStream trustCertChain) throws SSLException {
        return client(trustCertChain, DEFAULT_ENDPOINT_IDENTIFICATION);
    }

    /** 客户端：信任给定证书输入流 */
    public static SslContext client(InputStream trustCertChain, String endpointIdentificationAlgorithm)
            throws SSLException {
        return clientBuilder(endpointIdentificationAlgorithm)
                .trustManager(trustCertChain)
                .build();
    }

    /** 客户端：信任给定TrustManagerFactory */
    public static SslContext client(TrustManagerFactory trustManagerFactory, String endpointIdentificationAlgorithm)
            throws SSLException {
        return clientBuilder(endpointIdentificationAlgorithm)
                .trustManager(trustManagerFactory)
                .build();
    }

    /**
     * 客户端：信任任意服务端证书（<b>不校验身份</b>），仅用于本地开发/测试。
     *
     * <p>切勿在生产使用：它无法抵御中间人攻击。</p>
     */
    public static SslContext insecureClient() throws SSLException {
        return clientBuilder(null)
                .trustManager(InsecureTrustManagerFactory.INSTANCE)
                .build();
    }

    private static SslContextBuilder clientBuilder(String endpointIdentificationAlgorithm) {
        SslContextBuilder builder = SslContextBuilder.forClient().sslProvider(SslProvider.JDK);
        if (endpointIdentificationAlgorithm != null && !endpointIdentificationAlgorithm.isBlank()) {
            builder.endpointIdentificationAlgorithm(endpointIdentificationAlgorithm);
        }
        return builder;
    }
}
