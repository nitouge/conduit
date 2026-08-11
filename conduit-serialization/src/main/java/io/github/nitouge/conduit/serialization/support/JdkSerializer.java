package io.github.nitouge.conduit.serialization.support;

import io.github.nitouge.conduit.serialization.SerializationException;
import io.github.nitouge.conduit.serialization.Serializer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputFilter;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

/**
 * 基于JDK原生 {@link ObjectOutputStream} / {@link ObjectInputStream} 的序列化器。
 *
 * <p>M1默认：零依赖、零配置，几行代码即可端到端跑通。被序列化的对象必须实现
 * {@link java.io.Serializable}（RpcRequest/RpcResponse/ServiceMeta均已实现）。</p>
 *
 * <p>线程安全：每次调用都新建一对流，无共享状态，天然线程安全。</p>
 *
 * <p><b>安全（CWE-502）：</b> {@code readObject()} 会按字节流里的类名反射实例化任意可序列化类，
 * 这是Java反序列化gadget链RCE的经典触发点。而协议解码侧（{@code RpcDecoder}）是按帧头里的
 * {@code codec} 字段选序列化器的——即便把默认 {@code @SPI} 改成kryo，攻击者仍可把codec置为
 * {@code CODEC_JDK} 强制走到这里。因此真正兜底的是下面这道 {@link ObjectInputFilter}：用
 * <b>黑名单 + 资源上限</b> 拦掉常见gadget包与超深/超大对象图，同时放行普通业务对象（{@code Object[] args}
 * 可能是任意用户类型，故不能用严格白名单）。生产环境应改用Kryo/Protostuff，或把白名单包做成可配置。</p>
 *
 * <p>缺点：体积大、慢、仅Java、对serialVersionUID敏感——<b>不要用于生产</b>，生产请用Kryo。</p>
 */
public class JdkSerializer implements Serializer {

    /**
     * 反序列化过滤器（防御式加固）：
     * <ul>
     *   <li>资源上限 {@code maxdepth/maxrefs/maxarray}：拦掉构造畸形对象图导致的OOM/栈溢出；</li>
     *   <li>{@code !pkg.*} 黑名单：拒绝已知gadget链常用包（commons-collections functors、groovy、bsh、
     *       rmi、xalan、rhino 等）；</li>
     *   <li>未命中任何模式的类返回UNDECIDED → 默认放行，保证普通业务POJO（{@code io.github.nitouge.conduit.*} / {@code java.*} /用户包）仍可正常反序列化。</li>
     * </ul>
     */
    private static final ObjectInputFilter SAFE_FILTER = ObjectInputFilter.Config.createFilter(
            "maxdepth=64;maxrefs=100000;maxarray=1000000;"
                    + "!org.apache.commons.collections.functors.*;"
                    + "!org.apache.commons.collections4.functors.*;"
                    + "!org.apache.commons.beanutils.*;"
                    + "!org.codehaus.groovy.runtime.*;"
                    + "!org.apache.xalan.*;!com.sun.org.apache.xalan.**;"
                    + "!bsh.*;!org.python.core.*;!org.mozilla.javascript.*;"
                    + "!java.rmi.server.*;!sun.rmi.server.*;"
                    + "!org.springframework.beans.factory.ObjectFactory;"
                    + "!javax.management.BadAttributeValueExpException");

    @Override
    public byte[] serialize(Object obj) {
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
             ObjectOutputStream oos = new ObjectOutputStream(bos)) {
            oos.writeObject(obj);
            oos.flush();
            return bos.toByteArray();
        } catch (IOException e) {
            throw new SerializationException("JDK serialize failed", e);
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T deserialize(byte[] bytes, Class<T> clazz) {
        try (ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
             ObjectInputStream ois = new ObjectInputStream(bis)) {
            ois.setObjectInputFilter(SAFE_FILTER); // 还原任意对象前先装上黑名单 + 资源上限过滤器
            return (T) ois.readObject();
        } catch (IOException | ClassNotFoundException e) {
            throw new SerializationException("JDK deserialize failed", e);
        }
    }
}
