package io.github.nitouge.conduit.api;

import java.io.Serial;
import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * 一次远程调用的请求体。
 *
 * <p>它只描述调哪个接口的哪个方法、传什么参数，<b>不包含requestId</b>——
 * requestId由协议头（8字节long）携带，用于请求-响应关联，避免重复。</p>
 */
public class RpcRequest implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    /** 接口全限定名，例如com.example.UserService */
    private String interfaceName;

    /** 方法名 */
    private String methodName;

    /** 方法参数类型，用于在服务端精确定位重载方法 */
    private Class<?>[] paramTypes;

    /** 方法实参 */
    private Object[] args;

    /** 服务版本（可选），用于灰度/多版本共存 */
    private String version;

    /** 服务分组（可选） */
    private String group;

    /**
     * 隐式附加参数：随请求跨进程透传的K-V（traceId/token 等），配合 {@link RpcContext}。
     *
     * <p>不是业务方法的显式入参，而是链路追踪、鉴权、泛化调用等横切能力的载体。
     * 消费侧从 {@link RpcContext} 拷入、提供侧还原到 {@link RpcContext}。
     * 各序列化器对 {@code Map<String,String>} 天然支持，无需改协议codec。</p>
     */
    private Map<String, String> attachments;

    public RpcRequest() {
    }

    public String getInterfaceName() {
        return interfaceName;
    }

    public void setInterfaceName(String interfaceName) {
        this.interfaceName = interfaceName;
    }

    public String getMethodName() {
        return methodName;
    }

    public void setMethodName(String methodName) {
        this.methodName = methodName;
    }

    public Class<?>[] getParamTypes() {
        return paramTypes;
    }

    public void setParamTypes(Class<?>[] paramTypes) {
        this.paramTypes = paramTypes;
    }

    public Object[] getArgs() {
        return args;
    }

    public void setArgs(Object[] args) {
        this.args = args;
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public String getGroup() {
        return group;
    }

    public void setGroup(String group) {
        this.group = group;
    }

    public Map<String, String> getAttachments() {
        return attachments;
    }

    public void setAttachments(Map<String, String> attachments) {
        this.attachments = attachments;
    }

    /**
     * 读取单个附加参数；未携带或 {@code key} 不存在时返回 {@code null}。
     *
     * <p>提供此便利方法供业务侧/Filter 读取，避免显式 null 判断。</p>
     */
    public String getAttachment(String key) {
        if (attachments == null || key == null) {
            return null;
        }
        return attachments.get(key);
    }

    /** 写入单个附加参数（懒初始化底层 map） */
    public void setAttachment(String key, String value) {
        if (key == null) {
            return;
        }
        if (attachments == null) {
            attachments = new HashMap<>();
        }
        attachments.put(key, value);
    }

    @Override
    public String toString() {
        return "RpcRequest{" +
                "interfaceName='" + interfaceName + '\'' +
                ", methodName='" + methodName + '\'' +
                ", paramTypes=" + Arrays.toString(paramTypes) +
                ", args=" + Arrays.toString(args) +
                ", version='" + version + '\'' +
                ", group='" + group + '\'' +
                ", attachments=" + attachments +
                '}';
    }
}
