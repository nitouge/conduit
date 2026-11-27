package io.github.nitouge.conduit.it.multi;

/**
 * {@link NodeService} 实现：仅返回节点标识，业务逻辑尽可能少。
 */
public class NodeServiceImpl implements NodeService {

    private final String nodeId;

    public NodeServiceImpl(String nodeId) {
        this.nodeId = nodeId;
    }

    @Override
    public String whoAmI() {
        return nodeId;
    }

    @Override
    public String echo(String message) {
        return nodeId + ":" + message;
    }
}
