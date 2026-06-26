package ru.hse.drip.systems.raft.protocol;

import java.util.Objects;

public class NodeEndpoint {
    public String nodeId;
    public String host;
    public int port;

    public NodeEndpoint() {
    }

    public NodeEndpoint(String nodeId, String host, int port) {
        this.nodeId = nodeId;
        this.host = host;
        this.port = port;
    }

    public String address() {
        return host + ":" + port;
    }

    @Override
    public String toString() {
        return nodeId + "=" + address();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof NodeEndpoint that)) {
            return false;
        }
        return port == that.port
                && Objects.equals(nodeId, that.nodeId)
                && Objects.equals(host, that.host);
    }

    @Override
    public int hashCode() {
        return Objects.hash(nodeId, host, port);
    }
}
