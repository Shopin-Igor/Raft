package ru.hse.drip.systems.raft.node;

import ru.hse.drip.systems.raft.protocol.NodeEndpoint;
import ru.hse.drip.systems.raft.protocol.WireMessage;

import java.io.IOException;

public interface RpcClient {
    WireMessage request(NodeEndpoint endpoint, WireMessage message) throws IOException;
}
