package ru.hse.drip.systems.raft.node;

import ru.hse.drip.systems.raft.protocol.NodeEndpoint;
import ru.hse.drip.systems.raft.protocol.WireMessage;
import ru.hse.drip.systems.raft.transport.TcpClient;

import java.io.IOException;
import java.time.Duration;

public class TcpRpcClient implements RpcClient {
    private final TcpClient tcpClient;

    public TcpRpcClient(Duration connectTimeout, Duration readTimeout) {
        this.tcpClient = new TcpClient(connectTimeout, readTimeout);
    }

    @Override
    public WireMessage request(NodeEndpoint endpoint, WireMessage message) throws IOException {
        return tcpClient.request(endpoint, message);
    }
}
