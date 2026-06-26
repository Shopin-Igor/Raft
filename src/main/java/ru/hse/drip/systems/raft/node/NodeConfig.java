package ru.hse.drip.systems.raft.node;

import ru.hse.drip.systems.raft.protocol.NodeEndpoint;

import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;

public record NodeConfig(
        String nodeId,
        int port,
        Path dataDir,
        boolean fsync,
        Map<String, NodeEndpoint> peers) {
    public NodeConfig {
        peers = new LinkedHashMap<>(peers);
    }

    @Override
    public Map<String, NodeEndpoint> peers() {
        return new LinkedHashMap<>(peers);
    }

    public static Map<String, NodeEndpoint> parsePeers(String raw) {
        Map<String, NodeEndpoint> endpoints = new LinkedHashMap<>();
        if (raw == null || raw.isBlank()) {
            throw new IllegalArgumentException("--peers is required");
        }
        for (String part : raw.split(",")) {
            String[] idAndAddress = part.trim().split("=", 2);
            if (idAndAddress.length != 2) {
                throw new IllegalArgumentException("Bad peer entry: " + part);
            }
            String[] hostAndPort = idAndAddress[1].split(":", 2);
            if (hostAndPort.length != 2) {
                throw new IllegalArgumentException("Bad peer address: " + part);
            }
            String nodeId = idAndAddress[0].trim();
            String host = hostAndPort[0].trim();
            int port = Integer.parseInt(hostAndPort[1].trim());
            endpoints.put(nodeId, new NodeEndpoint(nodeId, host, port));
        }
        return endpoints;
    }

    public static String defaultPeers() {
        return "n1=127.0.0.1:9101,n2=127.0.0.1:9102,n3=127.0.0.1:9103,n4=127.0.0.1:9104,n5=127.0.0.1:9105";
    }
}
