package ru.hse.drip.systems.raft.node;

import ru.hse.drip.systems.raft.protocol.NodeEndpoint;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

public final class RaftNodeMain {
    private RaftNodeMain() {
    }

    public static void main(String[] args) throws Exception {
        Map<String, String> options = parseArgs(args);
        String nodeId = require(options, "--id");
        int port = Integer.parseInt(require(options, "--port"));
        Path dataDir = Path.of(options.getOrDefault("--data-dir", "data"));
        boolean fsync = !"off".equalsIgnoreCase(options.getOrDefault("--fsync", "on"));
        Map<String, NodeEndpoint> peers = NodeConfig.parsePeers(options.getOrDefault("--peers", NodeConfig.defaultPeers()));

        NodeConfig config = new NodeConfig(nodeId, port, dataDir, fsync, peers);
        RaftNode node = new RaftNode(config);
        Runtime.getRuntime().addShutdownHook(new Thread(node::stop, "raft-shutdown"));
        node.start();
        Thread.currentThread().join();
    }

    private static Map<String, String> parseArgs(String[] args) {
        Map<String, String> options = new HashMap<>();
        for (int i = 0; i < args.length; i += 2) {
            if (i + 1 >= args.length) {
                throw new IllegalArgumentException("Missing value for " + args[i]);
            }
            options.put(args[i], args[i + 1]);
        }
        return options;
    }

    private static String require(Map<String, String> options, String key) {
        String value = options.get(key);
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(key + " is required");
        }
        return value;
    }
}
