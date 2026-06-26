package ru.hse.drip.systems.raft.transport;

import ru.hse.drip.systems.raft.protocol.JsonCodec;
import ru.hse.drip.systems.raft.protocol.NodeEndpoint;
import ru.hse.drip.systems.raft.protocol.WireMessage;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.time.Duration;

public class TcpClient {
    private final int connectTimeoutMs;
    private final int readTimeoutMs;

    public TcpClient(Duration connectTimeout, Duration readTimeout) {
        this.connectTimeoutMs = Math.toIntExact(connectTimeout.toMillis());
        this.readTimeoutMs = Math.toIntExact(readTimeout.toMillis());
    }

    public WireMessage request(NodeEndpoint endpoint, WireMessage message) throws IOException {
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(endpoint.host, endpoint.port), connectTimeoutMs);
            socket.setSoTimeout(readTimeoutMs);
            BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream(), StandardCharsets.UTF_8));
            BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));

            writer.write(JsonCodec.encode(message));
            writer.newLine();
            writer.flush();

            String line = reader.readLine();
            if (line == null || line.isBlank()) {
                throw new IOException("No response from " + endpoint);
            }
            return JsonCodec.decode(line);
        }
    }
}
