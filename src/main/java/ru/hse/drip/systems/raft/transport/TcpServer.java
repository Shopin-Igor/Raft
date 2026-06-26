package ru.hse.drip.systems.raft.transport;

import ru.hse.drip.systems.raft.protocol.ErrorCode;
import ru.hse.drip.systems.raft.protocol.JsonCodec;
import ru.hse.drip.systems.raft.protocol.WireMessage;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;

public class TcpServer implements AutoCloseable {
    private final int port;
    private final MessageHandler handler;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final ExecutorService workers;
    private ServerSocket serverSocket;
    private Thread acceptThread;

    public TcpServer(int port, MessageHandler handler) {
        this.port = port;
        this.handler = Objects.requireNonNull(handler);
        this.workers = Executors.newCachedThreadPool(new NamedThreadFactory("tcp-worker-" + port + "-"));
    }

    public void start() throws IOException {
        serverSocket = new ServerSocket(port);
        running.set(true);
        acceptThread = new Thread(this::acceptLoop, "tcp-accept-" + port);
        acceptThread.setDaemon(false);
        acceptThread.start();
    }

    private void acceptLoop() {
        while (running.get()) {
            try {
                Socket socket = serverSocket.accept();
                workers.submit(() -> serve(socket));
            } catch (IOException e) {
                if (running.get()) {
                    System.err.println("TCP accept failed on " + port + ": " + e.getMessage());
                }
            }
        }
    }

    private void serve(Socket socket) {
        try (socket;
             BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));
             BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream(), StandardCharsets.UTF_8))) {
            socket.setSoTimeout(2_000);
            String line = reader.readLine();
            WireMessage response;
            if (line == null || line.isBlank()) {
                response = WireMessage.error(null, ErrorCode.BAD_REQUEST, "Empty request");
            } else {
                response = dispatch(line);
            }
            writer.write(JsonCodec.encode(response));
            writer.newLine();
            writer.flush();
        } catch (IOException e) {
            if (running.get()) {
                System.err.println("TCP connection failed on " + port + ": " + e.getMessage());
            }
        }
    }

    private WireMessage dispatch(String line) {
        try {
            return handler.handle(JsonCodec.decode(line));
        } catch (IllegalArgumentException e) {
            return WireMessage.error(null, ErrorCode.BAD_REQUEST, e.getMessage());
        } catch (Exception e) {
            return WireMessage.error(null, ErrorCode.UNAVAILABLE, e.getMessage());
        }
    }

    @Override
    public void close() {
        running.set(false);
        try {
            if (serverSocket != null) {
                serverSocket.close();
            }
        } catch (IOException ignored) {
        }
        workers.shutdownNow();
    }

    private static final class NamedThreadFactory implements ThreadFactory {
        private final String prefix;
        private int counter;

        private NamedThreadFactory(String prefix) {
            this.prefix = prefix;
        }

        @Override
        public synchronized Thread newThread(Runnable runnable) {
            Thread thread = new Thread(runnable, prefix + counter++);
            thread.setDaemon(true);
            return thread;
        }
    }
}
