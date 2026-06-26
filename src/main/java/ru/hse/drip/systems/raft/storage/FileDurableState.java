package ru.hse.drip.systems.raft.storage;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import ru.hse.drip.systems.raft.protocol.LogEntry;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;

public class FileDurableState implements DurableState {
    private static final ObjectMapper MAPPER = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    private final Path nodeDir;
    private final Path metaFile;
    private final Path logFile;
    private final boolean fsync;

    public FileDurableState(Path dataDir, String nodeId, boolean fsync) {
        this.nodeDir = dataDir.resolve(nodeId);
        this.metaFile = nodeDir.resolve("meta.json");
        this.logFile = nodeDir.resolve("log.jsonl");
        this.fsync = fsync;
    }

    @Override
    public synchronized PersistentState readState() throws IOException {
        Files.createDirectories(nodeDir);
        Meta meta = readMeta();
        List<LogEntry> entries = readLog();
        return new PersistentState(meta.currentTerm, meta.votedFor, entries);
    }

    @Override
    public synchronized void saveMeta(int currentTerm, String votedFor) throws IOException {
        Files.createDirectories(nodeDir);
        Meta meta = new Meta();
        meta.currentTerm = currentTerm;
        meta.votedFor = votedFor;
        Path tmp = nodeDir.resolve("meta.json.tmp");
        writeBytes(tmp, MAPPER.writeValueAsBytes(meta), true);
        Files.move(tmp, metaFile, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        forceDirectory();
    }

    @Override
    public synchronized void appendLog(LogEntry entry) throws IOException {
        Files.createDirectories(nodeDir);
        String line = encode(entry) + System.lineSeparator();
        try (FileChannel channel = FileChannel.open(
                logFile,
                StandardOpenOption.CREATE,
                StandardOpenOption.WRITE,
                StandardOpenOption.APPEND)) {
            channel.write(ByteBuffer.wrap(line.getBytes(StandardCharsets.UTF_8)));
            if (fsync) {
                channel.force(true);
            }
        }
    }

    @Override
    public synchronized void replaceLog(List<LogEntry> entries) throws IOException {
        Files.createDirectories(nodeDir);
        Path tmp = nodeDir.resolve("log.jsonl.tmp");
        StringBuilder builder = new StringBuilder();
        for (LogEntry entry : entries) {
            builder.append(encode(entry)).append(System.lineSeparator());
        }
        writeBytes(tmp, builder.toString().getBytes(StandardCharsets.UTF_8), true);
        Files.move(tmp, logFile, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        forceDirectory();
    }

    private Meta readMeta() throws IOException {
        if (!Files.exists(metaFile)) {
            Meta meta = new Meta();
            meta.currentTerm = 0;
            meta.votedFor = null;
            saveMeta(0, null);
            return meta;
        }
        return MAPPER.readValue(Files.readString(metaFile, StandardCharsets.UTF_8), Meta.class);
    }

    private List<LogEntry> readLog() throws IOException {
        List<LogEntry> entries = new ArrayList<>();
        if (!Files.exists(logFile)) {
            Files.createFile(logFile);
            return entries;
        }
        try (BufferedReader reader = Files.newBufferedReader(logFile, StandardCharsets.UTF_8)) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (!line.isBlank()) {
                    entries.add(MAPPER.readValue(line, LogEntry.class));
                }
            }
        }
        return entries;
    }

    private void writeBytes(Path file, byte[] bytes, boolean force) throws IOException {
        try (FileChannel channel = FileChannel.open(
                file,
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING,
                StandardOpenOption.WRITE)) {
            channel.write(ByteBuffer.wrap(bytes));
            if (fsync && force) {
                channel.force(true);
            }
        }
    }

    private void forceDirectory() {
        if (!fsync) {
            return;
        }
        try (FileChannel channel = FileChannel.open(nodeDir, StandardOpenOption.READ)) {
            channel.force(true);
        } catch (IOException ignored) {
            // Some file systems do not allow opening directories. File data was already forced.
        }
    }

    private static String encode(LogEntry entry) {
        try {
            return MAPPER.writeValueAsString(entry);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Cannot encode log entry", e);
        }
    }

    public static class Meta {
        public int currentTerm;
        public String votedFor;
    }
}
