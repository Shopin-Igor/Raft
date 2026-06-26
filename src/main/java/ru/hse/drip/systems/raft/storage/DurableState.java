package ru.hse.drip.systems.raft.storage;

import ru.hse.drip.systems.raft.protocol.LogEntry;

import java.io.IOException;
import java.util.List;

public interface DurableState {
    PersistentState readState() throws IOException;

    void saveMeta(int currentTerm, String votedFor) throws IOException;

    void appendLog(LogEntry entry) throws IOException;

    void replaceLog(List<LogEntry> entries) throws IOException;
}
