package ru.hse.drip.systems.raft.storage;

import ru.hse.drip.systems.raft.protocol.LogEntry;

import java.util.ArrayList;
import java.util.List;

public class PersistentState {
    private final int currentTerm;
    private final String votedFor;
    private final List<LogEntry> log;

    public PersistentState(int currentTerm, String votedFor, List<LogEntry> log) {
        this.currentTerm = currentTerm;
        this.votedFor = votedFor;
        this.log = new ArrayList<>(log);
    }

    public int currentTerm() {
        return currentTerm;
    }

    public String votedFor() {
        return votedFor;
    }

    public List<LogEntry> log() {
        return new ArrayList<>(log);
    }
}
