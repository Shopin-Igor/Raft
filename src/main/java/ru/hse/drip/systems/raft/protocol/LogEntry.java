package ru.hse.drip.systems.raft.protocol;

public class LogEntry {
    public int term;
    public String command;

    public LogEntry() {
    }

    public LogEntry(int term, String command) {
        this.term = term;
        this.command = command;
    }
}
