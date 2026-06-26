package ru.hse.drip.systems.raft.protocol;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.ArrayList;
import java.util.List;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class WireMessage {
    public String type;
    public String requestId;

    public Integer term;
    public String candidateId;
    public Integer lastLogIndex;
    public Integer lastLogTerm;
    public String nodeId;
    public Boolean voteGranted;

    public String leaderId;
    public Integer prefixLen;
    public Integer prefixTerm;
    public Integer leaderCommit;
    public List<LogEntry> suffix;
    public Integer ack;
    public Boolean success;

    public ResponseStatus status;
    public ErrorCode error;
    public String message;
    public String leaderHint;
    public String key;
    public String value;
    public String command;

    public Role role;
    public Integer currentTerm;
    public Integer commitIndex;
    public Integer lastApplied;
    public Integer logLength;
    public List<LogEntry> log;
    public List<String> blockedPeers;
    public Long delayMs;
    public Integer electionMinMs;
    public Integer electionMaxMs;
    public Long throughputOpsSec;
    public Double avgMs;
    public Double p50Ms;
    public Double p95Ms;
    public Double p99Ms;
    public Long failoverMs;

    public static WireMessage request(String type) {
        WireMessage message = new WireMessage();
        message.type = type;
        return message;
    }

    public static WireMessage ok(String requestId) {
        WireMessage message = new WireMessage();
        message.type = "RESPONSE";
        message.requestId = requestId;
        message.status = ResponseStatus.OK;
        return message;
    }

    public static WireMessage error(String requestId, ErrorCode code, String details) {
        WireMessage message = new WireMessage();
        message.type = "RESPONSE";
        message.requestId = requestId;
        message.status = ResponseStatus.ERROR;
        message.error = code;
        message.message = details;
        return message;
    }

    public List<LogEntry> suffixOrEmpty() {
        if (suffix == null) {
            return List.of();
        }
        return suffix;
    }

    public List<String> blockedPeersOrEmpty() {
        if (blockedPeers == null) {
            return List.of();
        }
        return blockedPeers;
    }

    public WireMessage withType(String type) {
        this.type = type;
        return this;
    }

    public WireMessage copyLog(List<LogEntry> entries) {
        this.log = new ArrayList<>(entries);
        return this;
    }
}
