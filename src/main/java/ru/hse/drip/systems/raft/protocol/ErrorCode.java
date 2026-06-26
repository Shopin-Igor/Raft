package ru.hse.drip.systems.raft.protocol;

public enum ErrorCode {
    NOT_LEADER,
    NO_LEADER,
    TIMEOUT,
    BAD_REQUEST,
    UNKNOWN_NODE,
    UNAVAILABLE
}
