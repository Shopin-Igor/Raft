package ru.hse.drip.systems.raft.transport;

import ru.hse.drip.systems.raft.protocol.WireMessage;

@FunctionalInterface
public interface MessageHandler {
    WireMessage handle(WireMessage request);
}
