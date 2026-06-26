package ru.hse.drip.systems.raft.node;

import ru.hse.drip.systems.raft.protocol.ErrorCode;
import ru.hse.drip.systems.raft.protocol.WireMessage;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class PendingClientRequests {
    private final Map<Integer, List<CompletableFuture<WireMessage>>> pendingPuts = new HashMap<>();

    public CompletableFuture<WireMessage> registerPut(int logIndex) {
        CompletableFuture<WireMessage> future = new CompletableFuture<>();
        pendingPuts.computeIfAbsent(logIndex, ignored -> new ArrayList<>()).add(future);
        return future;
    }

    public void completeCommitted(int commitIndex, String leaderId, int currentTerm) {
        List<Integer> committed = pendingPuts.keySet().stream()
                .filter(index -> index <= commitIndex)
                .sorted(Comparator.naturalOrder())
                .toList();
        for (Integer index : committed) {
            List<CompletableFuture<WireMessage>> futures = pendingPuts.remove(index);
            if (futures == null) {
                continue;
            }
            for (CompletableFuture<WireMessage> future : futures) {
                WireMessage response = WireMessage.ok(null);
                response.leaderHint = leaderId;
                response.term = currentTerm;
                response.commitIndex = commitIndex;
                future.complete(response);
            }
        }
    }

    public void failAll(ErrorCode code, String details, String leaderHint, int currentTerm) {
        for (List<CompletableFuture<WireMessage>> futures : pendingPuts.values()) {
            for (CompletableFuture<WireMessage> future : futures) {
                WireMessage response = WireMessage.error(null, code, details);
                response.leaderHint = leaderHint;
                response.term = currentTerm;
                future.complete(response);
            }
        }
        pendingPuts.clear();
    }
}
