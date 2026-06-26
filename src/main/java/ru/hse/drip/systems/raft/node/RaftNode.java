package ru.hse.drip.systems.raft.node;

import ru.hse.drip.systems.raft.protocol.ErrorCode;
import ru.hse.drip.systems.raft.protocol.LogEntry;
import ru.hse.drip.systems.raft.protocol.NodeEndpoint;
import ru.hse.drip.systems.raft.protocol.Role;
import ru.hse.drip.systems.raft.protocol.WireMessage;
import ru.hse.drip.systems.raft.storage.DurableState;
import ru.hse.drip.systems.raft.storage.FileDurableState;
import ru.hse.drip.systems.raft.storage.PersistentState;
import ru.hse.drip.systems.raft.transport.TcpServer;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class RaftNode {
    private static final int HEARTBEAT_MS = 50;
    private static final int CLIENT_TIMEOUT_MS = 10_000;

    private final Object lock = new Object();
    private final String nodeId;
    private final Map<String, NodeEndpoint> endpoints;
    private final DurableState storage;
    private final RpcClient rpcClient;
    private final KeyValueStateMachine stateMachine = new KeyValueStateMachine();
    private final PendingClientRequests pendingRequests = new PendingClientRequests();
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(2);
    private final ExecutorService rpcExecutor = Executors.newCachedThreadPool();
    private final Random random = new Random();

    private TcpServer server;
    private Role role = Role.FOLLOWER;
    private int currentTerm;
    private String votedFor;
    private String leaderHint;
    private List<LogEntry> log = new ArrayList<>();
    private int commitIndex;
    private int lastApplied;
    private final Map<String, Integer> sentLength = new HashMap<>();
    private final Map<String, Integer> ackedLength = new HashMap<>();
    private final Set<String> votesGranted = new HashSet<>();
    private final Set<String> blockedPeers = new HashSet<>();
    private long outgoingDelayMs;
    private int electionMinMs = 150;
    private int electionMaxMs = 300;
    private long electionDeadlineNanos;

    public RaftNode(NodeConfig config) {
        this(
                config,
                new FileDurableState(config.dataDir(), config.nodeId(), config.fsync()),
                new TcpRpcClient(Duration.ofMillis(250), Duration.ofMillis(750)));
    }

    public RaftNode(NodeConfig config, DurableState storage, RpcClient rpcClient) {
        this.nodeId = config.nodeId();
        this.endpoints = new LinkedHashMap<>(config.peers());
        this.storage = Objects.requireNonNull(storage);
        this.rpcClient = Objects.requireNonNull(rpcClient);
        this.server = new TcpServer(config.port(), this::handle);
    }

    public void start() throws IOException {
        PersistentState state = storage.readState();
        synchronized (lock) {
            currentTerm = state.currentTerm();
            votedFor = state.votedFor();
            log = state.log();
            role = Role.FOLLOWER;
            leaderHint = null;
            commitIndex = 0;
            lastApplied = 0;
            stateMachine.clear();
            resetElectionTimeoutLocked();
        }
        server.start();
        scheduler.scheduleAtFixedRate(this::tickElection, 20, 20, TimeUnit.MILLISECONDS);
        scheduler.scheduleAtFixedRate(this::tickHeartbeat, HEARTBEAT_MS, HEARTBEAT_MS, TimeUnit.MILLISECONDS);
        System.out.printf("[%s] started at term=%d logLength=%d%n", nodeId, currentTerm, log.size());
    }

    public void stop() {
        scheduler.shutdownNow();
        rpcExecutor.shutdownNow();
        if (server != null) {
            server.close();
        }
    }

    private WireMessage handle(WireMessage request) {
        if (request == null || request.type == null) {
            return WireMessage.error(null, ErrorCode.BAD_REQUEST, "Missing message type");
        }
        if (isBlockedInbound(request)) {
            return WireMessage.error(request.requestId, ErrorCode.TIMEOUT, "Partitioned from sender");
        }
        return switch (request.type) {
            case "REQUEST_VOTE" -> handleRequestVote(request);
            case "APPEND_ENTRIES" -> handleAppendEntries(request);
            case "CLIENT_PUT" -> handleClientPut(request);
            case "CLIENT_GET" -> handleClientGet(request);
            case "LEADER_QUERY" -> handleLeaderQuery(request);
            case "STATUS_QUERY" -> handleStatus(request);
            case "LOG_DUMP" -> handleLogDump(request);
            case "KILL" -> handleKill(request);
            case "SET_PARTITION" -> handleSetPartition(request);
            case "HEAL" -> handleHeal(request);
            case "SET_DELAY" -> handleSetDelay(request);
            case "SET_ELECTION_TIMEOUT" -> handleSetElectionTimeout(request);
            default -> WireMessage.error(request.requestId, ErrorCode.BAD_REQUEST, "Unknown message type " + request.type);
        };
    }

    private WireMessage handleRequestVote(WireMessage request) {
        synchronized (lock) {
            int term = valueOrZero(request.term);
            try {
                if (term > currentTerm) {
                    becomeFollowerLocked(term, null, true);
                }
                WireMessage response = WireMessage.request("REQUEST_VOTE_RESP");
                response.nodeId = nodeId;
                response.term = currentTerm;
                response.voteGranted = false;
                if (term < currentTerm) {
                    return response;
                }
                boolean notVotedYet = votedFor == null || Objects.equals(votedFor, request.candidateId);
                boolean upToDate = isCandidateLogUpToDate(
                        valueOrZero(request.lastLogTerm),
                        valueOrZero(request.lastLogIndex));
                if (notVotedYet && upToDate) {
                    votedFor = request.candidateId;
                    storage.saveMeta(currentTerm, votedFor);
                    resetElectionTimeoutLocked();
                    response.voteGranted = true;
                }
                response.term = currentTerm;
                return response;
            } catch (IOException e) {
                return WireMessage.error(request.requestId, ErrorCode.UNAVAILABLE, e.getMessage());
            }
        }
    }

    private WireMessage handleAppendEntries(WireMessage request) {
        synchronized (lock) {
            int term = valueOrZero(request.term);
            try {
                if (term > currentTerm) {
                    becomeFollowerLocked(term, request.leaderId, true);
                }
                WireMessage response = WireMessage.request("APPEND_ENTRIES_RESP");
                response.nodeId = nodeId;
                response.term = currentTerm;
                response.ack = 0;
                response.success = false;
                if (term < currentTerm) {
                    return response;
                }

                if (term == currentTerm) {
                    if (role != Role.FOLLOWER) {
                        failPendingLocked(ErrorCode.NOT_LEADER, "Stepped down after AppendEntries");
                    }
                    role = Role.FOLLOWER;
                    leaderHint = request.leaderId;
                    resetElectionTimeoutLocked();
                }

                int prefixLen = valueOrZero(request.prefixLen);
                int prefixTerm = valueOrZero(request.prefixTerm);
                boolean logOk = log.size() >= prefixLen
                        && (prefixLen == 0 || log.get(prefixLen - 1).term == prefixTerm);
                if (!logOk) {
                    response.term = currentTerm;
                    return response;
                }

                appendEntriesLocalLocked(prefixLen, valueOrZero(request.leaderCommit), request.suffixOrEmpty());
                response.term = currentTerm;
                response.ack = prefixLen + request.suffixOrEmpty().size();
                response.success = true;
                return response;
            } catch (IOException e) {
                return WireMessage.error(request.requestId, ErrorCode.UNAVAILABLE, e.getMessage());
            }
        }
    }

    private WireMessage handleClientPut(WireMessage request) {
        CompletableFuture<WireMessage> future;
        int appendedIndex;
        synchronized (lock) {
            if (role != Role.LEADER) {
                return notLeader(request.requestId);
            }
            String command = request.command;
            if (command == null || command.isBlank()) {
                if (request.key == null || request.value == null) {
                    return WireMessage.error(request.requestId, ErrorCode.BAD_REQUEST, "put requires key and value");
                }
                command = "put " + request.key + " " + request.value;
            }
            try {
                LogEntry entry = new LogEntry(currentTerm, command);
                storage.appendLog(entry);
                log.add(entry);
                appendedIndex = log.size();
                ackedLength.put(nodeId, log.size());
                sentLength.put(nodeId, log.size());
                future = pendingRequests.registerPut(appendedIndex);
                System.out.printf("[%s] appended index=%d term=%d command=%s%n", nodeId, appendedIndex, currentTerm, command);
            } catch (IOException e) {
                return WireMessage.error(request.requestId, ErrorCode.UNAVAILABLE, e.getMessage());
            }
        }
        replicateToAllFollowers();
        try {
            WireMessage response = future.get(CLIENT_TIMEOUT_MS, TimeUnit.MILLISECONDS);
            response.requestId = request.requestId;
            return response;
        } catch (Exception e) {
            return WireMessage.error(request.requestId, ErrorCode.TIMEOUT, "Commit timeout");
        }
    }

    private WireMessage handleClientGet(WireMessage request) {
        synchronized (lock) {
            if (role != Role.LEADER) {
                return notLeader(request.requestId);
            }
            if (request.key == null || request.key.isBlank()) {
                return WireMessage.error(request.requestId, ErrorCode.BAD_REQUEST, "get requires key");
            }
            WireMessage response = WireMessage.ok(request.requestId);
            response.value = stateMachine.get(request.key);
            response.leaderHint = nodeId;
            response.term = currentTerm;
            return response;
        }
    }

    private WireMessage handleLeaderQuery(WireMessage request) {
        synchronized (lock) {
            if (role == Role.LEADER) {
                WireMessage response = WireMessage.ok(request.requestId);
                response.leaderHint = nodeId;
                response.role = role;
                response.term = currentTerm;
                return response;
            }
            WireMessage response = WireMessage.error(request.requestId, ErrorCode.NOT_LEADER, "Known leader is " + leaderHint);
            response.leaderHint = leaderHint;
            response.role = role;
            response.term = currentTerm;
            return response;
        }
    }

    private WireMessage handleStatus(WireMessage request) {
        synchronized (lock) {
            WireMessage response = WireMessage.ok(request.requestId);
            response.nodeId = nodeId;
            response.role = role;
            response.currentTerm = currentTerm;
            response.term = currentTerm;
            response.commitIndex = commitIndex;
            response.lastApplied = lastApplied;
            response.logLength = log.size();
            response.leaderHint = leaderHint;
            response.blockedPeers = new ArrayList<>(blockedPeers);
            response.delayMs = outgoingDelayMs;
            return response;
        }
    }

    private WireMessage handleLogDump(WireMessage request) {
        synchronized (lock) {
            WireMessage response = WireMessage.ok(request.requestId);
            response.nodeId = nodeId;
            response.currentTerm = currentTerm;
            response.commitIndex = commitIndex;
            response.copyLog(log);
            return response;
        }
    }

    private WireMessage handleKill(WireMessage request) {
        WireMessage response = WireMessage.ok(request.requestId);
        response.message = "Node " + nodeId + " exits";
        Thread killer = new Thread(() -> {
            try {
                Thread.sleep(100);
            } catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
            }
            System.exit(0);
        }, "raft-kill");
        killer.setDaemon(false);
        killer.start();
        return response;
    }

    private WireMessage handleSetPartition(WireMessage request) {
        synchronized (lock) {
            blockedPeers.clear();
            blockedPeers.addAll(request.blockedPeersOrEmpty());
            WireMessage response = WireMessage.ok(request.requestId);
            response.nodeId = nodeId;
            response.blockedPeers = new ArrayList<>(blockedPeers);
            return response;
        }
    }

    private WireMessage handleHeal(WireMessage request) {
        synchronized (lock) {
            blockedPeers.clear();
            resetElectionTimeoutLocked();
            WireMessage response = WireMessage.ok(request.requestId);
            response.nodeId = nodeId;
            return response;
        }
    }

    private WireMessage handleSetDelay(WireMessage request) {
        synchronized (lock) {
            outgoingDelayMs = Math.max(0L, request.delayMs == null ? 0L : request.delayMs);
            WireMessage response = WireMessage.ok(request.requestId);
            response.nodeId = nodeId;
            response.delayMs = outgoingDelayMs;
            return response;
        }
    }

    private WireMessage handleSetElectionTimeout(WireMessage request) {
        synchronized (lock) {
            int min = request.electionMinMs == null ? electionMinMs : request.electionMinMs;
            int max = request.electionMaxMs == null ? electionMaxMs : request.electionMaxMs;
            if (min <= 0 || max < min) {
                return WireMessage.error(request.requestId, ErrorCode.BAD_REQUEST, "Bad election timeout range");
            }
            electionMinMs = min;
            electionMaxMs = max;
            resetElectionTimeoutLocked();
            WireMessage response = WireMessage.ok(request.requestId);
            response.nodeId = nodeId;
            response.electionMinMs = electionMinMs;
            response.electionMaxMs = electionMaxMs;
            return response;
        }
    }

    private void tickElection() {
        boolean shouldStart;
        synchronized (lock) {
            shouldStart = role != Role.LEADER && System.nanoTime() >= electionDeadlineNanos;
        }
        if (shouldStart) {
            startElection();
        }
    }

    private void startElection() {
        WireMessage voteRequest;
        synchronized (lock) {
            try {
                role = Role.CANDIDATE;
                currentTerm += 1;
                votedFor = nodeId;
                leaderHint = null;
                storage.saveMeta(currentTerm, votedFor);
                votesGranted.clear();
                votesGranted.add(nodeId);
                resetElectionTimeoutLocked();
                voteRequest = WireMessage.request("REQUEST_VOTE");
                voteRequest.requestId = UUID.randomUUID().toString();
                voteRequest.term = currentTerm;
                voteRequest.candidateId = nodeId;
                voteRequest.lastLogIndex = log.size();
                voteRequest.lastLogTerm = lastLogTermLocked();
                System.out.printf("[%s] starts election term=%d lastLog=%d/%d%n",
                        nodeId, currentTerm, voteRequest.lastLogIndex, voteRequest.lastLogTerm);
            } catch (IOException e) {
                System.err.printf("[%s] cannot persist election term: %s%n", nodeId, e.getMessage());
                return;
            }
        }
        if (hasMajority(1)) {
            synchronized (lock) {
                becomeLeaderLocked();
            }
            return;
        }
        for (String peerId : peerIds()) {
            sendRpc(peerId, voteRequest, response -> handleRequestVoteResponse(peerId, response));
        }
    }

    private void handleRequestVoteResponse(String peerId, WireMessage response) {
        if (response == null) {
            return;
        }
        synchronized (lock) {
            int responseTerm = valueOrZero(response.term);
            try {
                if (responseTerm > currentTerm) {
                    becomeFollowerLocked(responseTerm, null, true);
                    return;
                }
            } catch (IOException e) {
                System.err.printf("[%s] cannot persist higher term: %s%n", nodeId, e.getMessage());
                return;
            }
            if (role != Role.CANDIDATE || responseTerm != currentTerm) {
                return;
            }
            if (Boolean.TRUE.equals(response.voteGranted)) {
                votesGranted.add(peerId);
                if (hasMajority(votesGranted.size())) {
                    becomeLeaderLocked();
                }
            }
        }
    }

    private void tickHeartbeat() {
        boolean isLeader;
        synchronized (lock) {
            isLeader = role == Role.LEADER;
        }
        if (isLeader) {
            replicateToAllFollowers();
        }
    }

    private void replicateToAllFollowers() {
        for (String peerId : peerIds()) {
            replicateLog(peerId);
        }
    }

    private void replicateLog(String peerId) {
        WireMessage request;
        synchronized (lock) {
            if (role != Role.LEADER) {
                return;
            }
            int prefixLen = sentLength.getOrDefault(peerId, log.size());
            prefixLen = Math.max(0, Math.min(prefixLen, log.size()));
            List<LogEntry> suffix = new ArrayList<>(log.subList(prefixLen, log.size()));
            request = WireMessage.request("APPEND_ENTRIES");
            request.requestId = UUID.randomUUID().toString();
            request.term = currentTerm;
            request.leaderId = nodeId;
            request.nodeId = nodeId;
            request.prefixLen = prefixLen;
            request.prefixTerm = prefixLen > 0 ? log.get(prefixLen - 1).term : 0;
            request.leaderCommit = commitIndex;
            request.suffix = suffix;
        }
        sendRpc(peerId, request, response -> handleAppendEntriesResponse(peerId, response));
    }

    private void handleAppendEntriesResponse(String peerId, WireMessage response) {
        if (response == null) {
            return;
        }
        synchronized (lock) {
            int responseTerm = valueOrZero(response.term);
            try {
                if (responseTerm > currentTerm) {
                    becomeFollowerLocked(responseTerm, null, true);
                    return;
                }
            } catch (IOException e) {
                System.err.printf("[%s] cannot persist higher term: %s%n", nodeId, e.getMessage());
                return;
            }
            if (role != Role.LEADER || responseTerm != currentTerm) {
                return;
            }
            if (Boolean.TRUE.equals(response.success)
                    && response.ack != null
                    && response.ack >= ackedLength.getOrDefault(peerId, 0)) {
                sentLength.put(peerId, response.ack);
                ackedLength.put(peerId, response.ack);
                commitLogEntriesLocked();
            } else if (sentLength.getOrDefault(peerId, 0) > 0) {
                sentLength.put(peerId, sentLength.get(peerId) - 1);
                replicateLog(peerId);
            }
        }
    }

    private void appendEntriesLocalLocked(int prefixLen, int leaderCommit, List<LogEntry> suffix) throws IOException {
        boolean rewritten = false;
        if (!suffix.isEmpty() && log.size() > prefixLen) {
            int index = Math.min(log.size(), prefixLen + suffix.size()) - 1;
            if (index >= prefixLen && log.get(index).term != suffix.get(index - prefixLen).term) {
                log = new ArrayList<>(log.subList(0, prefixLen));
                storage.replaceLog(log);
                rewritten = true;
                if (commitIndex > log.size()) {
                    commitIndex = log.size();
                }
                if (lastApplied > log.size()) {
                    lastApplied = log.size();
                }
            }
        }
        if (prefixLen + suffix.size() > log.size()) {
            for (int i = log.size() - prefixLen; i < suffix.size(); i++) {
                LogEntry entry = suffix.get(i);
                storage.appendLog(entry);
                log.add(new LogEntry(entry.term, entry.command));
            }
        } else if (rewritten) {
            storage.replaceLog(log);
        }
        if (leaderCommit > commitIndex) {
            applyCommittedLocked(Math.min(leaderCommit, log.size()));
        }
    }

    private void commitLogEntriesLocked() {
        int newCommit = commitIndex;
        for (int len = log.size(); len > commitIndex; len--) {
            int ackCount = 0;
            for (String memberId : endpoints.keySet()) {
                if (ackedLength.getOrDefault(memberId, 0) >= len) {
                    ackCount++;
                }
            }
            if (hasMajority(ackCount) && log.get(len - 1).term == currentTerm) {
                newCommit = len;
                break;
            }
        }
        if (newCommit > commitIndex) {
            applyCommittedLocked(newCommit);
            completePendingLocked(newCommit);
            System.out.printf("[%s] commitIndex=%d term=%d%n", nodeId, commitIndex, currentTerm);
        }
    }

    private void applyCommittedLocked(int newCommit) {
        while (lastApplied < newCommit) {
            LogEntry entry = log.get(lastApplied);
            applyCommandLocked(entry.command);
            lastApplied++;
        }
        commitIndex = newCommit;
    }

    private void applyCommandLocked(String command) {
        stateMachine.apply(command);
    }

    private void completePendingLocked(int newCommit) {
        pendingRequests.completeCommitted(newCommit, nodeId, currentTerm);
    }

    private void becomeLeaderLocked() {
        if (role == Role.LEADER) {
            return;
        }
        role = Role.LEADER;
        leaderHint = nodeId;
        votesGranted.clear();
        sentLength.clear();
        ackedLength.clear();
        for (String memberId : endpoints.keySet()) {
            sentLength.put(memberId, log.size());
            ackedLength.put(memberId, 0);
        }
        sentLength.put(nodeId, log.size());
        ackedLength.put(nodeId, log.size());
        System.out.printf("[%s] became LEADER term=%d logLength=%d%n", nodeId, currentTerm, log.size());
        replicateToAllFollowers();
    }

    private void becomeFollowerLocked(int newTerm, String newLeaderHint, boolean persist) throws IOException {
        if (newTerm > currentTerm) {
            currentTerm = newTerm;
            votedFor = null;
            if (persist) {
                storage.saveMeta(currentTerm, votedFor);
            }
        }
        if (role == Role.LEADER || role == Role.CANDIDATE) {
            failPendingLocked(ErrorCode.NOT_LEADER, "Node is no longer leader");
        }
        role = Role.FOLLOWER;
        leaderHint = newLeaderHint;
        votesGranted.clear();
        sentLength.clear();
        ackedLength.clear();
        resetElectionTimeoutLocked();
    }

    private void failPendingLocked(ErrorCode code, String details) {
        pendingRequests.failAll(code, details, leaderHint, currentTerm);
    }

    private void sendRpc(String peerId, WireMessage request, RpcCallback callback) {
        NodeEndpoint endpoint = endpoints.get(peerId);
        if (endpoint == null || peerId.equals(nodeId)) {
            return;
        }
        synchronized (lock) {
            if (blockedPeers.contains(peerId)) {
                return;
            }
        }
        rpcExecutor.submit(() -> {
            try {
                long delay;
                synchronized (lock) {
                    delay = outgoingDelayMs;
                }
                if (delay > 0) {
                    Thread.sleep(delay);
                }
                WireMessage response = rpcClient.request(endpoint, request);
                callback.onResponse(response);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (Exception ignored) {
                callback.onResponse(null);
            }
        });
    }

    private boolean isBlockedInbound(WireMessage request) {
        String source = switch (request.type) {
            case "REQUEST_VOTE" -> request.candidateId;
            case "APPEND_ENTRIES" -> request.leaderId;
            default -> null;
        };
        if (source == null) {
            return false;
        }
        synchronized (lock) {
            return blockedPeers.contains(source);
        }
    }

    private WireMessage notLeader(String requestId) {
        WireMessage response = WireMessage.error(requestId, leaderHint == null ? ErrorCode.NO_LEADER : ErrorCode.NOT_LEADER,
                leaderHint == null ? "Leader is unknown" : "Leader is " + leaderHint);
        response.leaderHint = leaderHint;
        response.term = currentTerm;
        return response;
    }

    private boolean isCandidateLogUpToDate(int candidateLastTerm, int candidateLastIndex) {
        int localLastTerm = lastLogTermLocked();
        return candidateLastTerm > localLastTerm
                || (candidateLastTerm == localLastTerm && candidateLastIndex >= log.size());
    }

    private int lastLogTermLocked() {
        return log.isEmpty() ? 0 : log.get(log.size() - 1).term;
    }

    private void resetElectionTimeoutLocked() {
        int spread = Math.max(1, electionMaxMs - electionMinMs + 1);
        int timeout = electionMinMs + random.nextInt(spread);
        electionDeadlineNanos = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeout);
    }

    private boolean hasMajority(int votes) {
        return votes >= endpoints.size() / 2 + 1;
    }

    private List<String> peerIds() {
        return endpoints.keySet().stream()
                .filter(peer -> !peer.equals(nodeId))
                .toList();
    }

    private static int valueOrZero(Integer value) {
        return value == null ? 0 : value;
    }

    @FunctionalInterface
    private interface RpcCallback {
        void onResponse(WireMessage response);
    }
}
