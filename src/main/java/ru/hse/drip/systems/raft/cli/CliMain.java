package ru.hse.drip.systems.raft.cli;

import ru.hse.drip.systems.raft.node.NodeConfig;
import ru.hse.drip.systems.raft.node.RaftNodeMain;
import ru.hse.drip.systems.raft.protocol.ErrorCode;
import ru.hse.drip.systems.raft.protocol.LogEntry;
import ru.hse.drip.systems.raft.protocol.NodeEndpoint;
import ru.hse.drip.systems.raft.protocol.ResponseStatus;
import ru.hse.drip.systems.raft.protocol.Role;
import ru.hse.drip.systems.raft.protocol.WireMessage;
import ru.hse.drip.systems.raft.transport.TcpClient;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

public final class CliMain {
    private final Map<String, NodeEndpoint> endpoints;
    private final TcpClient client = new TcpClient(Duration.ofMillis(300), Duration.ofMillis(1_500));
    private final Map<String, Process> ownedProcesses = new HashMap<>();
    private final String peersSpec;
    private final String dataDir;
    private final String fsync;

    private CliMain(String peersSpec, String dataDir, String fsync) {
        this.peersSpec = peersSpec;
        this.endpoints = new LinkedHashMap<>(NodeConfig.parsePeers(peersSpec));
        this.dataDir = dataDir;
        this.fsync = fsync;
    }

    public static void main(String[] args) {
        Map<String, String> options = extractGlobalOptions(args);
        CliMain cli = new CliMain(
                options.getOrDefault("--peers", NodeConfig.defaultPeers()),
                options.getOrDefault("--data-dir", "data"),
                options.getOrDefault("--fsync", "on"));
        List<String> commandArgs = stripGlobalOptions(args);
        if (commandArgs.isEmpty()) {
            cli.repl();
        } else {
            cli.execute(commandArgs);
        }
    }

    private void repl() {
        System.out.println("Raft CLI. Type help for commands.");
        try (Scanner scanner = new Scanner(System.in)) {
            while (true) {
                System.out.print("raft> ");
                if (!scanner.hasNextLine()) {
                    break;
                }
                String line = scanner.nextLine().trim();
                if (line.isBlank()) {
                    continue;
                }
                if ("exit".equalsIgnoreCase(line) || "quit".equalsIgnoreCase(line)) {
                    break;
                }
                execute(splitCommand(line));
            }
        }
    }

    private void execute(List<String> args) {
        if (args.isEmpty()) {
            return;
        }
        try {
            switch (args.get(0)) {
                case "help" -> printHelp();
                case "put" -> put(args);
                case "get" -> get(args);
                case "leader" -> leader();
                case "status" -> status();
                case "logDump" -> logDump(args);
                case "killNode" -> killNode(requireArg(args, 1, "nodeId"));
                case "startNode" -> startNode(requireArg(args, 1, "nodeId"));
                case "startCluster" -> startCluster();
                case "partition" -> partition(requireArg(args, 1, "group1"), requireArg(args, 2, "group2"));
                case "healPartition" -> healPartition();
                case "setDelay" -> setDelay(requireArg(args, 1, "nodeId"), Long.parseLong(requireArg(args, 2, "ms")));
                case "setElectionTimeout" -> setElectionTimeout(
                        Integer.parseInt(requireArg(args, 1, "minMs")),
                        Integer.parseInt(requireArg(args, 2, "maxMs")));
                case "bench" -> bench(args.subList(1, args.size()));
                default -> System.out.println("Unknown command: " + args.get(0));
            }
        } catch (Exception e) {
            System.out.println("ERROR: " + e.getMessage());
        }
    }

    private void printHelp() {
        System.out.println("""
                Commands:
                  put <key> <value>
                  get <key>
                  leader
                  status
                  logDump [--node <id>]
                  killNode <nodeId>
                  startNode <nodeId>
                  startCluster
                  partition <n1,n2> <n3,n4,n5>
                  healPartition
                  setDelay <nodeId> <ms>
                  setElectionTimeout <minMs> <maxMs>
                  bench --puts <N> --threads <T> [--read-ratio <r>]
                  exit
                """);
    }

    private void put(List<String> args) {
        if (args.size() < 3) {
            throw new IllegalArgumentException("put requires key and value");
        }
        String key = args.get(1);
        String value = String.join(" ", args.subList(2, args.size()));
        WireMessage request = WireMessage.request("CLIENT_PUT");
        request.requestId = UUID.randomUUID().toString();
        request.key = key;
        request.value = value;
        request.command = "put " + key + " " + value;
        WireMessage response = sendToLeader(request, Duration.ofSeconds(8));
        if (isOk(response)) {
            System.out.printf("OK commitIndex=%d leader=%s%n", response.commitIndex, response.leaderHint);
        } else {
            printError(response);
        }
    }

    private void get(List<String> args) {
        String key = requireArg(args, 1, "key");
        WireMessage request = WireMessage.request("CLIENT_GET");
        request.requestId = UUID.randomUUID().toString();
        request.key = key;
        WireMessage response = sendToLeader(request, Duration.ofSeconds(5));
        if (isOk(response)) {
            System.out.println(response.value == null ? "null" : response.value);
        } else {
            printError(response);
        }
    }

    private WireMessage sendToLeader(WireMessage request, Duration timeout) {
        long deadline = System.nanoTime() + timeout.toNanos();
        String hintedLeader = null;
        while (System.nanoTime() < deadline) {
            NodeEndpoint leader = hintedLeader == null ? findLeader() : endpoints.get(hintedLeader);
            if (leader != null) {
                WireMessage response = send(leader.nodeId, request);
                if (isOk(response)) {
                    return response;
                }
                if (response != null && response.leaderHint != null && endpoints.containsKey(response.leaderHint)) {
                    hintedLeader = response.leaderHint;
                } else if (response != null && response.error != ErrorCode.NOT_LEADER) {
                    return response;
                } else {
                    hintedLeader = null;
                }
            }
            sleep(120);
        }
        return WireMessage.error(request.requestId, ErrorCode.TIMEOUT, "Leader was not found before timeout");
    }

    private NodeEndpoint findLeader() {
        String hinted = null;
        int hintedTerm = -1;
        for (String nodeId : endpoints.keySet()) {
            WireMessage request = WireMessage.request("LEADER_QUERY");
            request.requestId = UUID.randomUUID().toString();
            WireMessage response = send(nodeId, request);
            if (response == null) {
                continue;
            }
            int term = response.term == null ? 0 : response.term;
            if (isOk(response) && response.role == Role.LEADER) {
                return endpoints.get(response.leaderHint);
            }
            if (response.leaderHint != null && term >= hintedTerm) {
                hinted = response.leaderHint;
                hintedTerm = term;
            }
        }
        return hinted == null ? null : endpoints.get(hinted);
    }

    private void leader() {
        List<WireMessage> responses = new ArrayList<>();
        for (String nodeId : endpoints.keySet()) {
            WireMessage request = WireMessage.request("LEADER_QUERY");
            request.requestId = UUID.randomUUID().toString();
            WireMessage response = send(nodeId, request);
            if (response != null) {
                response.nodeId = nodeId;
                responses.add(response);
            }
        }
        if (responses.isEmpty()) {
            System.out.println("No nodes responded");
            return;
        }
        for (WireMessage response : responses) {
            System.out.printf("%s role=%s term=%s leaderHint=%s status=%s%n",
                    response.nodeId, response.role, response.term, response.leaderHint, response.status);
        }
    }

    private void status() {
        System.out.printf("%-6s %-10s %-6s %-12s %-10s %-12s %-10s%n",
                "node", "role", "term", "commitIndex", "log.length", "leaderHint", "delayMs");
        for (String nodeId : endpoints.keySet()) {
            WireMessage request = WireMessage.request("STATUS_QUERY");
            request.requestId = UUID.randomUUID().toString();
            WireMessage response = send(nodeId, request);
            if (response == null) {
                System.out.printf("%-6s %-10s%n", nodeId, "DOWN");
                continue;
            }
            System.out.printf("%-6s %-10s %-6s %-12s %-10s %-12s %-10s%n",
                    nodeId,
                    response.role,
                    response.currentTerm,
                    response.commitIndex,
                    response.logLength,
                    response.leaderHint,
                    response.delayMs);
        }
    }

    private void logDump(List<String> args) {
        String target = null;
        for (int i = 1; i < args.size(); i++) {
            if ("--node".equals(args.get(i)) && i + 1 < args.size()) {
                target = args.get(i + 1);
            }
        }
        List<String> nodes = target == null ? new ArrayList<>(endpoints.keySet()) : List.of(target);
        for (String nodeId : nodes) {
            WireMessage request = WireMessage.request("LOG_DUMP");
            request.requestId = UUID.randomUUID().toString();
            WireMessage response = send(nodeId, request);
            if (response == null) {
                System.out.println(nodeId + " DOWN");
                continue;
            }
            System.out.printf("== %s term=%d commitIndex=%d ==%n", nodeId, response.currentTerm, response.commitIndex);
            List<LogEntry> entries = response.log == null ? List.of() : response.log;
            for (int i = 0; i < entries.size(); i++) {
                LogEntry entry = entries.get(i);
                System.out.printf("%d term=%d command=%s%n", i + 1, entry.term, entry.command);
            }
        }
    }

    private void killNode(String nodeId) {
        requireKnownNode(nodeId);
        WireMessage request = WireMessage.request("KILL");
        request.requestId = UUID.randomUUID().toString();
        WireMessage response = send(nodeId, request);
        if (isOk(response)) {
            System.out.println("OK killed " + nodeId);
        } else {
            printError(response);
        }
        Process process = ownedProcesses.remove(nodeId);
        if (process != null) {
            process.destroy();
        }
    }

    private void startCluster() throws IOException {
        for (String nodeId : endpoints.keySet()) {
            startNode(nodeId);
        }
    }

    private void startNode(String nodeId) throws IOException {
        requireKnownNode(nodeId);
        Process previous = ownedProcesses.get(nodeId);
        if (previous != null && previous.isAlive()) {
            System.out.println(nodeId + " already started by this CLI");
            return;
        }
        NodeEndpoint endpoint = endpoints.get(nodeId);
        Path logsDir = Path.of("logs");
        Files.createDirectories(logsDir);
        String javaBin = Path.of(System.getProperty("java.home"), "bin", "java").toString();
        List<String> command = List.of(
                javaBin,
                "-cp",
                System.getProperty("java.class.path"),
                RaftNodeMain.class.getName(),
                "--id", nodeId,
                "--port", Integer.toString(endpoint.port),
                "--data-dir", dataDir,
                "--peers", peersSpec,
                "--fsync", fsync);
        Process process = new ProcessBuilder(command)
                .redirectOutput(logsDir.resolve(nodeId + ".out").toFile())
                .redirectError(logsDir.resolve(nodeId + ".err").toFile())
                .start();
        ownedProcesses.put(nodeId, process);
        System.out.printf("OK started %s pid=%d%n", nodeId, process.pid());
    }

    private void partition(String group1Raw, String group2Raw) {
        Set<String> group1 = parseNodeSet(group1Raw);
        Set<String> group2 = parseNodeSet(group2Raw);
        for (String nodeId : endpoints.keySet()) {
            Set<String> blocked = new HashSet<>();
            if (group1.contains(nodeId)) {
                blocked.addAll(group2);
            } else if (group2.contains(nodeId)) {
                blocked.addAll(group1);
            }
            WireMessage request = WireMessage.request("SET_PARTITION");
            request.requestId = UUID.randomUUID().toString();
            request.blockedPeers = new ArrayList<>(blocked);
            WireMessage response = send(nodeId, request);
            System.out.printf("%s blocked=%s status=%s%n", nodeId, blocked, response == null ? "DOWN" : response.status);
        }
    }

    private void healPartition() {
        for (String nodeId : endpoints.keySet()) {
            WireMessage request = WireMessage.request("HEAL");
            request.requestId = UUID.randomUUID().toString();
            WireMessage response = send(nodeId, request);
            System.out.printf("%s heal status=%s%n", nodeId, response == null ? "DOWN" : response.status);
        }
    }

    private void setDelay(String nodeId, long delayMs) {
        requireKnownNode(nodeId);
        WireMessage request = WireMessage.request("SET_DELAY");
        request.requestId = UUID.randomUUID().toString();
        request.delayMs = delayMs;
        WireMessage response = send(nodeId, request);
        if (isOk(response)) {
            System.out.printf("OK %s delay=%dms%n", nodeId, response.delayMs);
        } else {
            printError(response);
        }
    }

    private void setElectionTimeout(int minMs, int maxMs) {
        for (String nodeId : endpoints.keySet()) {
            WireMessage request = WireMessage.request("SET_ELECTION_TIMEOUT");
            request.requestId = UUID.randomUUID().toString();
            request.electionMinMs = minMs;
            request.electionMaxMs = maxMs;
            WireMessage response = send(nodeId, request);
            System.out.printf("%s electionTimeout=%d-%d status=%s%n",
                    nodeId, minMs, maxMs, response == null ? "DOWN" : response.status);
        }
    }

    private void bench(List<String> args) throws Exception {
        Map<String, String> options = parseOptionList(args);
        int totalOps = Integer.parseInt(options.getOrDefault("--puts", "1000"));
        int threads = Integer.parseInt(options.getOrDefault("--threads", "16"));
        double readRatio = Double.parseDouble(options.getOrDefault("--read-ratio", "0"));
        ExecutorService executor = Executors.newFixedThreadPool(threads);
        List<Future<Long>> futures = new ArrayList<>();
        long started = System.nanoTime();
        for (int i = 0; i < totalOps; i++) {
            int opId = i;
            futures.add(executor.submit(new BenchOp(opId, readRatio)));
        }
        List<Long> latencies = new ArrayList<>();
        for (Future<Long> future : futures) {
            latencies.add(future.get());
        }
        long elapsedNanos = System.nanoTime() - started;
        executor.shutdownNow();
        BenchmarkStats stats = BenchmarkStats.from(latencies, elapsedNanos, totalOps);
        System.out.printf(Locale.US,
                "throughput=%d ops/s avg=%.2fms p50=%.2fms p95=%.2fms p99=%.2fms%n",
                stats.throughputOpsSec, stats.avgMs, stats.p50Ms, stats.p95Ms, stats.p99Ms);
        appendBenchmarkCsv("cli", endpoints.size(), threads, totalOps, stats);
    }

    private void appendBenchmarkCsv(String scenario, int liveNodes, int threads, int totalOps, BenchmarkStats stats) throws IOException {
        Path file = Path.of("benchmarks", "results.csv");
        Files.createDirectories(file.getParent());
        boolean newFile = !Files.exists(file) || Files.size(file) == 0;
        List<String> lines = new ArrayList<>();
        if (newFile) {
            lines.add("part,scenario,liveNodes,electionTimeoutMs,fsync,threads,totalOps,throughputOpsSec,avgMs,p50Ms,p95Ms,p99Ms,failoverMs");
        }
        lines.add(String.format(Locale.US,
                "manual,%s,%d,150-300,%s,%d,%d,%d,%.2f,%.2f,%.2f,%.2f,",
                scenario, liveNodes, fsync, threads, totalOps, stats.throughputOpsSec,
                stats.avgMs, stats.p50Ms, stats.p95Ms, stats.p99Ms));
        Files.write(file, lines, Files.exists(file) ? java.nio.file.StandardOpenOption.APPEND : java.nio.file.StandardOpenOption.CREATE);
    }

    private WireMessage send(String nodeId, WireMessage request) {
        NodeEndpoint endpoint = endpoints.get(nodeId);
        if (endpoint == null) {
            return null;
        }
        try {
            return client.request(endpoint, request);
        } catch (IOException e) {
            return null;
        }
    }

    private Set<String> parseNodeSet(String raw) {
        Set<String> nodes = new HashSet<>();
        for (String nodeId : raw.split(",")) {
            String trimmed = nodeId.trim();
            requireKnownNode(trimmed);
            nodes.add(trimmed);
        }
        return nodes;
    }

    private void requireKnownNode(String nodeId) {
        if (!endpoints.containsKey(nodeId)) {
            throw new IllegalArgumentException("Unknown node " + nodeId);
        }
    }

    private static boolean isOk(WireMessage response) {
        return response != null && response.status == ResponseStatus.OK;
    }

    private static void printError(WireMessage response) {
        if (response == null) {
            System.out.println("ERROR: node unavailable");
        } else {
            System.out.printf("ERROR: %s %s leaderHint=%s%n", response.error, response.message, response.leaderHint);
        }
    }

    private static String requireArg(List<String> args, int index, String name) {
        if (index >= args.size()) {
            throw new IllegalArgumentException("Missing " + name);
        }
        return args.get(index);
    }

    private static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static Map<String, String> extractGlobalOptions(String[] args) {
        Map<String, String> options = new HashMap<>();
        for (int i = 0; i < args.length - 1; i++) {
            if (List.of("--peers", "--data-dir", "--fsync").contains(args[i])) {
                options.put(args[i], args[i + 1]);
                i++;
            }
        }
        return options;
    }

    private static List<String> stripGlobalOptions(String[] args) {
        List<String> result = new ArrayList<>();
        for (int i = 0; i < args.length; i++) {
            if (List.of("--peers", "--data-dir", "--fsync").contains(args[i])) {
                i++;
            } else {
                result.add(args[i]);
            }
        }
        return result;
    }

    private static Map<String, String> parseOptionList(List<String> args) {
        Map<String, String> options = new HashMap<>();
        for (int i = 0; i < args.size(); i += 2) {
            if (i + 1 >= args.size()) {
                throw new IllegalArgumentException("Missing value for " + args.get(i));
            }
            options.put(args.get(i), args.get(i + 1));
        }
        return options;
    }

    private static List<String> splitCommand(String line) {
        if (line.isBlank()) {
            return List.of();
        }
        return Arrays.asList(line.trim().split("\\s+"));
    }

    private final class BenchOp implements Callable<Long> {
        private final int opId;
        private final double readRatio;

        private BenchOp(int opId, double readRatio) {
            this.opId = opId;
            this.readRatio = readRatio;
        }

        @Override
        public Long call() {
            long started = System.nanoTime();
            if (ThreadLocalRandom.current().nextDouble() < readRatio) {
                WireMessage request = WireMessage.request("CLIENT_GET");
                request.requestId = UUID.randomUUID().toString();
                request.key = "k" + ThreadLocalRandom.current().nextInt(Math.max(1, opId + 1));
                sendToLeader(request, Duration.ofSeconds(8));
            } else {
                WireMessage request = WireMessage.request("CLIENT_PUT");
                request.requestId = UUID.randomUUID().toString();
                request.key = "k" + opId;
                request.value = "v" + opId;
                request.command = "put " + request.key + " " + request.value;
                sendToLeader(request, Duration.ofSeconds(12));
            }
            return TimeUnit.NANOSECONDS.toMicros(System.nanoTime() - started);
        }
    }

    private static final class BenchmarkStats {
        private final long throughputOpsSec;
        private final double avgMs;
        private final double p50Ms;
        private final double p95Ms;
        private final double p99Ms;

        private BenchmarkStats(long throughputOpsSec, double avgMs, double p50Ms, double p95Ms, double p99Ms) {
            this.throughputOpsSec = throughputOpsSec;
            this.avgMs = avgMs;
            this.p50Ms = p50Ms;
            this.p95Ms = p95Ms;
            this.p99Ms = p99Ms;
        }

        private static BenchmarkStats from(List<Long> micros, long elapsedNanos, int totalOps) {
            List<Long> sorted = new ArrayList<>(micros);
            Collections.sort(sorted);
            double avg = sorted.stream().mapToLong(Long::longValue).average().orElse(0) / 1000.0;
            long throughput = elapsedNanos == 0 ? 0 : Math.round(totalOps / (elapsedNanos / 1_000_000_000.0));
            return new BenchmarkStats(
                    throughput,
                    avg,
                    percentile(sorted, 0.50) / 1000.0,
                    percentile(sorted, 0.95) / 1000.0,
                    percentile(sorted, 0.99) / 1000.0);
        }

        private static long percentile(List<Long> sorted, double p) {
            if (sorted.isEmpty()) {
                return 0;
            }
            int index = (int) Math.ceil(p * sorted.size()) - 1;
            return sorted.get(Math.max(0, Math.min(index, sorted.size() - 1)));
        }
    }
}
