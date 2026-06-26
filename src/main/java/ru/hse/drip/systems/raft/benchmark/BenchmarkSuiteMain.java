package ru.hse.drip.systems.raft.benchmark;

import ru.hse.drip.systems.raft.node.NodeConfig;
import ru.hse.drip.systems.raft.node.RaftNodeMain;
import ru.hse.drip.systems.raft.protocol.ErrorCode;
import ru.hse.drip.systems.raft.protocol.NodeEndpoint;
import ru.hse.drip.systems.raft.protocol.ResponseStatus;
import ru.hse.drip.systems.raft.protocol.Role;
import ru.hse.drip.systems.raft.protocol.WireMessage;
import ru.hse.drip.systems.raft.transport.TcpClient;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

public final class BenchmarkSuiteMain {
    private static final String PEERS = NodeConfig.defaultPeers();
    private static final Path BENCH_DIR = Path.of("benchmarks");
    private static final Path RUNTIME_DIR = Path.of("bench-runtime");
    private static final Path LOGS_DIR = Path.of("bench-logs");
    private static final int DEFAULT_TOTAL_OPS = 10_000;
    private static final int DEFAULT_THREADS = 100;

    private final Map<String, NodeEndpoint> endpoints = new LinkedHashMap<>(NodeConfig.parsePeers(PEERS));
    private final TcpClient client = new TcpClient(Duration.ofMillis(500), Duration.ofSeconds(30));
    private final int totalOps;
    private final int threads;
    private final List<ResultRow> results = new ArrayList<>();

    private BenchmarkSuiteMain(int totalOps, int threads) {
        this.totalOps = totalOps;
        this.threads = threads;
    }

    public static void main(String[] args) throws Exception {
        Map<String, String> options = parseArgs(args);
        int totalOps = Integer.parseInt(options.getOrDefault("--total-ops", Integer.toString(DEFAULT_TOTAL_OPS)));
        int threads = Integer.parseInt(options.getOrDefault("--threads", Integer.toString(DEFAULT_THREADS)));
        BenchmarkSuiteMain suite = new BenchmarkSuiteMain(totalOps, threads);
        suite.run();
    }

    private void run() throws Exception {
        Files.createDirectories(BENCH_DIR);
        Files.createDirectories(LOGS_DIR);
        stopAnyRunningNodes();

        runThroughputScenario("A", "all-alive", 5, "on", List.of());
        runThroughputScenario("A", "one-dead", 4, "on", List.of("one-follower"));
        runThroughputScenario("A", "two-dead", 3, "on", List.of("two-followers"));

        runFailoverScenario("failover-fast", 150, 300);
        runFailoverScenario("failover-slow", 500, 1000);

        runThroughputScenario("C", "fsync-on", 5, "on", List.of());
        runThroughputScenario("C", "fsync-off", 5, "off", List.of());

        writeCsv();
        writeGraphs();
        writeReport();
        stopAnyRunningNodes();
        System.out.println("Benchmark suite completed. Results: " + BENCH_DIR.resolve("results.csv"));
    }

    private void runThroughputScenario(String part, String scenario, int liveNodes, String fsync, List<String> killMode) throws Exception {
        System.out.printf("== %s/%s fsync=%s liveNodes=%d ==%n", part, scenario, fsync, liveNodes);
        try (Cluster cluster = startFreshCluster(scenario, fsync)) {
            String leader = waitForLeader(Duration.ofSeconds(10));
            if (killMode.contains("one-follower")) {
                killFollowers(cluster, leader, 1);
            } else if (killMode.contains("two-followers")) {
                killFollowers(cluster, leader, 2);
            }
            waitForStableLeader(Duration.ofSeconds(3));
            BenchmarkStats stats = runPutLoad(scenario, totalOps, threads);
            ResultRow row = ResultRow.throughput(
                    part,
                    scenario,
                    liveNodes,
                    "150-300",
                    fsync,
                    threads,
                    totalOps,
                    stats);
            results.add(row);
            System.out.printf(Locale.US, "%s throughput=%d ops/s avg=%.2f p95=%.2f p99=%.2f%n",
                    scenario, stats.throughputOpsSec, stats.avgMs, stats.p95Ms, stats.p99Ms);
        }
    }

    private void runFailoverScenario(String scenario, int minMs, int maxMs) throws Exception {
        System.out.printf("== B/%s election=%d-%d ==%n", scenario, minMs, maxMs);
        try (Cluster ignored = startFreshCluster(scenario, "on")) {
            setElectionTimeout(minMs, maxMs);
            String oldLeader = waitForLeader(Duration.ofSeconds(10));
            long started = System.nanoTime();
            killNode(oldLeader);
            long failoverMs = waitForFirstSuccessfulPut(scenario, Duration.ofSeconds(15), started);
            ResultRow row = ResultRow.failover(
                    "B",
                    scenario,
                    4,
                    minMs + "-" + maxMs,
                    "on",
                    failoverMs);
            results.add(row);
            System.out.printf("%s failover=%d ms%n", scenario, failoverMs);
        }
    }

    private Cluster startFreshCluster(String scenario, String fsync) throws Exception {
        stopAnyRunningNodes();
        Path dataDir = RUNTIME_DIR.resolve(scenario + "-" + fsync);
        deleteRecursively(dataDir);
        Files.createDirectories(dataDir);
        Map<String, Process> processes = new LinkedHashMap<>();
        for (NodeEndpoint endpoint : endpoints.values()) {
            processes.put(endpoint.nodeId, startNode(endpoint, dataDir, fsync));
        }
        Cluster cluster = new Cluster(processes);
        waitForLeader(Duration.ofSeconds(10));
        return cluster;
    }

    private Process startNode(NodeEndpoint endpoint, Path dataDir, String fsync) throws IOException {
        String javaBin = Path.of(System.getProperty("java.home"), "bin", "java").toString();
        Path out = LOGS_DIR.resolve(endpoint.nodeId + ".out");
        Path err = LOGS_DIR.resolve(endpoint.nodeId + ".err");
        List<String> command = List.of(
                javaBin,
                "-cp",
                System.getProperty("java.class.path"),
                RaftNodeMain.class.getName(),
                "--id", endpoint.nodeId,
                "--port", Integer.toString(endpoint.port),
                "--data-dir", dataDir.toString(),
                "--peers", PEERS,
                "--fsync", fsync);
        return new ProcessBuilder(command)
                .redirectOutput(ProcessBuilder.Redirect.appendTo(out.toFile()))
                .redirectError(ProcessBuilder.Redirect.appendTo(err.toFile()))
                .start();
    }

    private void stopAnyRunningNodes() {
        for (String nodeId : endpoints.keySet()) {
            WireMessage request = WireMessage.request("KILL");
            request.requestId = UUID.randomUUID().toString();
            send(nodeId, request);
        }
        sleep(350);
    }

    private void killFollowers(Cluster cluster, String leader, int count) {
        int killed = 0;
        for (String nodeId : endpoints.keySet()) {
            if (nodeId.equals(leader)) {
                continue;
            }
            killNode(nodeId);
            Process process = cluster.processes.get(nodeId);
            if (process != null) {
                process.destroy();
            }
            killed++;
            if (killed == count) {
                return;
            }
        }
    }

    private void killNode(String nodeId) {
        WireMessage request = WireMessage.request("KILL");
        request.requestId = UUID.randomUUID().toString();
        send(nodeId, request);
        sleep(250);
    }

    private BenchmarkStats runPutLoad(String scenario, int operations, int threadCount) throws InterruptedException, ExecutionException {
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        List<Future<Long>> futures = new ArrayList<>(operations);
        String leader = waitForLeader(Duration.ofSeconds(10));
        long started = System.nanoTime();
        for (int i = 0; i < operations; i++) {
            int opId = i;
            futures.add(executor.submit(new PutOperation(scenario, opId, leader)));
        }
        List<Long> latenciesMicros = new ArrayList<>(operations);
        for (Future<Long> future : futures) {
            latenciesMicros.add(future.get());
        }
        long elapsedNanos = System.nanoTime() - started;
        executor.shutdownNow();
        return BenchmarkStats.from(latenciesMicros, elapsedNanos, operations);
    }

    private long waitForFirstSuccessfulPut(String scenario, Duration timeout, long startedNanos) {
        long deadline = System.nanoTime() + timeout.toNanos();
        int attempt = 0;
        while (System.nanoTime() < deadline) {
            WireMessage request = WireMessage.request("CLIENT_PUT");
            request.requestId = UUID.randomUUID().toString();
            request.key = scenario + "-failover";
            request.value = "attempt-" + attempt;
            request.command = "put " + request.key + " " + request.value;
            WireMessage response = sendToLeader(request, Duration.ofSeconds(2), null);
            if (isOk(response)) {
                return TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startedNanos);
            }
            attempt++;
            sleep(25);
        }
        throw new IllegalStateException("Failover did not complete in " + timeout.toMillis() + " ms");
    }

    private final class PutOperation implements Callable<Long> {
        private final String scenario;
        private final int opId;
        private final String initialLeader;

        private PutOperation(String scenario, int opId, String initialLeader) {
            this.scenario = scenario;
            this.opId = opId;
            this.initialLeader = initialLeader;
        }

        @Override
        public Long call() {
            WireMessage request = WireMessage.request("CLIENT_PUT");
            request.requestId = UUID.randomUUID().toString();
            request.key = scenario + "-k" + opId + "-" + ThreadLocalRandom.current().nextInt(1_000_000);
            request.value = "v" + opId;
            request.command = "put " + request.key + " " + request.value;
            long started = System.nanoTime();
            WireMessage response = sendToLeader(request, Duration.ofSeconds(30), initialLeader);
            if (!isOk(response)) {
                throw new IllegalStateException("put failed: " + describe(response));
            }
            return TimeUnit.NANOSECONDS.toMicros(System.nanoTime() - started);
        }
    }

    private WireMessage sendToLeader(WireMessage request, Duration timeout, String initialLeader) {
        long deadline = System.nanoTime() + timeout.toNanos();
        String leader = initialLeader;
        while (System.nanoTime() < deadline) {
            if (leader == null) {
                leader = tryFindLeader();
            }
            if (leader != null) {
                WireMessage response = send(leader, request);
                if (isOk(response)) {
                    return response;
                }
                if (response != null && response.leaderHint != null && endpoints.containsKey(response.leaderHint)) {
                    leader = response.leaderHint;
                } else {
                    leader = null;
                }
            }
            sleep(20);
        }
        return WireMessage.error(request.requestId, ErrorCode.TIMEOUT, "leader request timeout");
    }

    private String waitForStableLeader(Duration duration) {
        String leader = waitForLeader(duration);
        sleep(250);
        return leader;
    }

    private String waitForLeader(Duration timeout) {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadline) {
            String leader = tryFindLeader();
            if (leader != null) {
                return leader;
            }
            sleep(50);
        }
        throw new IllegalStateException("Leader was not elected in " + timeout.toMillis() + " ms");
    }

    private String tryFindLeader() {
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
            if (isOk(response) && response.role == Role.LEADER && response.leaderHint != null) {
                return response.leaderHint;
            }
            if (response.leaderHint != null && term >= hintedTerm) {
                hinted = response.leaderHint;
                hintedTerm = term;
            }
        }
        return hinted;
    }

    private void setElectionTimeout(int minMs, int maxMs) {
        for (String nodeId : endpoints.keySet()) {
            WireMessage request = WireMessage.request("SET_ELECTION_TIMEOUT");
            request.requestId = UUID.randomUUID().toString();
            request.electionMinMs = minMs;
            request.electionMaxMs = maxMs;
            send(nodeId, request);
        }
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

    private void writeCsv() throws IOException {
        Path file = BENCH_DIR.resolve("results.csv");
        List<String> lines = new ArrayList<>();
        lines.add("part,scenario,liveNodes,electionTimeoutMs,fsync,threads,totalOps,throughputOpsSec,avgMs,p50Ms,p95Ms,p99Ms,failoverMs");
        for (ResultRow result : results) {
            lines.add(result.toCsv());
        }
        Files.write(file, lines);
    }

    private void writeGraphs() throws IOException {
        writeBarChart(
                BENCH_DIR.resolve("throughput-vs-live-nodes.svg"),
                "Throughput vs live nodes",
                results.stream().filter(row -> "A".equals(row.part)).toList(),
                row -> row.liveNodes + " nodes",
                row -> row.throughputOpsSec,
                "ops/s");
        writeBarChart(
                BENCH_DIR.resolve("failover-vs-election-timeout.svg"),
                "Failover vs election timeout",
                results.stream().filter(row -> "B".equals(row.part)).toList(),
                row -> row.electionTimeoutMs,
                row -> row.failoverMs,
                "ms");
        writeBarChart(
                BENCH_DIR.resolve("fsync-latency.svg"),
                "p95 put latency: fsync on/off",
                results.stream().filter(row -> "C".equals(row.part)).toList(),
                row -> row.fsync,
                row -> Math.round(row.p95Ms),
                "ms");
    }

    private void writeBarChart(
            Path path,
            String title,
            List<ResultRow> rows,
            LabelExtractor labelExtractor,
            ValueExtractor valueExtractor,
            String unit) throws IOException {
        long max = rows.stream().map(valueExtractor::value).filter(Objects::nonNull).max(Long::compareTo).orElse(1L);
        int width = 720;
        int height = 260;
        int left = 170;
        int barHeight = 34;
        int gap = 22;
        StringBuilder svg = new StringBuilder();
        svg.append("<svg xmlns=\"http://www.w3.org/2000/svg\" width=\"").append(width).append("\" height=\"").append(height).append("\">\n");
        svg.append("<rect width=\"100%\" height=\"100%\" fill=\"#ffffff\"/>\n");
        svg.append("<text x=\"24\" y=\"34\" font-family=\"Arial\" font-size=\"20\" font-weight=\"700\">").append(escape(title)).append("</text>\n");
        int y = 70;
        for (ResultRow row : rows) {
            Long value = valueExtractor.value(row);
            if (value == null) {
                continue;
            }
            int barWidth = (int) Math.max(2, Math.round((width - left - 90) * (value / (double) max)));
            svg.append("<text x=\"24\" y=\"").append(y + 23).append("\" font-family=\"Arial\" font-size=\"14\">")
                    .append(escape(labelExtractor.label(row))).append("</text>\n");
            svg.append("<rect x=\"").append(left).append("\" y=\"").append(y).append("\" width=\"").append(barWidth)
                    .append("\" height=\"").append(barHeight).append("\" rx=\"4\" fill=\"#3273dc\"/>\n");
            svg.append("<text x=\"").append(left + barWidth + 10).append("\" y=\"").append(y + 23)
                    .append("\" font-family=\"Arial\" font-size=\"14\">").append(value).append(" ").append(unit).append("</text>\n");
            y += barHeight + gap;
        }
        svg.append("</svg>\n");
        Files.writeString(path, svg.toString());
    }

    private void writeReport() throws IOException {
        ResultRow allAlive = find("A", "all-alive");
        ResultRow oneDead = find("A", "one-dead");
        ResultRow twoDead = find("A", "two-dead");
        ResultRow failoverFast = find("B", "failover-fast");
        ResultRow failoverSlow = find("B", "failover-slow");
        ResultRow fsyncOn = find("C", "fsync-on");
        ResultRow fsyncOff = find("C", "fsync-off");
        double fsyncCost = fsyncOff.p95Ms == 0 ? 0 : fsyncOn.p95Ms / fsyncOff.p95Ms;

        StringBuilder report = new StringBuilder();
        report.append("# Raft Benchmark Report\n\n");
        report.append("Generated at: ").append(Instant.now()).append("\n\n");
        report.append("Workload for parts A/C: 100% `put`, `threads=").append(threads)
                .append("`, `totalOps=").append(totalOps).append("`.\n\n");
        report.append("## Results\n\n");
        report.append("| part | scenario | liveNodes | electionTimeoutMs | fsync | threads | totalOps | throughputOpsSec | avgMs | p50Ms | p95Ms | p99Ms | failoverMs |\n");
        report.append("|---|---|---:|---|---|---:|---:|---:|---:|---:|---:|---:|---:|\n");
        for (ResultRow row : results) {
            report.append(row.toMarkdown()).append("\n");
        }
        report.append("\n## Graphs\n\n");
        report.append("![Throughput vs live nodes](benchmarks/throughput-vs-live-nodes.svg)\n\n");
        report.append("![Failover vs election timeout](benchmarks/failover-vs-election-timeout.svg)\n\n");
        report.append("![p95 put latency: fsync on/off](benchmarks/fsync-latency.svg)\n\n");
        report.append("## Analysis\n\n");
        report.append("### A. Throughput vs live nodes\n\n");
        report.append(String.format(Locale.US,
                "Throughput changed from `%d ops/s` with 5 live nodes to `%d ops/s` with 4 and `%d ops/s` with 3. ",
                allAlive.throughputOpsSec, oneDead.throughputOpsSec, twoDead.throughputOpsSec));
        report.append("The cluster keeps committing while a majority is alive: for 5 nodes the quorum is 3. ");
        report.append("With two killed nodes there are exactly 3 live replicas, so this is the last safe operating point; losing one more node would make majority commit impossible.\n\n");
        report.append("### B. Election timeout vs failover\n\n");
        report.append(String.format(Locale.US,
                "Failover with `150-300 ms` election timeout took `%d ms`; with `500-1000 ms` it took `%d ms`. ",
                failoverFast.failoverMs, failoverSlow.failoverMs));
        report.append("The shorter timeout detects leader loss faster, but in a noisy network it is also more likely to start unnecessary elections and disrupt a healthy leader.\n\n");
        report.append("### C. fsync cost\n\n");
        report.append(String.format(Locale.US,
                "`fsync on` p95 latency was `%.2f ms`; `fsync off` p95 latency was `%.2f ms`, about `%.2fx` difference on this run. ",
                fsyncOn.p95Ms, fsyncOff.p95Ms, fsyncCost));
        report.append("The durable mode is slower because every log append and term/vote update waits for storage flush. ");
        report.append("Without `force()` the system is faster, but a power loss can drop a log entry after a node has already acknowledged it, breaking Raft's majority durability assumption.\n");
        Files.writeString(Path.of("report.md"), report.toString());
    }

    private ResultRow find(String part, String scenario) {
        return results.stream()
                .filter(row -> row.part.equals(part) && row.scenario.equals(scenario))
                .findFirst()
                .orElseThrow();
    }

    private static String describe(WireMessage response) {
        if (response == null) {
            return "no response";
        }
        return response.status + " " + response.error + " " + response.message + " leaderHint=" + response.leaderHint;
    }

    private static boolean isOk(WireMessage response) {
        return response != null && response.status == ResponseStatus.OK;
    }

    private static void deleteRecursively(Path root) throws IOException {
        if (!Files.exists(root)) {
            return;
        }
        try (var stream = Files.walk(root)) {
            List<Path> paths = stream.sorted(Comparator.reverseOrder()).toList();
            for (Path path : paths) {
                Files.deleteIfExists(path);
            }
        }
    }

    private static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static Map<String, String> parseArgs(String[] args) {
        Map<String, String> options = new HashMap<>();
        for (int i = 0; i < args.length; i += 2) {
            if (i + 1 >= args.length) {
                throw new IllegalArgumentException("Missing value for " + args[i]);
            }
            options.put(args[i], args[i + 1]);
        }
        return options;
    }

    private static String escape(String value) {
        return value.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;");
    }

    private final class Cluster implements AutoCloseable {
        private final Map<String, Process> processes;

        private Cluster(Map<String, Process> processes) {
            this.processes = processes;
        }

        @Override
        public void close() {
            for (String nodeId : endpoints.keySet()) {
                killNode(nodeId);
            }
            for (Process process : processes.values()) {
                if (process.isAlive()) {
                    process.destroyForcibly();
                }
            }
            sleep(250);
        }
    }

    @FunctionalInterface
    private interface LabelExtractor {
        String label(ResultRow row);
    }

    @FunctionalInterface
    private interface ValueExtractor {
        Long value(ResultRow row);
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
            sorted.sort(Long::compareTo);
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

    private static final class ResultRow {
        private final String part;
        private final String scenario;
        private final int liveNodes;
        private final String electionTimeoutMs;
        private final String fsync;
        private final int threads;
        private final int totalOps;
        private final Long throughputOpsSec;
        private final double avgMs;
        private final double p50Ms;
        private final double p95Ms;
        private final double p99Ms;
        private final Long failoverMs;

        private ResultRow(
                String part,
                String scenario,
                int liveNodes,
                String electionTimeoutMs,
                String fsync,
                int threads,
                int totalOps,
                Long throughputOpsSec,
                double avgMs,
                double p50Ms,
                double p95Ms,
                double p99Ms,
                Long failoverMs) {
            this.part = part;
            this.scenario = scenario;
            this.liveNodes = liveNodes;
            this.electionTimeoutMs = electionTimeoutMs;
            this.fsync = fsync;
            this.threads = threads;
            this.totalOps = totalOps;
            this.throughputOpsSec = throughputOpsSec;
            this.avgMs = avgMs;
            this.p50Ms = p50Ms;
            this.p95Ms = p95Ms;
            this.p99Ms = p99Ms;
            this.failoverMs = failoverMs;
        }

        private static ResultRow throughput(
                String part,
                String scenario,
                int liveNodes,
                String electionTimeoutMs,
                String fsync,
                int threads,
                int totalOps,
                BenchmarkStats stats) {
            return new ResultRow(
                    part,
                    scenario,
                    liveNodes,
                    electionTimeoutMs,
                    fsync,
                    threads,
                    totalOps,
                    stats.throughputOpsSec,
                    stats.avgMs,
                    stats.p50Ms,
                    stats.p95Ms,
                    stats.p99Ms,
                    null);
        }

        private static ResultRow failover(
                String part,
                String scenario,
                int liveNodes,
                String electionTimeoutMs,
                String fsync,
                long failoverMs) {
            return new ResultRow(part, scenario, liveNodes, electionTimeoutMs, fsync, 1, 1, null, 0, 0, 0, 0, failoverMs);
        }

        private String toCsv() {
            return String.join(",",
                    part,
                    scenario,
                    Integer.toString(liveNodes),
                    electionTimeoutMs,
                    fsync,
                    Integer.toString(threads),
                    Integer.toString(totalOps),
                    throughputOpsSec == null ? "" : Long.toString(throughputOpsSec),
                    avgMs == 0 ? "" : format(avgMs),
                    p50Ms == 0 ? "" : format(p50Ms),
                    p95Ms == 0 ? "" : format(p95Ms),
                    p99Ms == 0 ? "" : format(p99Ms),
                    failoverMs == null ? "" : Long.toString(failoverMs));
        }

        private String toMarkdown() {
            return String.format(Locale.US,
                    "| %s | %s | %d | %s | %s | %d | %d | %s | %s | %s | %s | %s | %s |",
                    part,
                    scenario,
                    liveNodes,
                    electionTimeoutMs,
                    fsync,
                    threads,
                    totalOps,
                    throughputOpsSec == null ? "" : Long.toString(throughputOpsSec),
                    avgMs == 0 ? "" : format(avgMs),
                    p50Ms == 0 ? "" : format(p50Ms),
                    p95Ms == 0 ? "" : format(p95Ms),
                    p99Ms == 0 ? "" : format(p99Ms),
                    failoverMs == null ? "" : Long.toString(failoverMs));
        }

        private static String format(double value) {
            return String.format(Locale.US, "%.2f", value);
        }
    }
}
