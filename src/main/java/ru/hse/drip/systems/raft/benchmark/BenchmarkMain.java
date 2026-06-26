package ru.hse.drip.systems.raft.benchmark;

import ru.hse.drip.systems.raft.cli.CliMain;

public final class BenchmarkMain {
    private BenchmarkMain() {
    }

    public static void main(String[] args) {
        if (args.length == 0) {
            CliMain.main(new String[]{"bench", "--puts", "1000", "--threads", "16"});
        } else {
            String[] forwarded = new String[args.length + 1];
            forwarded[0] = "bench";
            System.arraycopy(args, 0, forwarded, 1, args.length);
            CliMain.main(forwarded);
        }
    }
}
