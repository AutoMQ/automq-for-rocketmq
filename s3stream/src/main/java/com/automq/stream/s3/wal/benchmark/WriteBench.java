package com.automq.stream.s3.wal.benchmark;

import com.automq.stream.s3.wal.BlockWALService;
import com.automq.stream.s3.wal.WriteAheadLog;
import com.automq.stream.utils.ThreadUtils;
import com.automq.stream.utils.Threads;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.helper.HelpScreenException;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * WriteBench is a tool for benchmarking write performance of {@link BlockWALService}
 */
public class WriteBench implements AutoCloseable {
    private static final Logger LOGGER = LoggerFactory.getLogger(WriteBench.class);

    private final WriteAheadLog log;

    public WriteBench(Config config) {
        BlockWALService.BlockWALServiceBuilder builder = BlockWALService.builder(config.path, config.capacity);
        if (config.depth != null) {
            builder.ioThreadNums(config.depth);
        }
        this.log = builder.build();
    }

    public static void main(String[] args) {
        Namespace ns = null;
        ArgumentParser parser = Config.parser();
        try {
            ns = parser.parseArgs(args);
        } catch (HelpScreenException e) {
            System.exit(0);
        } catch (ArgumentParserException e) {
            parser.handleError(e);
            System.exit(1);
        }
        Config config = new Config(ns);

        try (WriteBench bench = new WriteBench(config)) {
            bench.run(config);
        }
    }

    private void run(Config config) {
        LOGGER.info("Starting benchmark");

        ExecutorService executor = Threads.newFixedThreadPool(
                config.threads, ThreadUtils.createThreadFactory("append-thread-%d", false), LOGGER);
        AppendTaskConfig appendTaskConfig = new AppendTaskConfig(config);
        for (int i = 0; i < config.threads; i++) {
            int index = i;
            executor.submit(() -> {
                try {
                    runAppendTask(index, appendTaskConfig);
                } catch (Exception e) {
                    LOGGER.error("Append task failed", e);
                }
            });
        }

        executor.shutdown();
        try {
            if (!executor.awaitTermination(config.durationSeconds, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
        }

        LOGGER.info("Benchmark finished");
    }

    private void runAppendTask(int index, AppendTaskConfig config) throws Exception {
        // TODO
    }

    static class Config {
        // following fields are WAL configuration
        final String path;
        final Long capacity;
        final Integer depth;

        // following fields are benchmark configuration
        final Integer threads;
        final Integer throughputBytes;
        final Integer recordSizeBytes;
        final Long durationSeconds;

        Config(Namespace ns) {
            this.path = ns.getString("path");
            this.capacity = ns.getLong("capacity");
            this.depth = ns.getInt("depth");
            this.threads = ns.getInt("threads");
            this.throughputBytes = ns.getInt("throughput");
            this.recordSizeBytes = ns.getInt("recordSize");
            this.durationSeconds = ns.getLong("duration");
        }

        static ArgumentParser parser() {
            ArgumentParser parser = ArgumentParsers
                    .newFor("WriteBench")
                    .build()
                    .defaultHelp(true)
                    .description("Benchmark write performance of BlockWALService");
            parser.addArgument("-p", "--path")
                    .required(true)
                    .help("Path of the WAL file");
            parser.addArgument("—c", "--capacity")
                    .required(true)
                    .type(Long.class)
                    .help("Capacity of the WAL in bytes");
            parser.addArgument("—d", "--depth")
                    .type(Integer.class)
                    .help("IO depth of the WAL");
            parser.addArgument("--threads")
                    .type(Integer.class)
                    .setDefault(1)
                    .help("Number of threads to use to write");
            parser.addArgument("--throughput")
                    .type(Integer.class)
                    .setDefault(1 << 20)
                    .help("Expected throughput in total in bytes per second");
            parser.addArgument("--record-size")
                    .type(Integer.class)
                    .setDefault(1 << 10)
                    .help("Size of each record in bytes");
            parser.addArgument("--duration")
                    .type(Long.class)
                    .setDefault(60L)
                    .help("Duration of the benchmark in seconds");
            return parser;
        }
    }

    static class AppendTaskConfig {
        final int throughputBytes;
        final int recordSizeBytes;
        final long durationSeconds;

        AppendTaskConfig(Config config) {
            this.throughputBytes = config.throughputBytes / config.threads;
            this.recordSizeBytes = config.recordSizeBytes;
            this.durationSeconds = config.durationSeconds;
        }
    }

    @Override
    public void close() {
        log.shutdownGracefully();
    }
}
