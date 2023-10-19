package com.automq.stream.s3.wal.benchmark;

import com.automq.stream.s3.wal.BlockWALService;
import com.automq.stream.s3.wal.WriteAheadLog;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.helper.HelpScreenException;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * WriteBench is a tool for benchmarking write performance of {@link BlockWALService}
 */
public class WriteBench implements AutoCloseable {
    private static final Logger LOGGER = LoggerFactory.getLogger(WriteBench.class);

    private final WriteAheadLog log;

    static class Config {
        final String path;
        final Long capacity;

        Config(Namespace ns) {
            this.path = ns.getString("path");
            this.capacity = ns.getLong("capacity");
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
            return parser;
        }
    }

    public WriteBench(Config config) {
        BlockWALService.BlockWALServiceBuilder builder = BlockWALService.builder(config.path, config.capacity);
        this.log = builder.build();
    }

    public static void main(String[] args) throws Exception {
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
            bench.run();
        }
    }

    private void run() throws Exception {
        LOGGER.info("Starting benchmark");
        // TODO
    }

    @Override
    public void close() {
        log.shutdownGracefully();
    }
}
