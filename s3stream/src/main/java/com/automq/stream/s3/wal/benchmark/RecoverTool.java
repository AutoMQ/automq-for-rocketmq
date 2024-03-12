/*
 * Copyright 2024, AutoMQ CO.,LTD.
 *
 * Use of this software is governed by the Business Source License
 * included in the file BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

package com.automq.stream.s3.wal.benchmark;

import com.automq.stream.s3.wal.BlockWALService;
import com.automq.stream.s3.wal.WALHeader;
import java.io.IOException;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.Namespace;

import static com.automq.stream.s3.wal.benchmark.BenchTool.parseArgs;

/**
 * RecoverTool is a tool to recover records in a WAL manually.
 * It extends {@link BlockWALService} to use tools provided by {@link BlockWALService}
 */
public class RecoverTool extends BlockWALService implements AutoCloseable {

    public RecoverTool(Config config) throws IOException {
        super(BlockWALService.recoveryBuilder(config.path));
        super.start();
    }

    public static void main(String[] args) throws IOException {
        Namespace ns = parseArgs(WriteBench.Config.parser(), args);
        Config config = new Config(ns);

        try (RecoverTool tool = new RecoverTool(config)) {
            tool.run();
        }
    }

    private void run() {
        WALHeader header = super.tryReadWALHeader();
        System.out.println(header);
        super.recover().forEachRemaining(System.out::println);
    }

    @Override
    public void close() {
        super.shutdownGracefully();
    }

    public static class Config {
        final String path;

        Config(Namespace ns) {
            this.path = ns.getString("path");
        }

        static ArgumentParser parser() {
            ArgumentParser parser = ArgumentParsers
                .newFor("RecoverTool")
                .build()
                .defaultHelp(true)
                .description("Recover records in a WAL file");
            parser.addArgument("-p", "--path")
                .required(true)
                .help("Path of the WAL file");
            return parser;
        }
    }
}
