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

package com.automq.rocketmq.broker;

import com.automq.rocketmq.common.config.BrokerConfig;
import com.google.common.base.Stopwatch;
import com.google.common.base.Strings;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.logging.log4j.LogManager;
import org.apache.rocketmq.logging.ch.qos.logback.classic.ClassicConstants;
import org.apache.rocketmq.srvutil.ServerUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.introspector.BeanAccess;

public class BrokerStartup {
    private static final Logger LOGGER = LoggerFactory.getLogger(BrokerStartup.class);

    public static void main(String[] args) throws Exception {
        LOGGER.info("Starting broker...");
        long start = System.currentTimeMillis();

        configKernelLogger();

        Options options = buildCommandlineOptions();

        CommandLine commandLine = ServerUtil.parseCmdLine(
            "mqbroker", args, options, new DefaultParser());
        if (null == commandLine) {
            LOGGER.error("Failed to parse command line, please use `-h` to check.");
            System.exit(-1);
        }

        BrokerConfig brokerConfig = null;

        if (commandLine.hasOption('c')) {
            String configFile = commandLine.getOptionValue('c');
            if (configFile != null) {
                try {
                    String configStr = Files.readString(Path.of(configFile), StandardCharsets.UTF_8);
                    brokerConfig = loadBrokerConfig(configStr);
                } catch (IOException e) {
                    LOGGER.error("Failed to read config file {}", configFile, e);
                    return;
                }
            }
        }

        if (null == brokerConfig) {
            LOGGER.error("Default broker config file is not found");
            return;
        }

        // Fill overrides from environment variables
        brokerConfig.initEnvVar();

        brokerConfig.validate();

        BrokerController controller = buildBrokerController(brokerConfig);

        // Register shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(buildShutdownHook(controller), "ShutdownHook"));

        // Start the broker
        start(controller);

        LOGGER.info("Broker started, costs {} ms", System.currentTimeMillis() - start);
    }

    private static void start(BrokerController controller) throws Exception {
        try {
            controller.start();
        } catch (Exception e) {
            LOGGER.error("Failed to start broker, try to shutdown", e);
            controller.shutdown();
            System.exit(-1);
        }
    }

    private static BrokerController buildBrokerController(BrokerConfig config) throws Exception {
        return new BrokerController(config);
    }

    /**
     * Retrieve the configuration from the command line.
     */
    private static BrokerConfig loadBrokerConfig(String configStr) {
        Yaml yaml = new Yaml();
        yaml.setBeanAccess(BeanAccess.FIELD);
        return yaml.loadAs(configStr, BrokerConfig.class);
    }

    private static void configKernelLogger() {
        String logConfig = System.getProperty(ClassicConstants.CONFIG_FILE_PROPERTY);

        String defaultLogConfigFile = "rmq.proxy.logback.xml";
        if (Strings.isNullOrEmpty(logConfig)) {
            LOGGER.info("Load the default logback config for the kernel of rocketmq from classpath: {}", defaultLogConfigFile);
            System.setProperty(ClassicConstants.CONFIG_FILE_PROPERTY, defaultLogConfigFile);
        } else {
            LOGGER.info("Load the logback config for the kernel of rocketmq from the specific file: {}", logConfig);
        }
    }

    public static Runnable buildShutdownHook(BrokerController brokerController) {
        return new Runnable() {
            private volatile boolean hasShutdown = false;
            private final AtomicInteger shutdownTimes = new AtomicInteger(0);

            @Override
            public void run() {
                synchronized (this) {
                    LOGGER.info("Shutdown hook was invoked, {}", this.shutdownTimes.incrementAndGet());
                    if (!this.hasShutdown) {
                        this.hasShutdown = true;
                        Stopwatch stopwatch = Stopwatch.createStarted();
                        try {
                            brokerController.shutdown();
                        } catch (Exception e) {
                            LOGGER.error("Shutdown exception", e);
                        }
                        LOGGER.info("Shutdown hook over, consuming total time(ms): {}", stopwatch.elapsed(TimeUnit.MILLISECONDS));
                        LogManager.shutdown();
                    }
                }
            }
        };
    }

    private static Options buildCommandlineOptions() {
        Options options = new Options();
        Option opt = new Option("h", "help", false, "Print help");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("c", "configFile", true, "Broker config file in YAML format");
        opt.setRequired(false);
        options.addOption(opt);

        return options;
    }
}
