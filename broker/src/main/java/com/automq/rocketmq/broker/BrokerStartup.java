/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.automq.rocketmq.broker;

import com.automq.rocketmq.common.config.BrokerConfig;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.rocketmq.srvutil.ServerUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.introspector.BeanAccess;

public class BrokerStartup {
    private static final Logger LOGGER = LoggerFactory.getLogger(BrokerStartup.class);

    public static void main(String[] args) throws Exception {
        Options options = buildCommandlineOptions();

        CommandLine commandLine = ServerUtil.parseCmdLine(
            "mqbroker", args, options, new DefaultParser());
        if (null == commandLine) {
            LOGGER.error("Failed to parse command line, pelase use `-h` to check.");
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
                    System.exit(-1);
                }
            }
        }

        start(buildBrokerController(brokerConfig));
    }

    private static void start(BrokerController controller) {
        try {
            controller.start();
        } catch (Exception e) {
            LOGGER.error("Failed to start broker controller", e);
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
