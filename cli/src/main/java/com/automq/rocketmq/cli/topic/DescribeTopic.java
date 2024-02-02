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

package com.automq.rocketmq.cli.topic;

import apache.rocketmq.controller.v1.Topic;
import com.automq.rocketmq.cli.CliClientConfig;
import com.automq.rocketmq.cli.ConsoleHelper;
import com.automq.rocketmq.cli.MQAdmin;
import com.automq.rocketmq.controller.ControllerClient;
import com.automq.rocketmq.controller.client.GrpcControllerClient;
import java.util.concurrent.Callable;
import picocli.CommandLine;

@CommandLine.Command(name = "describeTopic", mixinStandardHelpOptions = true, showDefaultValues = true)
public class DescribeTopic implements Callable<Void> {

    @CommandLine.Option(names = {"-t", "--topicName"}, description = "Topic name", required = true)
    String topicName;

    @CommandLine.ParentCommand
    MQAdmin mqAdmin;

    @Override
    public Void call() throws Exception {
        try (ControllerClient client = new GrpcControllerClient(new CliClientConfig())) {
            Topic topic = client.describeTopic(mqAdmin.getEndpoint(), null, topicName)
                .join();
            if (null == topic) {
                System.err.printf("Topic '%s' is not found%n%n", topicName);
                return null;
            }
            ConsoleHelper.printTable(topic);
        }
        return null;
    }
}
