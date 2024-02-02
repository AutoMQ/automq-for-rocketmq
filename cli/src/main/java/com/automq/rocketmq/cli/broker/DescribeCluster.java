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

package com.automq.rocketmq.cli.broker;

import apache.rocketmq.controller.v1.Cluster;
import apache.rocketmq.controller.v1.DescribeClusterRequest;
import com.automq.rocketmq.cli.CliClientConfig;
import com.automq.rocketmq.cli.ConsoleHelper;
import com.automq.rocketmq.cli.MQAdmin;
import com.automq.rocketmq.controller.ControllerClient;
import com.automq.rocketmq.controller.client.GrpcControllerClient;
import java.util.concurrent.Callable;
import picocli.CommandLine;

@CommandLine.Command(name = "describeCluster", mixinStandardHelpOptions = true, showDefaultValues = true)
public class DescribeCluster implements Callable<Void> {

    @CommandLine.ParentCommand
    MQAdmin mqAdmin;

    @Override
    public Void call() throws Exception {
        try (ControllerClient client = new GrpcControllerClient(new CliClientConfig())) {
            DescribeClusterRequest request = DescribeClusterRequest.newBuilder()
                .build();
            Cluster cluster = client.describeCluster(mqAdmin.getEndpoint(), request).join();
            ConsoleHelper.printCluster(cluster);
        }
        return null;
    }
}
