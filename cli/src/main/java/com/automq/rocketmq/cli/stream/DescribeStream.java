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

package com.automq.rocketmq.cli.stream;

import apache.rocketmq.controller.v1.DescribeStreamRequest;
import apache.rocketmq.controller.v1.StreamDescription;
import com.automq.rocketmq.cli.CliClientConfig;
import com.automq.rocketmq.cli.ConsoleHelper;
import com.automq.rocketmq.cli.MQAdmin;
import com.automq.rocketmq.controller.ControllerClient;
import com.automq.rocketmq.controller.client.GrpcControllerClient;
import java.util.concurrent.Callable;
import picocli.CommandLine;

@CommandLine.Command(name = "describeStream", mixinStandardHelpOptions = true, showDefaultValues = true)
public class DescribeStream implements Callable<Void> {

    @CommandLine.ParentCommand
    MQAdmin mqAdmin;

    @CommandLine.Option(names = {"-i", "--streamId"}, description = "Stream ID", required = true)
    long streamId;

    @Override
    public Void call() throws Exception {
        try (ControllerClient client = new GrpcControllerClient(new CliClientConfig())) {
            DescribeStreamRequest request = DescribeStreamRequest.newBuilder()
                .setStreamId(streamId)
                .build();
            StreamDescription description = client.describeStream(mqAdmin.getEndpoint(), request).join();
            ConsoleHelper.printStream(description.getStream(), description.getRangesList());
        }
        return null;
    }
}
