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

package com.automq.rocketmq.cli.consumer;

import com.automq.rocketmq.cli.CliClientConfig;
import com.automq.rocketmq.cli.MQAdmin;
import com.automq.rocketmq.controller.ControllerClient;
import com.automq.rocketmq.controller.client.GrpcControllerClient;
import java.util.concurrent.Callable;
import picocli.CommandLine;

@CommandLine.Command(name = "deleteGroup", mixinStandardHelpOptions = true, showDefaultValues = true)
public class DeleteGroup implements Callable<Void> {

    @CommandLine.Option(names = {"-i", "--id"}, description = "Group ID", required = true)
    long id;

    @CommandLine.ParentCommand
    MQAdmin mqAdmin;

    @Override
    public Void call() throws Exception {
        try (ControllerClient client = new GrpcControllerClient(new CliClientConfig())) {
            client.deleteGroup(mqAdmin.getEndpoint(), id)
                .thenRun(() -> {
                    System.out.println("Deleted group whose group-id=" + id);
                })
                .join();
        }
        return null;
    }
}
