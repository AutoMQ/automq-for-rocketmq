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

import apache.rocketmq.controller.v1.CreateGroupRequest;
import apache.rocketmq.controller.v1.GroupType;
import apache.rocketmq.controller.v1.SubscriptionMode;
import com.automq.rocketmq.cli.CliClientConfig;
import com.automq.rocketmq.cli.MQAdmin;
import com.automq.rocketmq.controller.ControllerClient;
import com.automq.rocketmq.controller.client.GrpcControllerClient;
import java.util.concurrent.Callable;
import picocli.CommandLine;

@CommandLine.Command(name = "createGroup", mixinStandardHelpOptions = true, showDefaultValues = true)
public class CreateGroup implements Callable<Void> {
    @CommandLine.ParentCommand
    MQAdmin mqAdmin;

    @CommandLine.Option(names = {"-g", "--groupName"}, description = "Group name", required = true)
    String groupName;

    @CommandLine.Option(names = {"-d", "--maxDeliveryAttempt"}, description = "Max delivery attempt")
    int maxDeliveryAttempt = 16;

    @CommandLine.Option(names = {"-t", "--groupType"}, description = "Group type")
    GroupType groupType = GroupType.GROUP_TYPE_STANDARD;

    @CommandLine.Option(names = {"-m", "--subMode"}, description = "Subscription mode")
    SubscriptionMode subMode = SubscriptionMode.SUB_MODE_POP;

    @Override
    public Void call() throws Exception {
        try (ControllerClient client = new GrpcControllerClient(new CliClientConfig())) {
            CreateGroupRequest request = CreateGroupRequest.newBuilder()
                .setName(groupName)
                .setMaxDeliveryAttempt(maxDeliveryAttempt)
                .setGroupType(groupType)
                .setSubMode(subMode)
                .build();

            long groupId = client.createGroup(mqAdmin.getEndpoint(), request).join();
            System.out.println("Group created: " + groupId);
        }
        return null;
    }
}
