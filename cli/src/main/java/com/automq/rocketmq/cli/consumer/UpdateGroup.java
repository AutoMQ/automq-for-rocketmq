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

import apache.rocketmq.controller.v1.GroupType;
import apache.rocketmq.controller.v1.UpdateGroupRequest;
import com.automq.rocketmq.cli.CliClientConfig;
import com.automq.rocketmq.cli.MQAdmin;
import com.automq.rocketmq.controller.ControllerClient;
import com.automq.rocketmq.controller.client.GrpcControllerClient;
import com.google.common.base.Strings;
import java.util.concurrent.Callable;
import picocli.CommandLine;

@CommandLine.Command(name = "updateGroup", mixinStandardHelpOptions = true, showDefaultValues = true)
public class UpdateGroup implements Callable<Void> {

    @CommandLine.ParentCommand
    MQAdmin mqAdmin;

    @CommandLine.Option(names = {"-i", "--id"}, description = "Group ID", required = true)
    long groupId;

    @CommandLine.Option(names = {"-n", "--name"}, description = "Group Name")
    String name;

    @CommandLine.Option(names = {"-d", "--deadLetterTopicId"}, description = "Dead Letter Topic ID")
    Long deadLetterTopicId;

    @CommandLine.Option(names = {"-m", "--maxDeliveryAttempt"}, description = "Max Delivery Attempt")
    Integer maxDeliveryAttempt;

    @CommandLine.Option(names = {"-t", "--type"}, description = "Group Name")
    GroupType groupType;

    @Override
    public Void call() throws Exception {
        try (ControllerClient client = new GrpcControllerClient(new CliClientConfig())) {
            UpdateGroupRequest.Builder builder = UpdateGroupRequest.newBuilder().setGroupId(groupId);
            if (!Strings.isNullOrEmpty(name)) {
                builder.setName(name);
            }

            if (null != deadLetterTopicId) {
                builder.setDeadLetterTopicId(deadLetterTopicId);
            }

            if (null != maxDeliveryAttempt) {
                builder.setMaxRetryAttempt(maxDeliveryAttempt);
            }

            if (null != groupType) {
                builder.setGroupType(groupType);
            }

            client.updateGroup(mqAdmin.getEndpoint(), builder.build()).join();
        }
        return null;
    }
}
