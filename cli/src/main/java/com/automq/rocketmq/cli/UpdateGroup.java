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

package com.automq.rocketmq.cli;

import apache.rocketmq.controller.v1.GroupType;
import apache.rocketmq.controller.v1.UpdateGroupRequest;
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

            client.updateGroup(mqAdmin.endpoint, builder.build()).join();
        }
        return null;
    }
}
