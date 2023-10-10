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

import apache.rocketmq.controller.v1.CreateGroupReply;
import apache.rocketmq.controller.v1.CreateGroupRequest;
import apache.rocketmq.controller.v1.GroupType;
import com.automq.rocketmq.controller.metadata.GrpcControllerClient;
import java.util.concurrent.Callable;
import picocli.CommandLine;

@CommandLine.Command(name = "createGroup", mixinStandardHelpOptions = true)
public class CreateGroup implements Callable<Void>  {
    @CommandLine.ParentCommand
    MQAdmin mqAdmin;

    @CommandLine.Option(names = {"-g", "--groupName"}, description = "Group name", required = true)
    private String groupName;
    @CommandLine.Option(names = {"-r", "--maxRetryAttempt"}, description = "Max retry attempt", defaultValue = "16")
    private int maxRetryAttempt;
    @CommandLine.Option(names = {"-t", "--groupType"}, description = "Group type", defaultValue = "GROUP_TYPE_STANDARD")
    private GroupType groupType;

    @Override
    public Void call() throws Exception {
        GrpcControllerClient client = new GrpcControllerClient();
        CreateGroupRequest request = CreateGroupRequest.newBuilder()
            .setName(groupName)
            .setMaxRetryAttempt(maxRetryAttempt)
            .setGroupType(groupType)
            .build();

        CreateGroupReply groupReply = client.createGroup(mqAdmin.endpoint, request).join();
        System.out.println("Group created: " + groupReply.getGroupId());
        client.close();
        return null;
    }
}
