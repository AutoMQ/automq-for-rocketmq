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

package com.automq.rocketmq.cli.topic;

import apache.rocketmq.controller.v1.AcceptTypes;
import apache.rocketmq.controller.v1.CreateTopicRequest;
import apache.rocketmq.controller.v1.MessageType;
import com.automq.rocketmq.cli.CliClientConfig;
import com.automq.rocketmq.cli.MQAdmin;
import com.automq.rocketmq.common.util.DurationUtil;
import com.automq.rocketmq.controller.client.GrpcControllerClient;
import java.util.concurrent.Callable;
import picocli.CommandLine;

@CommandLine.Command(name = "createTopic", mixinStandardHelpOptions = true, showDefaultValues = true)
public class CreateTopic implements Callable<Void> {
    @CommandLine.Option(names = {"-t", "--topicName"}, description = "Topic name", required = true)
    String topicName;

    @CommandLine.Option(names = {"-q", "--queueNums"}, description = "Queue number")
    int queueNums = 1;

    @CommandLine.Option(names = {"-m", "--messageType"}, description = "Message type")
    MessageType messageType = MessageType.NORMAL;

    @CommandLine.Option(names = {"--ttl"}, description = "Time to live of the topic")
    String ttl = "3d0h";

    @CommandLine.ParentCommand
    MQAdmin mqAdmin;

    @Override
    public Void call() throws Exception {
        long retentionHours = 0;
        try {
            retentionHours = DurationUtil.parse(this.ttl).toHours();
            if (retentionHours > Integer.MAX_VALUE) {
                System.err.println("Invalid ttl: " + this.ttl + ", max value is 2147483647h");
                System.exit(1);
            }
            if (retentionHours < 1) {
                System.err.println("Invalid ttl: " + this.ttl + ", min value is 1h");
                System.exit(1);
            }
        } catch (Exception e) {
            System.err.println("Invalid ttl: " + this.ttl);
            System.exit(1);
        }

        GrpcControllerClient client = new GrpcControllerClient(new CliClientConfig());
        CreateTopicRequest request = CreateTopicRequest.newBuilder()
            .setTopic(topicName)
            .setCount(queueNums)
            .setAcceptTypes(AcceptTypes.newBuilder().addTypes(messageType).build())
            .setRetentionHours((int) retentionHours)
            .build();

        Long topicId = client.createTopic(mqAdmin.getEndpoint(), request).join();
        System.out.println("Topic created: " + topicId);
        client.close();
        return null;
    }
}
