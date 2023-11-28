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

import apache.rocketmq.controller.v1.Topic;
import apache.rocketmq.controller.v1.UpdateTopicRequest;
import com.automq.rocketmq.cli.CliClientConfig;
import com.automq.rocketmq.cli.ConsoleHelper;
import com.automq.rocketmq.cli.MQAdmin;
import com.automq.rocketmq.controller.ControllerClient;
import com.automq.rocketmq.controller.client.GrpcControllerClient;
import com.google.common.base.Strings;
import java.util.concurrent.Callable;
import picocli.CommandLine;

@CommandLine.Command(name = "updateTopic", mixinStandardHelpOptions = true, showDefaultValues = true)
public class UpdateTopic implements Callable<Void> {

    @CommandLine.Option(names = {"-i", "--topicId"}, description = "Topic ID", required = true)
    long topicId;

    @CommandLine.Option(names = {"-t", "--topicName"}, description = "Topic name")
    String topicName;

    @CommandLine.Option(names = {"-q", "--queueNumber"}, description = "Queue number")
    int queueNumber = 0;

    @CommandLine.ParentCommand
    MQAdmin mqAdmin;

    @Override
    public Void call() throws Exception {
        try (ControllerClient client = new GrpcControllerClient(new CliClientConfig())) {
            Topic topic = client.describeTopic(mqAdmin.getEndpoint(), topicId, null)
                .join();
            if (null == topic) {
                System.err.printf("Topic '%s' is not found%n%n", topicName);
                return null;
            }

            UpdateTopicRequest.Builder builder = UpdateTopicRequest.newBuilder()
                .setTopicId(topicId);
            if (queueNumber > topic.getCount()) {
                builder.setCount(queueNumber);
            }

            if (!Strings.isNullOrEmpty(topicName)) {
                builder.setName(topicName);
            }
            client.updateTopic(mqAdmin.getEndpoint(), builder.build()).join();
            topic = client.describeTopic(mqAdmin.getEndpoint(), topicId, null).join();
            ConsoleHelper.printTable(topic);
        }
        return null;
    }
}
