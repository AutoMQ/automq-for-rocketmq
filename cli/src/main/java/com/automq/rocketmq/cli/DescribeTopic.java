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

import apache.rocketmq.controller.v1.Topic;
import com.automq.rocketmq.controller.metadata.GrpcControllerClient;
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
        try (GrpcControllerClient client = new GrpcControllerClient()) {
            Topic topic = client.describeTopic(mqAdmin.endpoint, null, topicName)
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
