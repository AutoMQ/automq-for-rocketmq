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

import com.automq.rocketmq.controller.metadata.GrpcControllerClient;
import java.io.IOException;
import java.util.concurrent.Callable;
import picocli.CommandLine;

@CommandLine.Command(name = "createTopic", mixinStandardHelpOptions = true)
public class CreateTopic implements Callable<Void> {
    @CommandLine.Option(names = {"-t", "--topicName"}, description = "Topic name", required = true)
    String topicName;
    @CommandLine.Option(names = {"-q", "--queueNums"}, description = "Queue number", required = true)
    int queueNums;

    @CommandLine.ParentCommand
    MQAdmin mqAdmin;

    @Override
    public Void call() throws Exception {
        GrpcControllerClient client = new GrpcControllerClient();
        Long topicId = client.createTopic(mqAdmin.endpoint, topicName, queueNums).join();
        System.out.println("Topic created: " + topicId);
        client.close();
        return null;
    }
}
