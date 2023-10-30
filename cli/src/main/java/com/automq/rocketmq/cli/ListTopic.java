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

import apache.rocketmq.controller.v1.ListTopicsReply;
import apache.rocketmq.controller.v1.ListTopicsRequest;
import apache.rocketmq.controller.v1.MessageType;
import apache.rocketmq.controller.v1.Topic;
import com.automq.rocketmq.controller.metadata.ControllerClient;
import com.automq.rocketmq.controller.metadata.GrpcControllerClient;
import de.vandermeer.asciitable.AT_Row;
import de.vandermeer.asciitable.AsciiTable;
import de.vandermeer.asciitable.CWC_LongestLine;
import io.grpc.stub.StreamObserver;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import picocli.CommandLine;

@CommandLine.Command(name = "listTopic", mixinStandardHelpOptions = true, showDefaultValues = true)
public class ListTopic implements Callable<Void> {
    @CommandLine.ParentCommand
    MQAdmin mqAdmin;

    @Override
    public Void call() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        try (ControllerClient client = new GrpcControllerClient(new CliClientConfig())) {
            ListTopicsRequest request = ListTopicsRequest.newBuilder().build();

            AsciiTable topicList = new AsciiTable();
            topicList.addRule();
            AT_Row row = topicList.addRow("ID", "NAME", "QUEUE NUMBER", "RETENTION HOURS", "ACCEPT TYPES");
            ConsoleHelper.alignCentral(row);
            topicList.addRule();

            CWC_LongestLine cwc = new CWC_LongestLine();

            client.listTopics(mqAdmin.endpoint, request, new StreamObserver<>() {
                @Override
                public void onNext(ListTopicsReply reply) {
                    Topic topic = reply.getTopic();
                    StringBuilder sb = new StringBuilder();
                    for (MessageType type : topic.getAcceptTypes().getTypesList()) {
                        if (!sb.isEmpty()) {
                            sb.append(", ");
                        }
                        sb.append(type.name());
                    }

                    AT_Row row = topicList.addRow(topic.getTopicId(), topic.getName(), topic.getCount(),
                        topic.getRetentionHours(), sb.toString());
                    ConsoleHelper.alignCentral(row);
                    topicList.addRule();
                }

                @Override
                public void onError(Throwable t) {
                    t.printStackTrace();
                    latch.countDown();
                }

                @Override
                public void onCompleted() {
                    topicList.getRenderer().setCWC(cwc);
                    String render = topicList.render();
                    System.out.println(render);
                    latch.countDown();
                }
            });
        }
        if (!latch.await(60, TimeUnit.SECONDS)) {
            System.err.println("Failed to got response from server within 60s");
        }
        return null;
    }
}
