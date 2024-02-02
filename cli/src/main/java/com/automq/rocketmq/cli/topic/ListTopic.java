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

package com.automq.rocketmq.cli.topic;

import apache.rocketmq.controller.v1.ListTopicsReply;
import apache.rocketmq.controller.v1.ListTopicsRequest;
import apache.rocketmq.controller.v1.MessageType;
import apache.rocketmq.controller.v1.Topic;
import com.automq.rocketmq.cli.CliClientConfig;
import com.automq.rocketmq.cli.ConsoleHelper;
import com.automq.rocketmq.cli.MQAdmin;
import com.automq.rocketmq.controller.ControllerClient;
import com.automq.rocketmq.controller.client.GrpcControllerClient;
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

            client.listTopics(mqAdmin.getEndpoint(), request, new StreamObserver<>() {
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
