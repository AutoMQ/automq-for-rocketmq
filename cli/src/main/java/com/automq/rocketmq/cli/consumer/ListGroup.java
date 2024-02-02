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

import apache.rocketmq.controller.v1.ConsumerGroup;
import apache.rocketmq.controller.v1.ListGroupReply;
import apache.rocketmq.controller.v1.ListGroupRequest;
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

@CommandLine.Command(name = "listGroup", mixinStandardHelpOptions = true, showDefaultValues = true)
public class ListGroup implements Callable<Void> {

    @CommandLine.ParentCommand
    MQAdmin mqAdmin;

    @Override
    public Void call() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        try (ControllerClient client = new GrpcControllerClient(new CliClientConfig())) {
            ListGroupRequest request = ListGroupRequest.newBuilder().build();
            CWC_LongestLine cwc = new CWC_LongestLine();
            AsciiTable listGroups = new AsciiTable();
            listGroups.addRule();
            AT_Row row = listGroups.addRow("ID", "NAME", "TYPE", "MAX DELIVERY ATTEMPT", "DEAD LETTER TOPIC");
            ConsoleHelper.alignCentral(row);
            listGroups.addRule();

            client.listGroups(mqAdmin.getEndpoint(), request, new StreamObserver<>() {
                @Override
                public void onNext(ListGroupReply value) {
                    ConsumerGroup group = value.getGroup();
                    AT_Row row = listGroups.addRow(group.getGroupId(), group.getName(), group.getGroupType(), group.getMaxDeliveryAttempt(), group.getDeadLetterTopicId());
                    ConsoleHelper.alignCentral(row);
                    listGroups.addRule();
                }

                @Override
                public void onError(Throwable t) {
                    t.printStackTrace();
                    latch.countDown();
                }

                @Override
                public void onCompleted() {
                    listGroups.getRenderer().setCWC(cwc);
                    String render = listGroups.render();
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
