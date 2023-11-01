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

import apache.rocketmq.controller.v1.ConsumerGroup;
import apache.rocketmq.controller.v1.ListGroupReply;
import apache.rocketmq.controller.v1.ListGroupRequest;
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

            client.listGroups(mqAdmin.endpoint, request, new StreamObserver<>() {
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
