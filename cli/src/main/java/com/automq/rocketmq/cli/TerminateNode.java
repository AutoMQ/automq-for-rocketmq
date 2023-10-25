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

import apache.rocketmq.controller.v1.TerminateNodeReply;
import apache.rocketmq.controller.v1.TerminateNodeRequest;
import com.automq.rocketmq.controller.metadata.GrpcControllerClient;
import io.grpc.stub.StreamObserver;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import picocli.CommandLine;

@CommandLine.Command(name = "terminateNode", mixinStandardHelpOptions = true, showDefaultValues = true)
public class TerminateNode implements Callable<Void> {

    @CommandLine.Option(names = {"-n", "--nodeId"}, description = "Node ID", required = true)
    int nodeId;

    @CommandLine.ParentCommand
    MQAdmin mqAdmin;

    @Override
    public Void call() throws Exception {
        final CountDownLatch latch = new CountDownLatch(1);
        try (GrpcControllerClient client = new GrpcControllerClient()) {
            TerminateNodeRequest request = TerminateNodeRequest.newBuilder().setNodeId(nodeId).build();
            client.terminateNode(mqAdmin.endpoint, request, new StreamObserver<>() {
                @Override
                public void onNext(TerminateNodeReply value) {
                    switch (value.getStatus().getCode()) {
                        case OK -> {
                            System.out.printf("Node Current Termination Stage: %s%n", value.getStage());
                        }
                        case BAD_REQUEST -> {
                            System.err.printf("%s%n", value.getStatus().getMessage());
                            latch.countDown();
                        }
                    }
                }

                @Override
                public void onError(Throwable t) {
                    t.printStackTrace();
                    latch.countDown();
                }

                @Override
                public void onCompleted() {
                    latch.countDown();
                }
            });
        }
        boolean reachedZero = latch.await(3, TimeUnit.MINUTES);
        if (!reachedZero) {
            System.err.println("Timeout before broker node termination completes");
        }
        return null;
    }
}
