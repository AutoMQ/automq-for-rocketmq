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

package com.automq.rocketmq.cli.broker;

import apache.rocketmq.controller.v1.TerminateNodeReply;
import apache.rocketmq.controller.v1.TerminateNodeRequest;
import com.automq.rocketmq.cli.CliClientConfig;
import com.automq.rocketmq.cli.MQAdmin;
import com.automq.rocketmq.controller.ControllerClient;
import com.automq.rocketmq.controller.client.GrpcControllerClient;
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
        try (ControllerClient client = new GrpcControllerClient(new CliClientConfig())) {
            TerminateNodeRequest request = TerminateNodeRequest.newBuilder().setNodeId(nodeId).build();
            client.terminateNode(mqAdmin.getEndpoint(), request, new StreamObserver<>() {
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
