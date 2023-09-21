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

package com.automq.rocketmq.controller.metadata;

import apache.rocketmq.controller.v1.CreateTopicReply;
import apache.rocketmq.controller.v1.CreateTopicRequest;
import apache.rocketmq.controller.v1.NodeRegistrationReply;
import apache.rocketmq.controller.v1.NodeRegistrationRequest;
import apache.rocketmq.controller.v1.Code;
import apache.rocketmq.controller.v1.ControllerServiceGrpc;
import com.automq.rocketmq.controller.exception.ControllerException;
import com.automq.rocketmq.controller.metadata.database.dao.Node;
import com.google.common.base.Strings;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.MoreExecutors;
import io.grpc.Grpc;
import io.grpc.InsecureChannelCredentials;
import io.grpc.ManagedChannel;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GrpcControllerClient implements ControllerClient {

    private static final Logger LOGGER = LoggerFactory.getLogger(GrpcControllerClient.class);

    private final ConcurrentMap<String, ControllerServiceGrpc.ControllerServiceFutureStub> stubs;

    public GrpcControllerClient() {
        stubs = new ConcurrentHashMap<>();
    }

    public CompletableFuture<Node> registerBroker(String target, String name, String address,
        String instanceId) throws ControllerException {
        if (Strings.isNullOrEmpty(target)) {
            throw new ControllerException(Code.NO_LEADER_VALUE, "Target address to leader controller is null or empty");
        }

        if (!stubs.containsKey(target)) {
            ManagedChannel channel = Grpc.newChannelBuilder(target, InsecureChannelCredentials.create())
                .build();
            ControllerServiceGrpc.ControllerServiceFutureStub stub = ControllerServiceGrpc.newFutureStub(channel);
            stubs.putIfAbsent(target, stub);
        }
        NodeRegistrationRequest request = NodeRegistrationRequest.newBuilder()
            .setBrokerName(name)
            .setAddress(address)
            .setInstanceId(instanceId)
            .build();

        CompletableFuture<Node> future = new CompletableFuture<>();

        Futures.addCallback(stubs.get(target).registerNode(request), new FutureCallback<>() {
            @Override
            public void onSuccess(NodeRegistrationReply reply) {
                if (reply.getStatus().getCode() == Code.OK) {
                    Node node = new Node();
                    node.setName(name);
                    node.setAddress(address);
                    node.setInstanceId(instanceId);

                    node.setEpoch(reply.getEpoch());
                    node.setId(reply.getId());
                    future.complete(node);
                } else {
                    future.completeExceptionally(new ControllerException(reply.getStatus().getCode().getNumber(),
                        reply.getStatus().getMessage()));
                }
            }

            @Override
            public void onFailure(Throwable t) {
                future.completeExceptionally(new ControllerException(Code.INTERRUPTED_VALUE, t));
            }
        }, MoreExecutors.directExecutor());

        return future;
    }

    @Override
    public CompletableFuture<Long> createTopic(String target, String topicName, int queueNum)
        throws ControllerException {
        if (Strings.isNullOrEmpty(target)) {
            throw new ControllerException(Code.NO_LEADER_VALUE, "Target address to leader node is null or empty");
        }

        if (!stubs.containsKey(target)) {
            ManagedChannel channel = Grpc.newChannelBuilder(target, InsecureChannelCredentials.create()).build();
            ControllerServiceGrpc.ControllerServiceFutureStub stub = ControllerServiceGrpc.newFutureStub(channel);
            stubs.putIfAbsent(target, stub);
        }

        ControllerServiceGrpc.ControllerServiceFutureStub stub = stubs.get(target);
        CreateTopicRequest request = CreateTopicRequest.newBuilder().setTopic(topicName).setCount(queueNum).build();

        CompletableFuture<Long> future = new CompletableFuture<>();
        Futures.addCallback(stub.createTopic(request), new FutureCallback<>() {
            @Override
            public void onSuccess(CreateTopicReply result) {
                LOGGER.info("Leader has created topic for {} with topic-id={}", topicName, result.getTopicId());
                switch (result.getStatus().getCode()) {
                    case OK -> {
                        future.complete(result.getTopicId());
                    }
                    case DUPLICATED -> {
                        ControllerException e = new ControllerException(Code.DUPLICATED_VALUE, "Topic name is taken");
                        future.completeExceptionally(e);
                    }
                    default -> {
                        ControllerException e = new ControllerException(Code.INTERRUPTED_VALUE, "Internal error");
                        future.completeExceptionally(e);
                    }
                }
            }

            @Override
            public void onFailure(Throwable t) {
                future.completeExceptionally(t);
                LOGGER.error("Leader node failed to create topic on behalf", t);
            }
        }, MoreExecutors.directExecutor());
        return future;
    }
}
