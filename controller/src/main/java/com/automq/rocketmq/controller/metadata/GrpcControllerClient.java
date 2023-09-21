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

import apache.rocketmq.controller.v1.NodeRegistrationReply;
import apache.rocketmq.controller.v1.NodeRegistrationRequest;
import apache.rocketmq.controller.v1.Code;
import apache.rocketmq.controller.v1.ControllerServiceGrpc;
import com.automq.rocketmq.controller.exception.ControllerException;
import com.automq.rocketmq.controller.metadata.database.dao.Node;
import com.google.common.base.Strings;
import com.google.common.util.concurrent.ListenableFuture;
import io.grpc.Grpc;
import io.grpc.InsecureChannelCredentials;
import io.grpc.ManagedChannel;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;

public class GrpcControllerClient implements ControllerClient {
    private final ConcurrentMap<String, ControllerServiceGrpc.ControllerServiceFutureStub> stubs;

    public GrpcControllerClient() {
        stubs = new ConcurrentHashMap<>();
    }

    public Node registerBroker(String target, String name, String address,
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

        ListenableFuture<NodeRegistrationReply> future = stubs.get(target).registerNode(request);
        try {
            NodeRegistrationReply reply = future.get();
            if (reply.getStatus().getCode() == Code.OK) {
                Node node = new Node();
                node.setName(name);
                node.setAddress(address);
                node.setInstanceId(instanceId);

                node.setEpoch(reply.getEpoch());
                node.setId(reply.getId());
                return node;
            } else {
                throw new ControllerException(reply.getStatus().getCode().getNumber(), reply.getStatus().getMessage());
            }
        } catch (ExecutionException | InterruptedException e) {
            throw new ControllerException(Code.BAD_REQUEST_VALUE, e);
        }
    }

    @Override
    public void createTopic(String target, String topicName, int queueNum) throws ControllerException {

    }
}
