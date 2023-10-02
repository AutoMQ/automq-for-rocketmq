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

package com.automq.rocketmq.proxy.grpc.activity;

import apache.rocketmq.v2.Assignment;
import apache.rocketmq.v2.MessageQueue;
import apache.rocketmq.v2.QueryAssignmentRequest;
import apache.rocketmq.v2.QueryAssignmentResponse;
import apache.rocketmq.v2.QueryRouteRequest;
import apache.rocketmq.v2.QueryRouteResponse;
import com.automq.rocketmq.proxy.model.VirtualQueue;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.grpc.v2.channel.GrpcChannelManager;
import org.apache.rocketmq.proxy.grpc.v2.common.GrpcClientSettingsManager;
import org.apache.rocketmq.proxy.grpc.v2.route.RouteActivity;
import org.apache.rocketmq.proxy.processor.MessagingProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The gRPC clients can recognize the real queue id of current metadata model.
 * <p>
 * This activity is used to extend the route activity to support the new metadata model.
 */
public class ExtendRouteActivity extends RouteActivity {
    public static final Logger LOGGER = LoggerFactory.getLogger(ExtendRouteActivity.class);

    public ExtendRouteActivity(MessagingProcessor messagingProcessor,
        GrpcClientSettingsManager grpcClientSettingsManager,
        GrpcChannelManager grpcChannelManager) {
        super(messagingProcessor, grpcClientSettingsManager, grpcChannelManager);
    }

    @Override
    public CompletableFuture<QueryRouteResponse> queryRoute(ProxyContext ctx, QueryRouteRequest request) {
        return super.queryRoute(ctx, request).thenApply(response -> {
            QueryRouteResponse.Builder newBuilder = QueryRouteResponse.newBuilder(response);
            List<MessageQueue> mqList = newBuilder.getMessageQueuesList();
            for (int i = 0; i < mqList.size(); i++) {
                MessageQueue messageQueue = mqList.get(i);
                VirtualQueue virtualQueue = new VirtualQueue(messageQueue.getBroker().getName());

                MessageQueue newQueue = MessageQueue.newBuilder(messageQueue).setId(virtualQueue.physicalQueueId()).build();
                newBuilder.setMessageQueues(i, newQueue);
            }
            return newBuilder.build();
        });
    }

    @Override
    public CompletableFuture<QueryAssignmentResponse> queryAssignment(ProxyContext ctx,
        QueryAssignmentRequest request) {
        return super.queryAssignment(ctx, request).thenApply(response -> {
            QueryAssignmentResponse.Builder newBuilder = QueryAssignmentResponse.newBuilder(response);
            List<Assignment> assignmentsList = newBuilder.getAssignmentsList();
            for (int i = 0; i < assignmentsList.size(); i++) {
                Assignment assignment = assignmentsList.get(i);
                MessageQueue messageQueue = assignment.getMessageQueue();
                VirtualQueue virtualQueue = new VirtualQueue(messageQueue.getBroker().getName());

                MessageQueue newQueue = MessageQueue.newBuilder(messageQueue).setId(virtualQueue.physicalQueueId()).build();
                Assignment newAssignment = Assignment.newBuilder(assignment).setMessageQueue(newQueue).build();
                newBuilder.setAssignments(i, newAssignment);
            }
            return newBuilder.build();
        });
    }
}
