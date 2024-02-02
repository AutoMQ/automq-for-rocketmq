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
