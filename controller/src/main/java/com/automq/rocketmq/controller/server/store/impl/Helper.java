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

package com.automq.rocketmq.controller.server.store.impl;

import apache.rocketmq.controller.v1.AcceptTypes;
import apache.rocketmq.controller.v1.MessageQueue;
import apache.rocketmq.controller.v1.MessageQueueAssignment;
import apache.rocketmq.controller.v1.OngoingMessageQueueReassignment;
import apache.rocketmq.controller.v1.Topic;
import com.automq.rocketmq.controller.server.store.impl.cache.Inflight;
import com.automq.rocketmq.metadata.dao.QueueAssignment;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentMap;

public class Helper {
    public static Topic buildTopic(com.automq.rocketmq.metadata.dao.Topic topic,
        Collection<QueueAssignment> assignments) throws InvalidProtocolBufferException {
        AcceptTypes.Builder builder = AcceptTypes.newBuilder();

        // This method is kind of CPU-intensive
        JsonFormat.parser().ignoringUnknownFields().merge(topic.getAcceptMessageTypes(), builder);

        apache.rocketmq.controller.v1.Topic.Builder topicBuilder = apache.rocketmq.controller.v1.Topic
            .newBuilder()
            .setTopicId(topic.getId())
            .setName(topic.getName())
            .setCount(topic.getQueueNum())
            .setRetentionHours(topic.getRetentionHours())
            .setAcceptTypes(builder.build());
        setAssignments(topicBuilder, assignments);
        return topicBuilder.build();
    }

    public static void setAssignments(Topic.Builder topicBuilder, Collection<QueueAssignment> assignments) {
        if (null != assignments) {
            for (QueueAssignment assignment : assignments) {
                switch (assignment.getStatus()) {
                    case ASSIGNMENT_STATUS_DELETED -> {
                    }

                    case ASSIGNMENT_STATUS_ASSIGNED -> {
                        MessageQueueAssignment queueAssignment = MessageQueueAssignment.newBuilder()
                            .setQueue(MessageQueue.newBuilder()
                                .setTopicId(assignment.getTopicId())
                                .setQueueId(assignment.getQueueId()))
                            .setNodeId(assignment.getDstNodeId())
                            .build();
                        topicBuilder.addAssignments(queueAssignment);
                    }

                    case ASSIGNMENT_STATUS_YIELDING -> {
                        OngoingMessageQueueReassignment reassignment = OngoingMessageQueueReassignment.newBuilder()
                            .setQueue(MessageQueue.newBuilder()
                                .setTopicId(assignment.getTopicId())
                                .setQueueId(assignment.getQueueId())
                                .build())
                            .setSrcNodeId(assignment.getSrcNodeId())
                            .setDstNodeId(assignment.getDstNodeId())
                            .build();
                        topicBuilder.addReassignments(reassignment);
                    }
                }
            }
        }
    }

    public enum AddFutureResult {
        /**
         * Current request is the leader of the Inflight request chain, should fire a query or RPC call immediately.
         */
        LEADER,

        /**
         * There has been an outstanding request in progress. Added to the wait till response of the prior
         * request to arrive.
         */
        FOLLOWER,

        /**
         * The Inflight has already completed and cache should have been updated. Please retry to serve with cache.
         */
        COMPLETED,
    }

    public static <K, T> AddFutureResult addFuture(K key, CompletableFuture<T> future,
        ConcurrentMap<K, Inflight<T>> map) {
        Inflight<T> prev = map.get(key);
        if (null == prev) {
            Inflight<T> inflight = new Inflight<>();
            prev = map.putIfAbsent(key, inflight);
            if (null == prev) {
                boolean successful = inflight.addFuture(future);
                assert successful;
                return AddFutureResult.LEADER;
            }
        }

        if (!prev.addFuture(future)) {
            return AddFutureResult.COMPLETED;
        }

        return AddFutureResult.FOLLOWER;
    }
}
