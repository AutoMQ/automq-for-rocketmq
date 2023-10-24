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

package com.automq.rocketmq.proxy.mock;

import apache.rocketmq.controller.v1.AcceptTypes;
import apache.rocketmq.controller.v1.ConsumerGroup;
import apache.rocketmq.controller.v1.MessageQueue;
import apache.rocketmq.controller.v1.MessageQueueAssignment;
import apache.rocketmq.controller.v1.MessageType;
import apache.rocketmq.controller.v1.Topic;
import com.automq.rocketmq.metadata.api.ProxyMetadataService;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class MockProxyMetadataService implements ProxyMetadataService {
    Map<Long, Long> offsetMap = new HashMap<>();

    @Override
    public CompletableFuture<Topic> topicOf(String topicName) {
        // Return a dummy topic
        Topic.Builder topicBuilder = Topic.newBuilder();
        topicBuilder.setName(topicName);
        topicBuilder.setTopicId(2);
        topicBuilder.setCount(1);
        topicBuilder.setAcceptTypes(AcceptTypes.newBuilder().addTypes(MessageType.NORMAL).build());

        MessageQueueAssignment.Builder assignmentBuilder = MessageQueueAssignment.newBuilder();
        MessageQueue.Builder queueBuilder = MessageQueue.newBuilder();
        queueBuilder.setTopicId(2);
        queueBuilder.setQueueId(0);

        assignmentBuilder.setQueue(queueBuilder);
        assignmentBuilder.setNodeId(0);
        topicBuilder.addAssignments(assignmentBuilder);

        return CompletableFuture.completedFuture(topicBuilder.build());
    }

    @Override
    public CompletableFuture<Topic> topicOf(long topicId) {
        return null;
    }

    @Override
    public CompletableFuture<List<MessageQueueAssignment>> queueAssignmentsOf(String topicName) {
        return null;
    }

    @Override
    public CompletableFuture<String> addressOf(int brokerId) {
        return null;
    }

    @Override
    public CompletableFuture<ConsumerGroup> consumerGroupOf(String groupName) {
        long groupId = 8;
        return CompletableFuture.completedFuture(ConsumerGroup.newBuilder().setName(groupName).setGroupId(groupId).build());
    }

    @Override
    public CompletableFuture<ConsumerGroup> consumerGroupOf(long consumerGroupId) {
        return CompletableFuture.completedFuture(ConsumerGroup.newBuilder().setName("test").setGroupId(consumerGroupId).build());
    }

    @Override
    public CompletableFuture<Long> consumerOffsetOf(long consumerGroupId, long topicId, int queueId) {
        long offset = 0;
        if (offsetMap.containsKey(consumerGroupId + topicId + queueId)) {
            offset = offsetMap.get(consumerGroupId + topicId + queueId);
        }
        return CompletableFuture.completedFuture(offset);
    }

    @Override
    public CompletableFuture<Void> updateConsumerOffset(long consumerGroupId, long topicId, int queueId,
        long newOffset) {
        offsetMap.put(consumerGroupId + topicId + queueId, newOffset);
        return CompletableFuture.completedFuture(null);
    }
}
