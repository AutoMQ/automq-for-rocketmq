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

package com.automq.rocketmq.metadata;

import apache.rocketmq.controller.v1.ConsumerGroup;
import apache.rocketmq.controller.v1.CreateTopicRequest;
import apache.rocketmq.controller.v1.MessageQueueAssignment;
import apache.rocketmq.controller.v1.Topic;
import com.automq.rocketmq.controller.metadata.MetadataStore;
import com.automq.rocketmq.metadata.api.ProxyMetadataService;
import com.google.common.base.Stopwatch;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultProxyMetadataService implements ProxyMetadataService {

    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultProxyMetadataService.class);

    private final MetadataStore metadataStore;

    public DefaultProxyMetadataService(MetadataStore metadataStore) {
        this.metadataStore = metadataStore;
    }

    @Override
    public CompletableFuture<Topic> createTopic(CreateTopicRequest request) {
        return metadataStore.createTopic(request)
            .thenComposeAsync(this::topicOf);
    }

    @Override
    public CompletableFuture<Topic> topicOf(String topicName) {
        return topicOf(null, topicName);
    }

    @Override
    public CompletableFuture<Topic> topicOf(long topicId) {
        return topicOf(topicId, null);
    }

    private CompletableFuture<Topic> topicOf(Long topicId, String topicName) {
        Stopwatch stopwatch = Stopwatch.createStarted();
        return metadataStore.describeTopic(topicId, topicName).thenApply((topic -> {
            long elapsed = stopwatch.elapsed().toMillis();
            if (elapsed > 100) {
                LOGGER.warn("It took {}ms to query topic, id: {}, name: {}", elapsed, topicId, topicName);
            } else if (elapsed > 10) {
                LOGGER.debug("It took {}ms to query topic, id: {}, name: {}", elapsed, topicId, topicName);
            }
            return topic;
        }));
    }

    @Override
    public CompletableFuture<List<MessageQueueAssignment>> queueAssignmentsOf(String topicName) {
        return metadataStore.describeTopic(null, topicName)
            .thenApply(topic -> topic.getAssignmentsList()
                .stream()
                .filter(assignment -> assignment.getNodeId() == metadataStore.config().nodeId()).toList());
    }

    @Override
    public CompletableFuture<String> addressOf(int nodeId) {
        return metadataStore.addressOfNode(nodeId);
    }

    @Override
    public CompletableFuture<ConsumerGroup> consumerGroupOf(String groupName) {
        return metadataStore.describeGroup(null, groupName);
    }

    @Override
    public CompletableFuture<ConsumerGroup> consumerGroupOf(long consumerGroupId) {
        return metadataStore.describeGroup(consumerGroupId, null);
    }

    @Override
    public CompletableFuture<Long> consumerOffsetOf(long consumerGroupId, long topicId, int queueId) {
        return metadataStore.getConsumerOffset(consumerGroupId, topicId, queueId);
    }

    @Override
    public CompletableFuture<Void> updateConsumerOffset(long consumerGroupId, long topicId, int queueId,
        long newOffset) {
        return metadataStore.commitOffset(consumerGroupId, topicId, queueId, newOffset);
    }
}
