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

package com.automq.rocketmq.metadata.api;

import apache.rocketmq.controller.v1.CreateTopicRequest;
import apache.rocketmq.controller.v1.MessageQueueAssignment;
import apache.rocketmq.controller.v1.Topic;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public interface ProxyMetadataService extends ResourceMetadataService {

    CompletableFuture<Topic> createTopic(CreateTopicRequest request);

    /**
     * List all the queue assignments for a given topic that assigned to the current server.
     *
     * @param topicName topic name
     * @return {@link CompletableFuture} of {@link List<MessageQueueAssignment>}
     */
    CompletableFuture<List<MessageQueueAssignment>> queueAssignmentsOf(String topicName);

    /**
     * Query the advertised address of a given broker id.
     * @param nodeId node id
     * @return {@link CompletableFuture} of {@link String}
     */
    CompletableFuture<String> addressOf(int nodeId);

    /**
     * Query the consumer offset of a given consumer group, topic and queue.
     *
     * @param consumerGroupId consumer group id
     * @param topicId topic id
     * @param queueId queue id
     * @return {@link CompletableFuture} of {@link Long}
     */
    CompletableFuture<Long> consumerOffsetOf(long consumerGroupId, long topicId, int queueId);

    /**
     * Update the newest consumer offset of a given consumer group, topic and queue.
     *
     * @param consumerGroupId consumer group id
     * @param topicId topic id
     * @param queueId queue id
     * @param newOffset new offset
     * @return {@link CompletableFuture} of {@link Void}
     */
    CompletableFuture<Void> updateConsumerOffset(long consumerGroupId, long topicId, int queueId, long newOffset);
}
