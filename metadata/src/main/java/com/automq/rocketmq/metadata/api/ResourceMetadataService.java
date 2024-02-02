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

import apache.rocketmq.controller.v1.ConsumerGroup;
import apache.rocketmq.controller.v1.Topic;
import java.util.concurrent.CompletableFuture;

public interface ResourceMetadataService {

    /**
     * Query the topic metadata of a given topic name.
     *
     * @param topicName topic name
     * @return {@link CompletableFuture} of {@link Topic}
     */
    CompletableFuture<Topic> topicOf(String topicName);

    /**
     * Query the topic metadata of a given topic id.
     *
     * @param topicId topic id
     * @return {@link CompletableFuture} of {@link Topic}
     */
    CompletableFuture<Topic> topicOf(long topicId);

    /**
     * Query the consumer group metadata of a given group name
     *
     * @param groupName group name
     * @return {@link CompletableFuture} of {@link ConsumerGroup}
     */
    CompletableFuture<ConsumerGroup> consumerGroupOf(String groupName);

    /**
     * Query the consumer group metadata of a given group id
     *
     * @param consumerGroupId consumer group id
     * @return {@link CompletableFuture} of {@link ConsumerGroup}
     */
    CompletableFuture<ConsumerGroup> consumerGroupOf(long consumerGroupId);
}
