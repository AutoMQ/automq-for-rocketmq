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

package com.automq.rocketmq.proxy.service;

import apache.rocketmq.proxy.v1.QueueStats;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.commons.lang3.tuple.Pair;

public interface ExtendMessageService {

    /**
     * Reset the consume offset of the given consumer group to the given offset.
     *
     * @param topic The topic name.
     * @param queueId The queue id of the queue.
     * @param consumerGroup The consumer group.
     * @param newConsumeOffset The new consume offset.
     */
    CompletableFuture<Void> resetConsumeOffset(String topic, int queueId, String consumerGroup, long newConsumeOffset);

    /**
     * Reset the consume offset of the given consumer group to the given timestamp.
     *
     * @param topic The topic name.
     * @param queueId The queue id of the queue.
     * @param consumerGroup The consumer group.
     * @param timestamp The timestamp. The offset will be the earliest offset larger than the given timestamp.
     */
    CompletableFuture<Void> resetConsumeOffsetByTimestamp(String topic, int queueId, String consumerGroup, long timestamp);

    /**
     * Get the stats of the given topic.
     *
     * @param topic         The topic name.
     * @param queueId       The queue id of the queue.
     * @param consumerGroup The consumer group.
     * @return The stats of the given topic.
     */
    CompletableFuture<Pair<Long, List<QueueStats>>> getTopicStats(String topic, int queueId, String consumerGroup);
}
