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

package com.automq.rocketmq.store.service;

import com.automq.rocketmq.store.model.message.TopicQueueId;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

public class InflightService {
    private final ConcurrentMap<TopicQueueId /*topic-queue*/, ConcurrentMap<Long /*consumer-group*/, AtomicInteger>> inflightMap = new ConcurrentHashMap<>();

    private AtomicInteger getCounter(long consumerGroupId, long topicId, int queueId) {
        return inflightMap.computeIfAbsent(TopicQueueId.of(topicId, queueId), v -> new ConcurrentHashMap<>())
            .computeIfAbsent(consumerGroupId, v -> new AtomicInteger());
    }

    public void increaseInflightCount(long consumerGroupId, long topicId, int queueId, int count) {
        AtomicInteger counter = getCounter(consumerGroupId, topicId, queueId);
        counter.getAndAdd(count);
    }

    public void decreaseInflightCount(long consumerGroupId, long topicId, int queueId, int count) {
        AtomicInteger counter = getCounter(consumerGroupId, topicId, queueId);
        counter.getAndAdd(-1 * count);
    }

    public int getInflightCount(long consumerGroupId, long topicId, int queueId) {
        AtomicInteger counter = getCounter(consumerGroupId, topicId, queueId);
        return counter.get();
    }

    public void clearInflightCount(long topicId, int queueId) {
        inflightMap.remove(TopicQueueId.of(topicId, queueId));
    }
}
