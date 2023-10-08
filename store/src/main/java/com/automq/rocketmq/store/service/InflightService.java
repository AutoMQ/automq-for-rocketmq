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
