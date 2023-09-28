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
import apache.rocketmq.controller.v1.Topic;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

public class DefaultProxyMetadataService implements ProxyMetadataService {
    @Override
    public CompletableFuture<Topic> topicOf(String topicName) {
        return null;
    }

    @Override
    public CompletableFuture<Set<Integer>> queueAssignmentsOf(long topicId) {
        return null;
    }

    @Override
    public CompletableFuture<ConsumerGroup> consumerGroupOf(String groupName) {
        return null;
    }

    @Override
    public CompletableFuture<Long> consumerOffsetOf(long consumerGroupId, long topicId, int queueId) {
        return null;
    }

    @Override
    public CompletableFuture<Void> updateConsumerOffset(long consumerGroupId, long topicId, int queueId,
        long newOffset) {
        return null;
    }

    @Override
    public long queryTopicId(String name) {
        return 0;
    }

    @Override
    public Set<Integer> queryAssignmentQueueSet(long topicId) {
        return null;
    }

    @Override
    public long queryConsumerGroupId(String name) {
        return 0;
    }

    @Override
    public long queryConsumerOffset(long consumerGroupId, long topicId, int queueId) {
        return 0;
    }

    @Override
    public void updateConsumerOffset(long consumerGroupId, long topicId, int queueId, long offset, boolean retry) {

    }
}
