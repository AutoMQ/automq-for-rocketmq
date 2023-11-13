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
