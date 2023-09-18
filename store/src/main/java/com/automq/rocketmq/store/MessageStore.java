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

package com.automq.rocketmq.store;

import com.automq.rocketmq.store.model.message.AckResult;
import com.automq.rocketmq.store.model.message.ChangeInvisibleDurationResult;
import com.automq.rocketmq.store.model.message.PopResult;
import java.util.concurrent.CompletableFuture;

public interface MessageStore {
    /**
     * Pop message from specified topic and queue.
     *
     * @param consumerGroupId   consumer group id that launches this query
     * @param topicId           topic id to pop message from
     * @param queueId           queue id to pop message from
     * @param offset            offset to start from
     * @param batchSize         maximum count of messages
     * @param isOrder           is orderly pop
     * @param invisibleDuration the duration for the next time this batch of messages will be visible, in nanoseconds
     * @return pop result, see {@link PopResult}
     */
    CompletableFuture<PopResult> pop(long consumerGroupId, long topicId, int queueId, long offset, int batchSize,
        boolean isOrder, boolean isRetry, long invisibleDuration);

    /**
     * Ack message.
     *
     * @param receiptHandle unique receipt handle to identify inflight message
     * @return ack result, see {@link AckResult}
     */
    CompletableFuture<AckResult> ack(String receiptHandle);

    /**
     * Change invisible duration.
     *
     * @param receiptHandle     unique receipt handle to identify inflight message
     * @param invisibleDuration the duration for the next time this batch of messages will be visible, in nanoseconds
     * @return change invisible duration result, see {@link ChangeInvisibleDurationResult}
     */
    CompletableFuture<ChangeInvisibleDurationResult> changeInvisibleDuration(String receiptHandle,
        long invisibleDuration);

    int getInflightStatsByQueue(long topicId, int queueId);

    boolean cleanMetadata(long topicId, int queueId);
}
