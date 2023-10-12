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

package com.automq.rocketmq.store.api;

import com.automq.rocketmq.common.model.generated.FlatMessage;
import com.automq.rocketmq.common.util.Lifecycle;
import com.automq.rocketmq.store.model.message.AckResult;
import com.automq.rocketmq.store.model.message.ChangeInvisibleDurationResult;
import com.automq.rocketmq.store.model.message.Filter;
import com.automq.rocketmq.store.model.message.PopResult;
import com.automq.rocketmq.store.model.message.PutResult;
import java.util.concurrent.CompletableFuture;

public interface MessageStore extends Lifecycle {

    TopicQueueManager getTopicQueueManager();

    S3ObjectOperator getS3ObjectOperator();

    /**
     * Pop message from specified topic and queue.
     *
     * @param consumerGroupId   consumer group id that launches this query
     * @param topicId           topic id to pop message from
     * @param queueId           queue id to pop message from
     * @param filter            filter to apply to messages
     * @param batchSize         maximum count of messages
     * @param fifo              is orderly pop
     * @param invisibleDuration the duration for the next time this batch of messages will be visible, in milliseconds
     * @return pop result, see {@link PopResult}
     */
    CompletableFuture<PopResult> pop(long consumerGroupId, long topicId, int queueId, Filter filter,
        int batchSize, boolean fifo, boolean retry, long invisibleDuration);


    /**
     * Put a message to the specified topic and queue.
     *
     * @param flatMessage flat message to append
     * @return append result with an offset assigned to the message, see {@link PutResult}
     */
    CompletableFuture<PutResult> put(FlatMessage flatMessage);

    /**
     * Ack message.
     *
     * @param receiptHandle unique receipt handle to identify inflight message
     * @return ack result, see {@link AckResult}
     */
    CompletableFuture<AckResult> ack(String receiptHandle);

    /**
     * Change invisible duration for an inflight message.
     *
     * @param receiptHandle     unique receipt handle to identify inflight message
     * @param invisibleDuration the duration for the next time this batch of messages will be visible, in milliseconds
     * @return change invisible duration result, see {@link ChangeInvisibleDurationResult}
     */
    CompletableFuture<ChangeInvisibleDurationResult> changeInvisibleDuration(String receiptHandle, long invisibleDuration);

    /**
     * Close the specified queue.
     * <p>
     * Once a queue is closed, no more messages can be put into it, and move operation can apply to it.
     *
     * @param topicId the topic id of the queue to close
     * @param queueId queue id to close
     * @return {@link CompletableFuture} of close operation
     */
    CompletableFuture<Void> closeQueue(long topicId, int queueId);

    CompletableFuture<Integer> getInflightStats(long consumerGroupId, long topicId, int queueId);

    /**
     * Get offset range in queue.
     * @param topicId topic id
     * @param queueId queue id
     * @return offset range, <code>[startOffset, endOffset)</code>
     */
    CompletableFuture<LogicQueue.QueueOffsetRange> getOffsetRange(long topicId, int queueId);

    /**
     * Get consume offset of specified consumer group.
     *
     * @param consumerGroupId consumer group id
     * @param topicId         topic id
     * @param queueId         queue id
     * @return consume offset
     */
    CompletableFuture<Long> getConsumeOffset(long consumerGroupId, long topicId, int queueId);
}
