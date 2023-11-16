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
import com.automq.rocketmq.store.model.StoreContext;
import com.automq.rocketmq.store.model.message.AckResult;
import com.automq.rocketmq.store.model.message.ChangeInvisibleDurationResult;
import com.automq.rocketmq.store.model.message.Filter;
import com.automq.rocketmq.store.model.message.PopResult;
import com.automq.rocketmq.store.model.message.PullResult;
import com.automq.rocketmq.store.model.message.PutResult;
import com.automq.rocketmq.store.model.message.ResetConsumeOffsetResult;
import java.util.concurrent.CompletableFuture;

public abstract class LogicQueue {
    protected long topicId;
    protected int queueId;

    protected LogicQueue(long topicId, int queueId) {
        this.topicId = topicId;
        this.queueId = queueId;
    }

    public long topicId() {
        return topicId;
    }

    public int queueId() {
        return queueId;
    }

    public abstract CompletableFuture<Void> open();

    public abstract CompletableFuture<Void> close();

    public abstract CompletableFuture<PutResult> put(StoreContext context, FlatMessage flatMessage);

    public abstract CompletableFuture<PutResult> putRetry(StoreContext context, long consumerGroupId,
        FlatMessage flatMessage);

    public abstract CompletableFuture<PopResult> popNormal(StoreContext context, long consumerGroup, Filter filter,
        int batchSize, long invisibleDuration);

    public abstract CompletableFuture<PopResult> popFifo(StoreContext context, long consumerGroup, Filter filter,
        int batchSize, long invisibleDuration);

    public abstract CompletableFuture<PopResult> popRetry(StoreContext context, long consumerGroup, Filter filter,
        int batchSize, long invisibleDuration);

    public abstract CompletableFuture<AckResult> ack(String receiptHandle);

    public abstract CompletableFuture<AckResult> ackTimeout(String receiptHandle);

    public abstract CompletableFuture<ChangeInvisibleDurationResult> changeInvisibleDuration(String receiptHandle,
        long invisibleDuration);

    public abstract CompletableFuture<ResetConsumeOffsetResult> resetConsumeOffset(long consumerGroupId, long offset);

    public abstract CompletableFuture<QueueOffsetRange> getOffsetRange();

    public abstract int getInflightStats(long consumerGroupId);

    public abstract CompletableFuture<PullResult> pullNormal(long consumerGroupId, Filter filter, long startOffset,
        int batchSize);

    public abstract CompletableFuture<PullResult> pullRetry(long consumerGroupId, Filter filter, long startOffset,
        int batchSize);

    public abstract long getConsumeOffset(long consumerGroupId);

    public abstract long getAckOffset(long consumerGroupId);

    public abstract long getRetryConsumeOffset(long consumerGroupId);

    public abstract long getRetryAckOffset(long consumerGroupId);

    public abstract State getState();

    public abstract int getConsumeTimes(long consumerGroupId, long offset);

    public enum State {
        INIT,
        OPENING,
        OPENED,
        CLOSING,
        CLOSED
    }

    public record QueueOffsetRange(long startOffset, long endOffset) {
    }
}
