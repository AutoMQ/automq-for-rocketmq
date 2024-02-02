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

package com.automq.rocketmq.store.api;

import apache.rocketmq.controller.v1.StreamRole;
import com.automq.rocketmq.common.model.generated.FlatMessage;
import com.automq.rocketmq.store.model.StoreContext;
import com.automq.rocketmq.store.model.message.AckResult;
import com.automq.rocketmq.store.model.message.ChangeInvisibleDurationResult;
import com.automq.rocketmq.store.model.message.Filter;
import com.automq.rocketmq.store.model.message.PopResult;
import com.automq.rocketmq.store.model.message.PullResult;
import com.automq.rocketmq.store.model.message.PutResult;
import com.automq.rocketmq.store.model.message.ResetConsumeOffsetResult;
import java.util.List;
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

    public abstract List<StreamOffsetRange> getOffsetRange(long consumerGroupId);

    public abstract int getInflightStats(long consumerGroupId);

    public abstract CompletableFuture<PullResult> pullNormal(StoreContext context, long consumerGroupId, Filter filter,
        long startOffset,
        int batchSize);

    public abstract CompletableFuture<PullResult> pullRetry(StoreContext context, long consumerGroupId, Filter filter,
        long startOffset,
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

    public record StreamOffsetRange(long streamId, StreamRole streamRole, long startOffset, long endOffset) {
    }
}
