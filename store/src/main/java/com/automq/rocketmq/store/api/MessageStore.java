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

import com.automq.rocketmq.common.model.generated.FlatMessage;
import com.automq.rocketmq.common.util.Lifecycle;
import com.automq.rocketmq.store.exception.StoreException;
import com.automq.rocketmq.store.model.StoreContext;
import com.automq.rocketmq.store.model.generated.TimerTag;
import com.automq.rocketmq.store.model.message.AckResult;
import com.automq.rocketmq.store.model.message.ChangeInvisibleDurationResult;
import com.automq.rocketmq.store.model.message.ClearRetryMessagesResult;
import com.automq.rocketmq.store.model.message.Filter;
import com.automq.rocketmq.store.model.message.PopResult;
import com.automq.rocketmq.store.model.message.PullResult;
import com.automq.rocketmq.store.model.message.PutResult;
import com.automq.rocketmq.store.model.message.ResetConsumeOffsetResult;
import com.automq.rocketmq.store.model.transaction.TransactionResolution;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

public interface MessageStore extends Lifecycle {

    /**
     * Pop message from specified topic and queue.
     *
     * @param context           propagate context to store
     * @param consumerGroupId   consumer group id that launches this query
     * @param topicId           topic id to pop message from
     * @param queueId           queue id to pop message from
     * @param filter            filter to apply to messages
     * @param batchSize         maximum count of messages
     * @param fifo              is orderly pop
     * @param invisibleDuration the duration for the next time this batch of messages will be visible, in milliseconds
     * @return pop result, see {@link PopResult}
     */
    CompletableFuture<PopResult> pop(StoreContext context, long consumerGroupId, long topicId, int queueId,
        Filter filter, int batchSize, boolean fifo, boolean retry, long invisibleDuration);

    /**
     * Pull message from specified topic and queue.
     *
     * @param consumerGroupId consumer group id that launches this query
     * @param topicId         topic id to pull message from
     * @param queueId         queue id to pull message from
     * @param filter          filter to apply to messages
     * @param offset          offset to start pulling
     * @param batchSize       maximum count of messages
     * @param retry           whether to pull retry messages
     * @return pull result, see {@link PullResult}
     */
    CompletableFuture<PullResult> pull(StoreContext context, long consumerGroupId, long topicId, int queueId,
        Filter filter, long offset,
        int batchSize, boolean retry);

    /**
     * Put a message to the specified topic and queue.
     *
     * @param flatMessage flat message to append
     * @return append result with an offset assigned to the message, see {@link PutResult}
     */
    CompletableFuture<PutResult> put(StoreContext context, FlatMessage flatMessage);

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
    CompletableFuture<ChangeInvisibleDurationResult> changeInvisibleDuration(String receiptHandle,
        long invisibleDuration);

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
     *
     * @param topicId         topic id
     * @param queueId         queue id
     * @param consumerGroupId consumer group id
     * @return offset range, <code>[startOffset, endOffset)</code> for streams
     */
    List<LogicQueue.StreamOffsetRange> getOffsetRange(long topicId, int queueId, long consumerGroupId);

    /**
     * Get consume offset of specified consumer group.
     *
     * @param consumerGroupId consumer group id
     * @param topicId         topic id
     * @param queueId         queue id
     * @return consume offset
     */
    long getConsumeOffset(long consumerGroupId, long topicId, int queueId);

    /**
     * Get retry offset of specified consumer group.
     *
     * @param consumerGroupId consumer group id
     * @param topicId         topic id
     * @param queueId         queue id
     * @return ack offset
     */
    long getRetryConsumeOffset(long consumerGroupId, long topicId, int queueId);

    /**
     * Reset consume offset of specified consumer group.
     *
     * @param consumerGroupId consumer group id
     * @param topicId         topic id
     * @param queueId         queue id
     * @param offset          new consume offset
     * @return reset result, see {@link ResetConsumeOffsetResult}
     */
    CompletableFuture<ResetConsumeOffsetResult> resetConsumeOffset(long consumerGroupId, long topicId, int queueId,
        long offset);

    /**
     * Clear all retry messages of specified consumer group, topic and queue.
     *
     * @param consumerGroupId consumer group id
     * @param topicId         topic id
     * @param queueId         queue id
     * @return clear result, see {@link ClearRetryMessagesResult}
     */
    CompletableFuture<ClearRetryMessagesResult> clearRetryMessages(long consumerGroupId, long topicId, int queueId);

    /**
     * Cancel delayed message.
     *
     * @param messageId message id to cancel
     * @return cancel result
     */
    CompletableFuture<Boolean> cancelDelayMessage(String messageId);

    /**
     * End a transaction.
     *
     * @param transactionId transaction id
     * @param resolution    transaction resolution
     * @return end transaction result
     */
    CompletableFuture<Optional<FlatMessage>> endTransaction(String transactionId, TransactionResolution resolution);

    /**
     * Schedule a check for transaction status.
     *
     * @param message transaction message
     */
    void scheduleCheckTransaction(FlatMessage message) throws StoreException;

    /**
     * Register a listener to be notified when a message arrives.
     *
     * @param listener message arrive listener
     */
    void registerMessageArriveListener(MessageArrivalListener listener);

    /**
     * Register a hanler for denqueuing timer messages.
     *
     * @param handler timer message handler
     */
    void registerTimerMessageHandler(Consumer<TimerTag> handler) throws StoreException;

    /**
     * Register a hanler for checking transaction status.
     *
     * @param handler transaction status check handler
     */
    void registerTransactionCheckHandler(Consumer<TimerTag> handler) throws StoreException;
}
