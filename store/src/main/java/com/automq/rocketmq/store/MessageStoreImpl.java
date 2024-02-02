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

package com.automq.rocketmq.store;

import com.automq.rocketmq.common.config.StoreConfig;
import com.automq.rocketmq.common.model.generated.FlatMessage;
import com.automq.rocketmq.metadata.api.StoreMetadataService;
import com.automq.rocketmq.store.api.LogicQueue;
import com.automq.rocketmq.store.api.LogicQueueManager;
import com.automq.rocketmq.store.api.MessageArrivalListener;
import com.automq.rocketmq.store.api.MessageStore;
import com.automq.rocketmq.store.api.S3ObjectOperator;
import com.automq.rocketmq.store.api.StreamStore;
import com.automq.rocketmq.store.exception.StoreException;
import com.automq.rocketmq.store.model.StoreContext;
import com.automq.rocketmq.store.model.generated.ReceiptHandle;
import com.automq.rocketmq.store.model.generated.TimerHandlerType;
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
import com.automq.rocketmq.store.service.InflightService;
import com.automq.rocketmq.store.service.MessageArrivalNotificationService;
import com.automq.rocketmq.store.service.ReviveService;
import com.automq.rocketmq.store.service.SnapshotService;
import com.automq.rocketmq.store.service.TimerService;
import com.automq.rocketmq.store.service.TransactionService;
import com.automq.rocketmq.store.service.api.KVService;
import com.automq.rocketmq.store.util.SerializeUtil;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.instrumentation.annotations.SpanAttribute;
import io.opentelemetry.instrumentation.annotations.WithSpan;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import static com.automq.rocketmq.store.util.SerializeUtil.decodeReceiptHandle;

public class MessageStoreImpl implements MessageStore {
    public static final String KV_NAMESPACE_CHECK_POINT = "check_point";
    public static final String KV_NAMESPACE_FIFO_INDEX = "fifo_index";

    private final AtomicBoolean started = new AtomicBoolean(false);

    StoreConfig config;

    private final StreamStore streamStore;
    private final StoreMetadataService metadataService;
    private final KVService kvService;
    private final TimerService timerService;
    private final ReviveService reviveService;
    private final InflightService inflightService;
    private final SnapshotService snapshotService;
    private final LogicQueueManager logicQueueManager;
    private final S3ObjectOperator s3ObjectOperator;
    private final MessageArrivalNotificationService messageArrivalNotificationService;
    private final TransactionService transactionService;

    public MessageStoreImpl(StoreConfig config, StreamStore streamStore,
        StoreMetadataService metadataService, KVService kvService, TimerService timerService,
        InflightService inflightService, SnapshotService snapshotService, LogicQueueManager logicQueueManager,
        ReviveService reviveService, S3ObjectOperator s3ObjectOperator,
        MessageArrivalNotificationService messageArrivalNotificationService, TransactionService transactionService) {
        this.config = config;
        this.streamStore = streamStore;
        this.metadataService = metadataService;
        this.kvService = kvService;
        this.timerService = timerService;
        this.inflightService = inflightService;
        this.snapshotService = snapshotService;
        this.logicQueueManager = logicQueueManager;
        this.reviveService = reviveService;
        this.s3ObjectOperator = s3ObjectOperator;
        this.messageArrivalNotificationService = messageArrivalNotificationService;
        this.transactionService = transactionService;
    }

    public LogicQueueManager topicQueueManager() {
        return logicQueueManager;
    }

    public StreamStore streamStore() {
        return streamStore;
    }

    /**
     * @return {@link S3ObjectOperator} instance
     */
    public S3ObjectOperator s3ObjectOperator() {
        return s3ObjectOperator;
    }

    @Override
    public void start() throws Exception {
        if (!started.compareAndSet(false, true)) {
            return;
        }
        clearStateMachineData();
        streamStore.start();
        timerService.start();
        snapshotService.start();
        logicQueueManager.start();
    }

    @Override
    public void shutdown() throws Exception {
        if (!started.compareAndSet(true, false)) {
            return;
        }
        logicQueueManager.shutdown();
        snapshotService.shutdown();
        timerService.shutdown();
        streamStore.shutdown();
        clearStateMachineData();
    }

    private void clearStateMachineData() throws StoreException {
        // clear all statemachine related data in rocksdb
        kvService.clear(KV_NAMESPACE_CHECK_POINT);
        kvService.clear(KV_NAMESPACE_FIFO_INDEX);
        timerService.clear();
    }

    @Override
    @WithSpan(kind = SpanKind.SERVER)
    public CompletableFuture<PopResult> pop(StoreContext context, @SpanAttribute long consumerGroupId,
        @SpanAttribute long topicId, @SpanAttribute int queueId, Filter filter,
        int batchSize, @SpanAttribute boolean fifo, @SpanAttribute boolean retry,
        @SpanAttribute long invisibleDuration) {
        if (fifo && retry) {
            return CompletableFuture.failedFuture(new RuntimeException("Fifo and retry cannot be true at the same time"));
        }
        return logicQueueManager.getOrCreate(context, topicId, queueId)
            .thenCompose(topicQueue -> {
                if (fifo) {
                    return topicQueue.popFifo(context, consumerGroupId, filter, batchSize, invisibleDuration);
                }
                if (retry) {
                    return topicQueue.popRetry(context, consumerGroupId, filter, batchSize, invisibleDuration);
                }
                return topicQueue.popNormal(context, consumerGroupId, filter, batchSize, invisibleDuration);
            }).thenApply(result -> {
                context.span().ifPresent(span -> {
                    span.setAttribute("result.status", result.status().name());
                    span.setAttribute("result.messageCount", result.messageList().size());
                    span.setAttribute("result.restMessageCount", result.restMessageCount());
                });
                return result;
            });
    }

    @Override
    @WithSpan(kind = SpanKind.SERVER)
    public CompletableFuture<PullResult> pull(StoreContext context, long consumerGroupId, long topicId, int queueId,
        Filter filter,
        long offset, int batchSize, boolean retry) {
        return logicQueueManager.getOrCreate(context, topicId, queueId)
            .thenCompose(topicQueue -> {
                if (retry) {
                    return topicQueue.pullRetry(context, consumerGroupId, filter, offset, batchSize);
                }
                return topicQueue.pullNormal(context, consumerGroupId, filter, offset, batchSize);
            });
    }

    @Override
    public CompletableFuture<PutResult> put(StoreContext context, FlatMessage message) {
        // Deal with delay message
        long deliveryTimestamp = message.systemProperties().deliveryTimestamp();
        if (deliveryTimestamp > 0 && deliveryTimestamp - System.currentTimeMillis() > 1000) {
            try {
                String messageId = message.systemProperties().messageId();
                timerService.enqueue(deliveryTimestamp, messageId.getBytes(StandardCharsets.UTF_8), TimerHandlerType.TIMER_MESSAGE, SerializeUtil.flatBufferToByteArray(message));
                return CompletableFuture.completedFuture(new PutResult(PutResult.Status.PUT_DELAYED, -1));
            } catch (Exception e) {
                return CompletableFuture.failedFuture(e);
            }
        }

        // Deal with transaction message
        if (message.systemProperties().preparedTransactionMark()) {
            try {
                String transactionId = transactionService.begin(message);
                return CompletableFuture.completedFuture(new PutResult(PutResult.Status.PUT_TRANSACTION_PREPARED, -1, transactionId));
            } catch (StoreException e) {
                return CompletableFuture.failedFuture(e);
            }
        }

        // Deal with normal message
        return logicQueueManager.getOrCreate(context, message.topicId(), message.queueId())
            .thenCompose(topicQueue -> topicQueue.put(context, message))
            .thenCompose(result ->
                metadataService.topicOf(message.topicId())
                    .thenAccept(topic -> {
                        MessageArrivalListener.MessageSource source;
                        if (deliveryTimestamp > 0) {
                            source = MessageArrivalListener.MessageSource.DELAY_MESSAGE_DEQUEUE;
                        } else {
                            source = MessageArrivalListener.MessageSource.MESSAGE_PUT;
                        }
                        messageArrivalNotificationService.notify(source, topic, message.queueId(), result.offset(), message.tag());
                    })
                    .thenApply(v -> result)
            );
    }

    @Override
    public CompletableFuture<AckResult> ack(String receiptHandle) {
        // Write ack operation to operation log.
        // Operation id should be monotonically increasing for each queue
        ReceiptHandle handle = decodeReceiptHandle(receiptHandle);
        return logicQueueManager.getOrCreate(StoreContext.EMPTY, handle.topicId(), handle.queueId())
            .thenCompose(topicQueue -> topicQueue.ack(receiptHandle));
    }

    @Override
    public CompletableFuture<ChangeInvisibleDurationResult> changeInvisibleDuration(String receiptHandle,
        long invisibleDuration) {
        // Write change invisible duration operation to operation log.
        // Operation id should be monotonically increasing for each queue
        ReceiptHandle handle = decodeReceiptHandle(receiptHandle);
        return logicQueueManager.getOrCreate(StoreContext.EMPTY, handle.topicId(), handle.queueId())
            .thenCompose(topicQueue -> topicQueue.changeInvisibleDuration(receiptHandle, invisibleDuration));
    }

    @Override
    public CompletableFuture<Void> closeQueue(long topicId, int queueId) {
        return logicQueueManager.close(topicId, queueId);
    }

    @Override
    public CompletableFuture<Integer> getInflightStats(long consumerGroupId, long topicId, int queueId) {
        // Get check point count of specified consumer, topic and queue.
        return logicQueueManager.getOrCreate(StoreContext.EMPTY, topicId, queueId)
            .thenApply(topicQueue -> topicQueue.getInflightStats(consumerGroupId));
    }

    @Override
    public List<LogicQueue.StreamOffsetRange> getOffsetRange(long topicId, int queueId, long consumerGroupId) {
        CompletableFuture<Optional<LogicQueue>> future = logicQueueManager.get(topicId, queueId);
        if (future.isDone()) {
            return future.join()
                .map(topicQueue -> topicQueue.getOffsetRange(consumerGroupId))
                .orElse(List.of());
        }
        return List.of();
    }

    @Override
    public long getConsumeOffset(long consumerGroupId, long topicId, int queueId) {
        CompletableFuture<Optional<LogicQueue>> future = logicQueueManager.get(topicId, queueId);
        if (future.isDone()) {
            return future.join()
                .map(topicQueue -> topicQueue.getConsumeOffset(consumerGroupId))
                .orElse(0L);
        }
        return 0;
    }

    @Override
    public long getRetryConsumeOffset(long consumerGroupId, long topicId, int queueId) {
        CompletableFuture<Optional<LogicQueue>> future = logicQueueManager.get(topicId, queueId);
        if (future.isDone()) {
            return future.join()
                .map(topicQueue -> topicQueue.getRetryConsumeOffset(consumerGroupId))
                .orElse(0L);
        }
        return 0;
    }

    @Override
    public CompletableFuture<ResetConsumeOffsetResult> resetConsumeOffset(long consumerGroupId, long topicId,
        int queueId, long offset) {
        return logicQueueManager.getOrCreate(StoreContext.EMPTY, topicId, queueId)
            .thenCompose(topicQueue -> topicQueue.resetConsumeOffset(consumerGroupId, offset));
    }

    @Override
    public CompletableFuture<ClearRetryMessagesResult> clearRetryMessages(long consumerGroupId, long topicId,
        int queueId) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public CompletableFuture<Boolean> cancelDelayMessage(String messageId) {
        try {
            boolean result = timerService.cancel(messageId.getBytes(StandardCharsets.UTF_8));
            return CompletableFuture.completedFuture(result);
        } catch (Exception e) {
            return CompletableFuture.failedFuture(e);
        }
    }

    @Override
    public CompletableFuture<Optional<FlatMessage>> endTransaction(String transactionId,
        TransactionResolution resolution) {
        try {
            if (resolution == TransactionResolution.COMMIT) {
                Optional<FlatMessage> optional = transactionService.commit(transactionId);
                if (optional.isEmpty()) {
                    return CompletableFuture.completedFuture(Optional.empty());
                }
                optional.get().systemProperties().mutatePreparedTransactionMark(false);
                return CompletableFuture.completedFuture(optional);
            } else {
                transactionService.rollback(transactionId);
            }
            return CompletableFuture.completedFuture(Optional.empty());
        } catch (Exception e) {
            return CompletableFuture.failedFuture(e);
        }
    }

    @Override
    public void scheduleCheckTransaction(FlatMessage message) throws StoreException {
        transactionService.scheduleNextCheck(message);
    }

    @Override
    public void registerMessageArriveListener(MessageArrivalListener listener) {
        messageArrivalNotificationService.registerMessageArriveListener(listener);
    }

    @Override
    public void registerTimerMessageHandler(Consumer<TimerTag> handler) throws StoreException {
        timerService.registerHandler(TimerHandlerType.TIMER_MESSAGE, handler);
    }

    @Override
    public void registerTransactionCheckHandler(Consumer<TimerTag> handler) throws StoreException {
        timerService.registerHandler(TimerHandlerType.TRANSACTION_MESSAGE, handler);
    }
}
