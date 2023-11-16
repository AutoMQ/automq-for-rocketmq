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

package com.automq.rocketmq.store.queue;

import com.automq.rocketmq.common.config.StoreConfig;
import com.automq.rocketmq.common.model.FlatMessageExt;
import com.automq.rocketmq.common.model.generated.FlatMessage;
import com.automq.rocketmq.common.trace.TraceHelper;
import com.automq.rocketmq.metadata.api.StoreMetadataService;
import com.automq.rocketmq.store.api.LogicQueue;
import com.automq.rocketmq.store.api.MessageStateMachine;
import com.automq.rocketmq.store.api.StreamStore;
import com.automq.rocketmq.store.exception.StoreErrorCode;
import com.automq.rocketmq.store.exception.StoreException;
import com.automq.rocketmq.store.model.StoreContext;
import com.automq.rocketmq.store.model.generated.ReceiptHandle;
import com.automq.rocketmq.store.model.message.AckResult;
import com.automq.rocketmq.store.model.message.ChangeInvisibleDurationResult;
import com.automq.rocketmq.store.model.message.Filter;
import com.automq.rocketmq.store.model.message.PopResult;
import com.automq.rocketmq.store.model.message.PullResult;
import com.automq.rocketmq.store.model.message.PutResult;
import com.automq.rocketmq.store.model.message.ResetConsumeOffsetResult;
import com.automq.rocketmq.store.model.operation.AckOperation;
import com.automq.rocketmq.store.model.operation.ChangeInvisibleDurationOperation;
import com.automq.rocketmq.store.model.operation.PopOperation;
import com.automq.rocketmq.store.model.operation.ResetConsumeOffsetOperation;
import com.automq.rocketmq.store.model.stream.SingleRecord;
import com.automq.rocketmq.store.service.InflightService;
import com.automq.rocketmq.store.service.StreamReclaimService;
import com.automq.rocketmq.store.service.api.OperationLogService;
import com.automq.rocketmq.store.util.SerializeUtil;
import com.automq.stream.utils.FutureUtil;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.instrumentation.annotations.SpanAttribute;
import io.opentelemetry.instrumentation.annotations.WithSpan;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

import static com.automq.rocketmq.store.util.SerializeUtil.decodeReceiptHandle;

public class StreamLogicQueue extends LogicQueue {
    protected static final Logger LOGGER = LoggerFactory.getLogger(StreamLogicQueue.class);

    private final StoreMetadataService metadataService;
    private final MessageStateMachine stateMachine;
    private long dataStreamId;
    private long operationStreamId;
    private long snapshotStreamId;
    private final ConcurrentMap<Long/*consumerGroupId*/, CompletableFuture<Long>/*retryStreamId*/> retryStreamIdMap;
    private final StreamStore streamStore;
    private final StoreConfig config;
    private final OperationLogService operationLogService;
    private final InflightService inflightService;
    private final StreamReclaimService streamReclaimService;
    private final AtomicReference<State> state;

    public StreamLogicQueue(StoreConfig config, long topicId, int queueId,
        StoreMetadataService metadataService, MessageStateMachine stateMachine, StreamStore streamStore,
        OperationLogService operationLogService, InflightService inflightService,
        StreamReclaimService streamReclaimService) {
        super(topicId, queueId);
        this.config = config;
        this.metadataService = metadataService;
        this.stateMachine = stateMachine;
        this.streamStore = streamStore;
        this.retryStreamIdMap = new ConcurrentHashMap<>();
        this.operationLogService = operationLogService;
        this.inflightService = inflightService;
        this.streamReclaimService = streamReclaimService;
        this.state = new AtomicReference<>(State.INIT);
    }

    public long dataStreamId() {
        return dataStreamId;
    }

    public ConcurrentMap<Long, CompletableFuture<Long>> retryStreamIdMap() {
        return retryStreamIdMap;
    }

    @Override
    public CompletableFuture<Void> open() {
        if (state.compareAndSet(State.INIT, State.OPENING)) {
            // open all streams and load snapshot
            CompletableFuture<Void> openDataStreamFuture = metadataService.dataStreamOf(topicId, queueId)
                .thenCompose(metadata -> {
                    this.dataStreamId = metadata.getStreamId();
                    return streamStore.open(metadata.getStreamId(), metadata.getEpoch());
                });
            CompletableFuture<Void> openOperationStreamFuture = metadataService.operationStreamOf(topicId, queueId)
                .thenCompose(metadata -> {
                    this.operationStreamId = metadata.getStreamId();
                    return streamStore.open(metadata.getStreamId(), metadata.getEpoch());
                });
            CompletableFuture<Void> openSnapshotStreamFuture = metadataService.snapshotStreamOf(topicId, queueId)
                .thenCompose(metadata -> {
                    this.snapshotStreamId = metadata.getStreamId();
                    return streamStore.open(metadata.getStreamId(), metadata.getEpoch());
                });

            return CompletableFuture.allOf(openDataStreamFuture, openOperationStreamFuture, openSnapshotStreamFuture)
                .thenAccept(nil -> {
                    try {
                        stateMachine.clear();
                        inflightService.clearInflightCount(topicId, queueId);
                    } catch (StoreException e) {
                        LOGGER.error("Failed to clear state machine", e);
                        throw new CompletionException(e);
                    }
                })
                // recover from operation log
                .thenCompose(nil -> operationLogService.recover(stateMachine, operationStreamId, snapshotStreamId))
                .thenAccept(nil -> {
                    // register retry ack advance listener
                    this.stateMachine.registerRetryAckOffsetListener(this::onRetryAckOffsetAdvance);
                    state.set(State.OPENED);
                })
                .thenAccept(nil -> state.set(State.OPENED));
        }
        return CompletableFuture.completedFuture(null);
    }

    private void onRetryAckOffsetAdvance(long consumerGroupId, long ackOffset) {
        // TODO: add reclaim policy
        CompletableFuture<Long> retryStreamIdCf = retryStreamIdMap.get(consumerGroupId);
        if (retryStreamIdCf == null) {
            LOGGER.warn("Retry stream id not found for consumer group: {}", consumerGroupId);
            return;
        }
        CompletableFuture<StreamReclaimService.StreamReclaimResult> taskCf =
            streamReclaimService.addReclaimTask(new StreamReclaimService.StreamReclaimTask(retryStreamIdCf, ackOffset));
        taskCf.thenAccept(result -> {
            if (result.success()) {
                LOGGER.trace("Reclaim consumerGroup: {} 's retry stream to new start offset: {}", consumerGroupId, result.startOffset());
            } else {
                LOGGER.warn("Aborted to reclaim consumerGroup: {} 's retry stream to new start offset: {}", consumerGroupId, ackOffset);
            }
        }).exceptionally(e -> {
            Throwable cause = FutureUtil.cause(e);
            LOGGER.error("Failed to reclaim consumerGroup: {} 's retry stream to new start offset: {}", consumerGroupId, ackOffset, cause);
            return null;
        });
    }

    @Override
    public CompletableFuture<Void> close() {
        if (state.compareAndSet(State.OPENED, State.CLOSING)) {
            List<Long> retryStreamIdList = retryStreamIdMap.values()
                .stream()
                .map(CompletableFuture::join)
                .toList();
            retryStreamIdMap.clear();

            List<Long> streamIdList = new ArrayList<>();
            streamIdList.add(dataStreamId);
            streamIdList.add(operationStreamId);
            streamIdList.add(snapshotStreamId);
            streamIdList.addAll(retryStreamIdList);

            return streamStore.close(streamIdList)
                .thenAccept(nil -> {
                    try {
                        stateMachine.clear();
                        inflightService.clearInflightCount(topicId, queueId);
                    } catch (StoreException e) {
                        LOGGER.error("Failed to clear state machine", e);
                        throw new CompletionException(e);
                    }
                })
                .thenAccept(nil -> state.set(State.CLOSED));
        }
        return CompletableFuture.completedFuture(null);
    }

    @Override
    @WithSpan(kind = SpanKind.SERVER)
    public CompletableFuture<PutResult> put(StoreContext context, FlatMessage flatMessage) {
        if (state.get() != State.OPENED) {
            return CompletableFuture.failedFuture(new StoreException(StoreErrorCode.QUEUE_NOT_OPENED, "Topic queue not opened"));
        }

        String messageId = flatMessage.systemProperties().messageId();
        if (messageId != null) {
            context.span().ifPresent(span -> span.setAttribute("messageId", messageId));
        }

        return streamStore.append(context, dataStreamId, new SingleRecord(flatMessage.getByteBuffer()))
            .thenApply(appendResult -> new PutResult(PutResult.Status.PUT_OK, appendResult.baseOffset()));
    }

    @Override
    @WithSpan
    public CompletableFuture<PutResult> putRetry(StoreContext context, long consumerGroupId, FlatMessage flatMessage) {
        if (state.get() != State.OPENED) {
            return CompletableFuture.failedFuture(new StoreException(StoreErrorCode.QUEUE_NOT_OPENED, "Topic queue not opened"));
        }

        String messageId = flatMessage.systemProperties().messageId();
        if (messageId != null) {
            context.span().ifPresent(span -> span.setAttribute("messageId", messageId));
        }

        CompletableFuture<Long> retryStreamIdCf = retryStreamId(consumerGroupId);
        return retryStreamIdCf.thenCompose(streamId ->
            streamStore.append(context, streamId, new SingleRecord(flatMessage.getByteBuffer()))
                .thenApply(appendResult -> new PutResult(PutResult.Status.PUT_OK, appendResult.baseOffset())));
    }

    private CompletableFuture<Long> retryStreamId(long consumerGroupId) {
        if (!retryStreamIdMap.containsKey(consumerGroupId)) {
            synchronized (this) {
                if (retryStreamIdMap.containsKey(consumerGroupId)) {
                    return retryStreamIdMap.get(consumerGroupId);
                }

                CompletableFuture<Long> future = metadataService.retryStreamOf(consumerGroupId, topicId, queueId)
                    .thenCompose(streamMetadata ->
                        streamStore.open(streamMetadata.getStreamId(), streamMetadata.getEpoch())
                            .thenApply(nil -> streamMetadata.getStreamId()));

                retryStreamIdMap.put(consumerGroupId, future);
                future.exceptionally(ex -> {
                    retryStreamIdMap.remove(consumerGroupId);
                    return null;
                });
                return future;
            }
        }
        return retryStreamIdMap.get(consumerGroupId);
    }

    @Override
    public CompletableFuture<PopResult> popNormal(StoreContext context, long consumerGroup, Filter filter,
        int batchSize, long invisibleDuration) {
        // start from consume offset
        if (state.get() != State.OPENED) {
            return CompletableFuture.failedFuture(new StoreException(StoreErrorCode.QUEUE_NOT_OPENED, "Topic queue not opened"));
        }
        long offset = stateMachine.consumeOffset(consumerGroup);
        return pop(context, consumerGroup, dataStreamId, offset, PopOperation.PopOperationType.POP_NORMAL, filter, batchSize, invisibleDuration);
    }

    @WithSpan(kind = SpanKind.SERVER)
    private CompletableFuture<PopResult> pop(StoreContext context, @SpanAttribute long consumerGroupId,
        @SpanAttribute long streamId, @SpanAttribute long startOffset,
        @SpanAttribute PopOperation.PopOperationType operationType, @SpanAttribute Filter filter,
        int batchSize, long invisibleDuration) {
        // Check offset
        long confirmOffset = streamStore.confirmOffset(streamId);
        if (startOffset == confirmOffset) {
            return CompletableFuture.completedFuture(new PopResult(PopResult.Status.END_OF_QUEUE, 0, Collections.emptyList(), 0));
        }
        if (startOffset > confirmOffset) {
            return CompletableFuture.completedFuture(new PopResult(PopResult.Status.ILLEGAL_OFFSET, 0, Collections.emptyList(), 0));
        }

        int fetchBatchSize;
        if (filter.needApply()) {
            // If filter is applied, fetch more messages to apply filter.
            fetchBatchSize = batchSize * config.fetchBatchSizeFactor();
        } else {
            // If filter is not applied, fetch batchSize messages.
            fetchBatchSize = batchSize;
        }
        FilterFetchResult fetchResult = new FilterFetchResult(startOffset);
        long operationTimestamp = System.currentTimeMillis();
        // fetch messages
        CompletableFuture<FilterFetchResult> fetchCf = fetchAndFilterMessages(context, streamId, startOffset, batchSize,
            fetchBatchSize, filter, fetchResult, 0, 0, operationTimestamp);
        // log op
        AtomicReference<Span> spanRef = new AtomicReference<>();
        CompletableFuture<FilterFetchResult> fetchAndLogOpCf = fetchCf.thenCompose(filterFetchResult -> {
            Optional<Span> spanOptional = TraceHelper.createAndStartSpan(context, "logPopOperation", SpanKind.SERVER);
            spanOptional.ifPresent(spanRef::set);

            List<FlatMessageExt> messageExtList = filterFetchResult.messageList;
            // write pop operation to operation log
            long preOffset = filterFetchResult.startOffset - 1;
            List<CompletableFuture<OperationLogService.LogResult>> appendOpCfs = new ArrayList<>(messageExtList.size());
            // write pop operation for each need consumed message
            for (FlatMessageExt messageExt : messageExtList) {
                int count = (int) (messageExt.offset() - preOffset);
                preOffset = messageExt.offset();
                PopOperation popOperation = new PopOperation(topicId, queueId, operationStreamId, snapshotStreamId,
                    stateMachine, consumerGroupId, messageExt.offset(), count, invisibleDuration, operationTimestamp,
                    false, operationType);
                appendOpCfs.add(operationLogService.logPopOperation(popOperation)
                    .thenApply(logResult -> {
                        long operationId = logResult.getOperationOffset();
                        messageExt.setReceiptHandle(SerializeUtil.encodeReceiptHandle(consumerGroupId, topicId, queueId, operationId));
                        if (!messageExt.isRetryMessage()) {
                            messageExt.setDeliveryAttempts(logResult.getPopTimes());
                        }
                        return logResult;
                    }));
            }
            // write special pop operation for the last message to update consume offset
            int count = (int) (fetchResult.endOffset - 1 - preOffset);
            if (count > 0) {
                PopOperation popOperation = new PopOperation(topicId, queueId, operationStreamId, snapshotStreamId,
                    stateMachine, consumerGroupId, fetchResult.endOffset - 1, count, invisibleDuration,
                    operationTimestamp, true, operationType);
                appendOpCfs.add(operationLogService.logPopOperation(popOperation));
            }
            return CompletableFuture.allOf(appendOpCfs.toArray(new CompletableFuture[0]))
                .whenComplete((nil, throwable) -> {
                    Span span = spanRef.get();
                    if (span != null) {
                        span.setAttribute("operationCount", appendOpCfs.size());
                        TraceHelper.endSpan(context, span, throwable);
                    }
                })
                .thenApply(nil -> fetchResult);
        });

        return fetchAndLogOpCf.thenApply(filterFetchResult -> {
            List<FlatMessageExt> messageExtList = filterFetchResult.messageList;
            PopResult.Status status;
            if (messageExtList.isEmpty()) {
                status = PopResult.Status.NOT_FOUND;
            } else {
                status = PopResult.Status.FOUND;
            }
            inflightService.increaseInflightCount(consumerGroupId, topicId, queueId, messageExtList.size());
            return new PopResult(status, operationTimestamp, messageExtList, confirmOffset - filterFetchResult.endOffset);
        }).exceptionally(throwable -> new PopResult(PopResult.Status.ERROR, operationTimestamp, Collections.emptyList(), confirmOffset - startOffset));
    }

    @Override
    public CompletableFuture<PopResult> popFifo(StoreContext context, long consumerGroup, Filter filter, int batchSize,
        long invisibleDuration) {
        if (state.get() != State.OPENED) {
            return CompletableFuture.failedFuture(new StoreException(StoreErrorCode.QUEUE_NOT_OPENED, "Topic queue not opened"));
        }
        // start from ack offset
        try {
            long offset = stateMachine.ackOffset(consumerGroup);
            boolean isLocked = stateMachine.isLocked(consumerGroup, offset);
            if (isLocked) {
                return CompletableFuture.completedFuture(new PopResult(PopResult.Status.LOCKED, 0, Collections.emptyList(), 0));
            } else {
                return pop(context, consumerGroup, dataStreamId, offset, PopOperation.PopOperationType.POP_ORDER, filter, batchSize, invisibleDuration);
            }
        } catch (StoreException e) {
            return CompletableFuture.completedFuture(new PopResult(PopResult.Status.ERROR, 0, Collections.emptyList(), 0));
        }
    }

    @Override
    public CompletableFuture<PopResult> popRetry(StoreContext context, long consumerGroupId, Filter filter,
        int batchSize,
        long invisibleDuration) {
        if (state.get() != State.OPENED) {
            return CompletableFuture.failedFuture(new StoreException(StoreErrorCode.QUEUE_NOT_OPENED, "Topic queue not opened"));
        }
        long offset = stateMachine.retryConsumeOffset(consumerGroupId);
        CompletableFuture<Long> retryStreamIdFuture = retryStreamId(consumerGroupId);
        return retryStreamIdFuture.thenCompose(retryStreamId -> pop(context, consumerGroupId, retryStreamId, offset, PopOperation.PopOperationType.POP_RETRY, filter, batchSize, invisibleDuration));
    }

    @WithSpan(kind = SpanKind.SERVER)
    private CompletableFuture<List<FlatMessageExt>> fetchMessages(StoreContext context, @SpanAttribute long streamId,
        @SpanAttribute long offset, @SpanAttribute int batchSize) {
        long startOffset = streamStore.startOffset(streamId);
        if (offset < startOffset) {
            offset = startOffset;
        }

        long confirmOffset = streamStore.confirmOffset(streamId);
        if (offset >= confirmOffset) {
            return CompletableFuture.completedFuture(Collections.emptyList());
        }

        if (offset + batchSize > confirmOffset) {
            batchSize = (int) (confirmOffset - offset);
        }

        return streamStore.fetch(context, streamId, offset, batchSize)
            .thenApply(fetchResult -> {
                AtomicLong fetchBytes = new AtomicLong();

                // TODO: Assume message count is always 1 in each batch for now.
                List<FlatMessageExt> resultList = fetchResult.recordBatchList()
                    .stream()
                    .map(batch -> {
                        fetchBytes.addAndGet(batch.rawPayload().remaining());
                        FlatMessage message = FlatMessage.getRootAsFlatMessage(batch.rawPayload());
                        long messageOffset = batch.baseOffset();
                        return FlatMessageExt.Builder.builder()
                            .message(message)
                            .offset(messageOffset)
                            .build();
                    })
                    .toList();

                context.span().ifPresent(span -> span.setAttribute("fetchBytes", fetchBytes.get()));

                return resultList;
            });
    }

    // Fetch and filter messages until exceeding the limit.
    @WithSpan(kind = SpanKind.SERVER)
    private CompletableFuture<FilterFetchResult> fetchAndFilterMessages(StoreContext context,
        @SpanAttribute long streamId, @SpanAttribute long offset, @SpanAttribute int batchSize,
        @SpanAttribute int fetchBatchSize, @SpanAttribute Filter filter, FilterFetchResult result,
        int fetchCount, long fetchBytes, long operationTimestamp) {
        // Fetch more messages.
        return fetchMessages(context, streamId, offset, fetchBatchSize)
            .thenCompose(fetchResult -> {
                // Add filter result to message list.
                List<FlatMessageExt> matchedMessageList = filter.doFilter(fetchResult);
                // Update end offset
                int index = batchSize - result.size();
                if (matchedMessageList.size() > index) {
                    FlatMessageExt messageExt = matchedMessageList.get(index);
                    result.setEndOffset(messageExt.offset());
                    result.addMessageList(matchedMessageList.subList(0, index));
                } else {
                    result.setEndOffset(offset + fetchResult.size());
                    result.addMessageList(matchedMessageList);
                }
                // If not enough messages after applying filter, fetch more messages.
                boolean needToFetch = result.size() < batchSize;
                boolean hasMoreMessages = fetchResult.size() >= fetchBatchSize;

                int newFetchCount = fetchCount + fetchResult.size();
                long newFetchBytes = fetchBytes + fetchResult.stream()
                    .map(messageExt -> (long) messageExt.message().getByteBuffer().limit())
                    .reduce(0L, Long::sum);
                boolean notExceedLimit = newFetchCount < config.maxFetchCount() &&
                    newFetchBytes < config.maxFetchBytes() &&
                    System.currentTimeMillis() - operationTimestamp < config.maxFetchTimeMillis();

                context.span().ifPresent(span -> {
                    span.setAttribute("needToFetch", needToFetch);
                    span.setAttribute("hasMoreMessages", hasMoreMessages);
                    span.setAttribute("messageCount", result.size());
                });

                if (needToFetch && hasMoreMessages && notExceedLimit) {
                    return fetchAndFilterMessages(context, streamId, offset + fetchResult.size(),
                        batchSize, fetchBatchSize, filter, result, newFetchCount, newFetchBytes, operationTimestamp);
                } else {
                    return CompletableFuture.completedFuture(result);
                }
            });
    }

    /**
     * Fetch and filter result.
     * <p>
     * All matched messages in <code>[startOffset, endOffset)</code>
     */
    static class FilterFetchResult {
        private final long startOffset;
        private long endOffset;
        private final List<FlatMessageExt> messageList = new ArrayList<>();

        public FilterFetchResult(long startOffset) {
            this.startOffset = startOffset;
            this.endOffset = startOffset;
        }

        public void setEndOffset(long endOffset) {
            this.endOffset = endOffset;
        }

        public void addMessageList(List<FlatMessageExt> messageList) {
            this.messageList.addAll(messageList);
        }

        public int size() {
            return messageList.size();
        }
    }

    @Override
    public CompletableFuture<AckResult> ack(String receiptHandle) {
        if (state.get() != State.OPENED) {
            return CompletableFuture.failedFuture(new StoreException(StoreErrorCode.QUEUE_NOT_OPENED, "Topic queue not opened"));
        }
        ReceiptHandle handle = decodeReceiptHandle(receiptHandle);
        AckOperation operation = new AckOperation(handle.topicId(), handle.queueId(), operationStreamId,
            snapshotStreamId, stateMachine, handle.consumerGroupId(), handle.operationId(), System.currentTimeMillis(),
            AckOperation.AckOperationType.ACK_NORMAL);
        return operationLogService.logAckOperation(operation)
            .thenApply(nil -> {
                inflightService.decreaseInflightCount(handle.consumerGroupId(), handle.topicId(), handle.queueId(), 1);
                return new AckResult(AckResult.Status.SUCCESS);
            }).exceptionally(throwable -> new AckResult(AckResult.Status.ERROR));
    }

    @Override
    public CompletableFuture<AckResult> ackTimeout(String receiptHandle) {
        if (state.get() != State.OPENED) {
            return CompletableFuture.failedFuture(new StoreException(StoreErrorCode.QUEUE_NOT_OPENED, "Topic queue not opened"));
        }
        ReceiptHandle handle = decodeReceiptHandle(receiptHandle);
        AckOperation operation = new AckOperation(handle.topicId(), handle.queueId(), operationStreamId,
            snapshotStreamId, stateMachine, handle.consumerGroupId(), handle.operationId(), System.currentTimeMillis(),
            AckOperation.AckOperationType.ACK_TIMEOUT);
        return operationLogService.logAckOperation(operation)
            .thenApply(nil -> {
                inflightService.decreaseInflightCount(handle.consumerGroupId(), handle.topicId(), handle.queueId(), 1);
                return new AckResult(AckResult.Status.SUCCESS);
            }).exceptionally(throwable -> new AckResult(AckResult.Status.ERROR));
    }

    @Override
    public CompletableFuture<ChangeInvisibleDurationResult> changeInvisibleDuration(String receiptHandle,
        long invisibleDuration) {
        if (state.get() != State.OPENED) {
            return CompletableFuture.failedFuture(new StoreException(StoreErrorCode.QUEUE_NOT_OPENED, "Topic queue not opened"));
        }
        ReceiptHandle handle = decodeReceiptHandle(receiptHandle);
        ChangeInvisibleDurationOperation operation = new ChangeInvisibleDurationOperation(handle.topicId(),
            handle.queueId(), operationStreamId, snapshotStreamId, stateMachine, handle.consumerGroupId(),
            handle.operationId(), invisibleDuration, System.currentTimeMillis());
        return operationLogService.logChangeInvisibleDurationOperation(operation)
            .thenApply(nil -> new ChangeInvisibleDurationResult(ChangeInvisibleDurationResult.Status.SUCCESS))
            .exceptionally(throwable -> new ChangeInvisibleDurationResult(ChangeInvisibleDurationResult.Status.ERROR));
    }

    @Override
    public CompletableFuture<ResetConsumeOffsetResult> resetConsumeOffset(long consumerGroupId, long offset) {
        if (state.get() != State.OPENED) {
            return CompletableFuture.failedFuture(new StoreException(StoreErrorCode.QUEUE_NOT_OPENED, "Topic queue not opened"));
        }
        ResetConsumeOffsetOperation operation = new ResetConsumeOffsetOperation(topicId, queueId, operationStreamId,
            snapshotStreamId, stateMachine, consumerGroupId, offset, System.currentTimeMillis());
        return operationLogService.logResetConsumeOffsetOperation(operation)
            .thenApply(logResult -> new ResetConsumeOffsetResult(ResetConsumeOffsetResult.Status.SUCCESS))
            .exceptionally(throwable -> new ResetConsumeOffsetResult(ResetConsumeOffsetResult.Status.ERROR));
    }

    @Override
    public CompletableFuture<QueueOffsetRange> getOffsetRange() {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getInflightStats(long consumerGroupId) {
        return inflightService.getInflightCount(consumerGroupId, topicId, queueId);
    }

    @Override
    public CompletableFuture<PullResult> pullNormal(long consumerGroupId, Filter filter, long startOffset,
        int batchSize) {
        if (state.get() != State.OPENED) {
            return CompletableFuture.failedFuture(new StoreException(StoreErrorCode.QUEUE_NOT_OPENED, "Topic queue not opened"));
        }
        return pull(dataStreamId, consumerGroupId, filter, startOffset, batchSize);
    }

    private CompletableFuture<PullResult> pull(long streamId, long consumerGroupId, Filter filter, long startOffset,
        int batchSize) {
        int fetchBatchSize;
        if (filter.needApply()) {
            // If filter is applied, fetch more messages to apply filter.
            fetchBatchSize = batchSize * config.fetchBatchSizeFactor();
        } else {
            // If filter is not applied, fetch batchSize messages.
            fetchBatchSize = batchSize;
        }
        FilterFetchResult fetchResult = new FilterFetchResult(startOffset);
        long operationTimestamp = System.currentTimeMillis();
        CompletableFuture<FilterFetchResult> fetchCf = fetchAndFilterMessages(StoreContext.EMPTY, streamId, startOffset, batchSize,
            fetchBatchSize, filter, fetchResult, 0, 0, operationTimestamp);
        return fetchCf.thenApply(filterFetchResult -> {
            List<FlatMessageExt> messageExtList = filterFetchResult.messageList;
            PullResult.Status status;
            if (messageExtList.isEmpty()) {
                status = PullResult.Status.NO_MATCHED_MSG;
            } else {
                status = PullResult.Status.FOUND;
            }
            return new PullResult(status, filterFetchResult.endOffset, filterFetchResult.startOffset, filterFetchResult.endOffset - 1, messageExtList);
        }).exceptionally(throwable -> new PullResult(PullResult.Status.OFFSET_ILLEGAL, -1, -1, -1, Collections.emptyList()));

    }

    @Override
    public CompletableFuture<PullResult> pullRetry(long consumerGroupId, Filter filter, long startOffset,
        int batchSize) {
        if (state.get() != State.OPENED) {
            return CompletableFuture.failedFuture(new StoreException(StoreErrorCode.QUEUE_NOT_OPENED, "Topic queue not opened"));
        }
        CompletableFuture<Long> retryStreamIdCf = retryStreamId(consumerGroupId);
        return retryStreamIdCf.thenCompose(streamId -> pull(streamId, consumerGroupId, filter, startOffset, batchSize));
    }

    @Override
    public long getConsumeOffset(long consumerGroupId) {
        return stateMachine.consumeOffset(consumerGroupId);
    }

    @Override
    public long getAckOffset(long consumerGroupId) {
        return stateMachine.ackOffset(consumerGroupId);
    }

    @Override
    public long getRetryConsumeOffset(long consumerGroupId) {
        return stateMachine.retryConsumeOffset(consumerGroupId);
    }

    @Override
    public long getRetryAckOffset(long consumerGroupId) {
        return stateMachine.retryAckOffset(consumerGroupId);
    }

    @Override
    public State getState() {
        return state.get();
    }

    @Override
    public int getConsumeTimes(long consumerGroupId, long offset) {
        return stateMachine.consumeTimes(consumerGroupId, offset);
    }
}
