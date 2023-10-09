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

import com.automq.rocketmq.common.config.StoreConfig;
import com.automq.rocketmq.common.model.FlatMessageExt;
import com.automq.rocketmq.common.model.generated.FlatMessage;
import com.automq.rocketmq.common.util.Pair;
import com.automq.rocketmq.metadata.api.StoreMetadataService;
import com.automq.rocketmq.store.api.MessageStateMachine;
import com.automq.rocketmq.store.api.StreamStore;
import com.automq.rocketmq.store.api.TopicQueue;
import com.automq.rocketmq.store.exception.StoreErrorCode;
import com.automq.rocketmq.store.exception.StoreException;
import com.automq.rocketmq.store.model.generated.ReceiptHandle;
import com.automq.rocketmq.store.model.message.AckResult;
import com.automq.rocketmq.store.model.message.ChangeInvisibleDurationResult;
import com.automq.rocketmq.store.model.message.Filter;
import com.automq.rocketmq.store.model.message.PopResult;
import com.automq.rocketmq.store.model.message.PullResult;
import com.automq.rocketmq.store.model.message.PutResult;
import com.automq.rocketmq.store.model.operation.AckOperation;
import com.automq.rocketmq.store.model.operation.ChangeInvisibleDurationOperation;
import com.automq.rocketmq.store.model.operation.PopOperation;
import com.automq.rocketmq.store.model.stream.SingleRecord;
import com.automq.rocketmq.store.service.InflightService;
import com.automq.rocketmq.store.service.api.OperationLogService;
import com.automq.rocketmq.store.util.SerializeUtil;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;

import static com.automq.rocketmq.store.util.SerializeUtil.decodeReceiptHandle;

public class StreamTopicQueue extends TopicQueue {
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
    private final AtomicReference<State> state;

    public StreamTopicQueue(StoreConfig config, long topicId, int queueId,
        StoreMetadataService metadataService, MessageStateMachine stateMachine, StreamStore streamStore,
        OperationLogService operationLogService, InflightService inflightService) {
        super(topicId, queueId);
        this.config = config;
        this.metadataService = metadataService;
        this.stateMachine = stateMachine;
        this.streamStore = streamStore;
        this.retryStreamIdMap = new ConcurrentHashMap<>();
        this.operationLogService = operationLogService;
        this.inflightService = inflightService;
        this.state = new AtomicReference<>(State.INIT);
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

            // TODO: when we open retry-streams?
            return CompletableFuture.allOf(openDataStreamFuture, openOperationStreamFuture, openSnapshotStreamFuture)
                // recover from operation log
                .thenCompose(nil -> operationLogService.recover(stateMachine, operationStreamId, snapshotStreamId))
                .thenAccept(nil -> state.set(State.OPENED));
        }
        return CompletableFuture.completedFuture(null);
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
                .thenCompose(nil -> stateMachine.clear())
                .thenAccept(nil -> inflightService.clearInflightCount(topicId, queueId))
                .thenAccept(nil -> state.set(State.CLOSED));
        }
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<PutResult> put(FlatMessage flatMessage) {
        if (state.get() != State.OPENED) {
            return CompletableFuture.failedFuture(new StoreException(StoreErrorCode.TOPIC_QUEUE_NOT_OPENED, "Topic queue not opened"));
        }
        return streamStore.append(dataStreamId, new SingleRecord(flatMessage.getByteBuffer()))
            .thenApply(appendResult -> new PutResult(PutResult.Status.PUT_OK, appendResult.baseOffset()));
    }

    @Override
    public CompletableFuture<PutResult> putRetry(long consumerGroupId, FlatMessage flatMessage) {
        if (state.get() != State.OPENED) {
            return CompletableFuture.failedFuture(new StoreException(StoreErrorCode.TOPIC_QUEUE_NOT_OPENED, "Topic queue not opened"));
        }
        CompletableFuture<Long> retryStreamIdCf = retryStreamId(consumerGroupId);
        return retryStreamIdCf.thenCompose(streamId ->
            streamStore.append(streamId, new SingleRecord(flatMessage.getByteBuffer()))
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
    public CompletableFuture<PopResult> popNormal(long consumerGroup, Filter filter, int batchSize,
        long invisibleDuration) {
        // start from consume offset
        if (state.get() != State.OPENED) {
            return CompletableFuture.failedFuture(new StoreException(StoreErrorCode.TOPIC_QUEUE_NOT_OPENED, "Topic queue not opened"));
        }
        return stateMachine
            .consumeOffset(consumerGroup)
            .thenCompose(offset -> pop(consumerGroup, dataStreamId, offset,
                PopOperation.PopOperationType.POP_NORMAL, filter, batchSize, invisibleDuration));
    }

    private CompletableFuture<PopResult> pop(long consumerGroupId, long streamId, long startOffset,
        PopOperation.PopOperationType operationType, Filter filter,
        int batchSize, long invisibleDuration) {
        int fetchBatchSize;
        if (filter.needApply()) {
            // If filter is applied, fetch more messages to apply filter.
            fetchBatchSize = batchSize * config.fetchBatchSizeFactor();
        } else {
            // If filter is not applied, fetch batchSize messages.
            fetchBatchSize = batchSize;
        }
        FilterFetchResult fetchResult = new FilterFetchResult(startOffset);
        long operationTimestamp = System.nanoTime();
        // fetch messages
        CompletableFuture<FilterFetchResult> fetchCf = fetchAndFilterMessages(streamId, startOffset, batchSize,
            fetchBatchSize, filter, fetchResult, 0, 0, operationTimestamp);
        // log op
        CompletableFuture<FilterFetchResult> fetchAndLogOpCf = fetchCf.thenCompose(filterFetchResult -> {
            List<FlatMessageExt> messageExtList = filterFetchResult.messageList;
            // write pop operation to operation log
            long preOffset = filterFetchResult.startOffset - 1;
            List<CompletableFuture<Long>> appendOpCfs = new ArrayList<>(messageExtList.size());
            // write pop operation for each need consumed message
            for (FlatMessageExt messageExt : messageExtList) {
                int count = (int) (messageExt.offset() - preOffset);
                preOffset = messageExt.offset();
                PopOperation popOperation = new PopOperation(topicId, queueId, operationStreamId, snapshotStreamId,
                    stateMachine, consumerGroupId, messageExt.offset(), count, invisibleDuration, operationTimestamp,
                    false, operationType);
                appendOpCfs.add(operationLogService.logPopOperation(popOperation)
                    .thenApply(operationId -> {
                        messageExt.setReceiptHandle(SerializeUtil.encodeReceiptHandle(consumerGroupId, topicId, queueId, operationId));
                        return operationId;
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
            return new PopResult(status, operationTimestamp, messageExtList);
        }).exceptionally(throwable -> new PopResult(PopResult.Status.ERROR, operationTimestamp, Collections.emptyList()));
    }

    @Override
    public CompletableFuture<PopResult> popFifo(long consumerGroup, Filter filter, int batchSize,
        long invisibleDuration) {
        if (state.get() != State.OPENED) {
            return CompletableFuture.failedFuture(new StoreException(StoreErrorCode.TOPIC_QUEUE_NOT_OPENED, "Topic queue not opened"));
        }
        // start from ack offset
        return stateMachine.ackOffset(consumerGroup).thenCompose(offset ->
            stateMachine.isLocked(consumerGroup, offset)
                .thenCompose(isLocked -> {
                    if (isLocked) {
                        return CompletableFuture.completedFuture(new PopResult(PopResult.Status.LOCKED, 0, Collections.emptyList()));
                    } else {
                        return pop(consumerGroup, dataStreamId, offset, PopOperation.PopOperationType.POP_ORDER, filter, batchSize, invisibleDuration);
                    }
                }));
    }

    @Override
    public CompletableFuture<PopResult> popRetry(long consumerGroupId, Filter filter, int batchSize,
        long invisibleDuration) {
        if (state.get() != State.OPENED) {
            return CompletableFuture.failedFuture(new StoreException(StoreErrorCode.TOPIC_QUEUE_NOT_OPENED, "Topic queue not opened"));
        }
        CompletableFuture<Long> retryStreamIdCf = retryStreamId(consumerGroupId);
        CompletableFuture<Long> retryOffsetCf = stateMachine.retryConsumeOffset(consumerGroupId);
        return retryStreamIdCf.thenCombine(retryOffsetCf, Pair::new)
            .thenCompose(pair -> {
                Long retryStreamId = pair.left();
                Long startOffset = pair.right();
                return pop(consumerGroupId, retryStreamId, startOffset, PopOperation.PopOperationType.POP_RETRY, filter, batchSize, invisibleDuration);
            });
    }

    private CompletableFuture<List<FlatMessageExt>> fetchMessages(long streamId, long offset, int batchSize) {
        return streamStore.fetch(streamId, offset, batchSize)
            .thenApply(fetchResult -> {
                // TODO: Assume message count is always 1 in each batch for now.
                return fetchResult.recordBatchList()
                    .stream()
                    .map(batch -> {
                        FlatMessage message = FlatMessage.getRootAsFlatMessage(batch.rawPayload());
                        long messageOffset = batch.baseOffset();
                        return FlatMessageExt.Builder.builder()
                            .message(message)
                            .offset(messageOffset)
                            .build();
                    })
                    .toList();
            });
    }

    // Fetch and filter messages until exceeding the limit.
    private CompletableFuture<FilterFetchResult> fetchAndFilterMessages(long streamId,
        long offset, int batchSize, int fetchBatchSize, Filter filter, FilterFetchResult result,
        int fetchCount, long fetchBytes, long operationTimestamp) {
        // Fetch more messages.
        return fetchMessages(streamId, offset, fetchBatchSize)
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
                    System.nanoTime() - operationTimestamp < config.maxFetchTimeNanos();

                if (needToFetch && hasMoreMessages && notExceedLimit) {
                    return fetchAndFilterMessages(streamId, offset + fetchResult.size(), batchSize, fetchBatchSize, filter,
                        result, newFetchCount, newFetchBytes, operationTimestamp);
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
            return CompletableFuture.failedFuture(new StoreException(StoreErrorCode.TOPIC_QUEUE_NOT_OPENED, "Topic queue not opened"));
        }
        ReceiptHandle handle = decodeReceiptHandle(receiptHandle);
        AckOperation operation = new AckOperation(handle.topicId(), handle.queueId(), operationStreamId,
            snapshotStreamId, stateMachine, handle.consumerGroupId(), handle.operationId(), System.nanoTime(),
            AckOperation.AckOperationType.ACK_NORMAL);
        return operationLogService.logAckOperation(operation).thenApply(nil -> {
            inflightService.decreaseInflightCount(handle.consumerGroupId(), handle.topicId(), handle.queueId(), 1);
            return new AckResult(AckResult.Status.SUCCESS);
        }).exceptionally(throwable -> new AckResult(AckResult.Status.ERROR));
    }

    @Override
    public CompletableFuture<AckResult> ackTimeout(String receiptHandle) {
        if (state.get() != State.OPENED) {
            return CompletableFuture.failedFuture(new StoreException(StoreErrorCode.TOPIC_QUEUE_NOT_OPENED, "Topic queue not opened"));
        }
        ReceiptHandle handle = decodeReceiptHandle(receiptHandle);
        AckOperation operation = new AckOperation(handle.topicId(), handle.queueId(), operationStreamId,
            snapshotStreamId, stateMachine, handle.consumerGroupId(), handle.operationId(), System.nanoTime(),
            AckOperation.AckOperationType.ACK_TIMEOUT);
        return operationLogService.logAckOperation(operation).thenApply(nil -> {
            inflightService.decreaseInflightCount(handle.consumerGroupId(), handle.topicId(), handle.queueId(), 1);
            return new AckResult(AckResult.Status.SUCCESS);
        }).exceptionally(throwable -> new AckResult(AckResult.Status.ERROR));
    }

    @Override
    public CompletableFuture<ChangeInvisibleDurationResult> changeInvisibleDuration(String receiptHandle,
        long invisibleDuration) {
        if (state.get() != State.OPENED) {
            return CompletableFuture.failedFuture(new StoreException(StoreErrorCode.TOPIC_QUEUE_NOT_OPENED, "Topic queue not opened"));
        }
        ReceiptHandle handle = decodeReceiptHandle(receiptHandle);
        ChangeInvisibleDurationOperation operation = new ChangeInvisibleDurationOperation(handle.topicId(),
            handle.queueId(), operationStreamId, snapshotStreamId, stateMachine, handle.consumerGroupId(),
            handle.operationId(), System.nanoTime(), invisibleDuration);
        return operationLogService.logChangeInvisibleDurationOperation(operation).thenApply(nil ->
                new ChangeInvisibleDurationResult(ChangeInvisibleDurationResult.Status.SUCCESS))
            .exceptionally(throwable -> new ChangeInvisibleDurationResult(ChangeInvisibleDurationResult.Status.ERROR));
    }

    @Override
    public CompletableFuture<QueueOffsetRange> getOffsetRange() {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<Integer> getInflightStats(long consumerGroupId) {
        return CompletableFuture.completedFuture(inflightService.getInflightCount(consumerGroupId, topicId, queueId));
    }

    @Override
    public CompletableFuture<PullResult> pullNormal(long consumerGroupId, Filter filter, long startOffset,
        int batchSize) {
        if (state.get() != State.OPENED) {
            return CompletableFuture.failedFuture(new StoreException(StoreErrorCode.TOPIC_QUEUE_NOT_OPENED, "Topic queue not opened"));
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
        long operationTimestamp = System.nanoTime();
        CompletableFuture<FilterFetchResult> fetchCf = fetchAndFilterMessages(streamId, startOffset, batchSize,
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
            return CompletableFuture.failedFuture(new StoreException(StoreErrorCode.TOPIC_QUEUE_NOT_OPENED, "Topic queue not opened"));
        }
        CompletableFuture<Long> retryStreamIdCf = retryStreamId(consumerGroupId);
        return retryStreamIdCf.thenCompose(streamId -> pull(streamId, consumerGroupId, filter, startOffset, batchSize));
    }

    @Override
    public CompletableFuture<Long> getConsumeOffset(long consumerGroupId) {
        return stateMachine.consumeOffset(consumerGroupId);
    }

    @Override
    public CompletableFuture<Long> getAckOffset(long consumerGroupId) {
        return stateMachine.ackOffset(consumerGroupId);
    }

    @Override
    public CompletableFuture<Long> getRetryConsumeOffset(long consumerGroupId) {
        return stateMachine.retryConsumeOffset(consumerGroupId);
    }

    @Override
    public CompletableFuture<Long> getRetryAckOffset(long consumerGroupId) {
        return stateMachine.retryAckOffset(consumerGroupId);
    }

    @Override
    public CompletableFuture<State> getState() {
        return CompletableFuture.completedFuture(state.get());
    }
}
