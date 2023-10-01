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
import com.automq.rocketmq.metadata.StoreMetadataService;
import com.automq.rocketmq.store.api.MessageStateMachine;
import com.automq.rocketmq.store.api.StreamStore;
import com.automq.rocketmq.store.api.TopicQueue;
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
import com.automq.rocketmq.store.service.StreamOperationLogService;
import com.automq.rocketmq.store.service.api.OperationLogService;
import com.automq.rocketmq.store.util.SerializeUtil;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

import static com.automq.rocketmq.store.util.SerializeUtil.decodeReceiptHandle;

public class StreamTopicQueue extends TopicQueue {

    private OperationLogService operationLogService;

    private final StoreMetadataService metadataService;

    private final MessageStateMachine stateMachine;

    private long dataStreamId;

    private long operationStreamId;

    private long snapshotStreamId;

    private Map<Long/*consumerGroupId*/, Long/*retryStreamId*/> retryStreamIds;

    private final StreamStore streamStore;
    private final StoreConfig config;

    private InflightService inflightService;

    public StreamTopicQueue(StoreConfig config, long topicId, int queueId,
        StoreMetadataService metadataService, MessageStateMachine stateMachine, StreamStore streamStore,
        InflightService inflightService) {
        super(topicId, queueId);
        this.config = config;
        this.metadataService = metadataService;
        this.stateMachine = stateMachine;
        this.streamStore = streamStore;
        this.retryStreamIds = new ConcurrentHashMap<>();
        this.inflightService = inflightService;
    }

    @Override
    public CompletableFuture<Void> open() {
        // open all streams and load snapshot
        return CompletableFuture.allOf(
            metadataService.dataStreamOf(topicId, queueId).thenCompose(streamMetadata -> {
                this.dataStreamId = streamMetadata.getStreamId();
                return streamStore.open(streamMetadata.getStreamId());
            }),
            metadataService.operationStreamOf(topicId, queueId).thenCompose(streamMetadata -> {
                this.operationStreamId = streamMetadata.getStreamId();
                return streamStore.open(streamMetadata.getStreamId());
            }),
            metadataService.snapshotStreamOf(topicId, queueId).thenCompose(streamMetadata -> {
                this.snapshotStreamId = streamMetadata.getStreamId();
                return streamStore.open(streamMetadata.getStreamId());
            })
        ).thenCompose(nil -> {
            // recover from operation log
            this.operationLogService = new StreamOperationLogService(
                this.operationStreamId,
                this.snapshotStreamId,
                this.streamStore,
                this.metadataService,
                this.stateMachine
            );
            return recover();
        });
    }

    private CompletableFuture<Void> recover() {
        return this.operationLogService.start();
    }

    @Override
    public CompletableFuture<Void> close() {
        return streamStore.close(Arrays.asList(dataStreamId, operationStreamId, snapshotStreamId));
    }

    @Override
    public CompletableFuture<PutResult> put(FlatMessage flatMessage) {
        return streamStore.append(dataStreamId, new SingleRecord(flatMessage.getByteBuffer()))
            .thenApply(appendResult -> new PutResult(PutResult.Status.PUT_OK, appendResult.baseOffset()));
    }

    @Override
    public CompletableFuture<PutResult> putRetry(long consumerGroupId, FlatMessage flatMessage) {
        CompletableFuture<Long> retryStreamIdCf = retryStreamId(consumerGroupId);
        return retryStreamIdCf.thenCompose(streamId -> {
            return streamStore.append(streamId, new SingleRecord(flatMessage.getByteBuffer()))
                .thenApply(appendResult -> new PutResult(PutResult.Status.PUT_OK, appendResult.baseOffset()));
        });
    }

    private CompletableFuture<Long> retryStreamId(long consumerGroupId) {
        if (!retryStreamIds.containsKey(consumerGroupId)) {
            return metadataService.retryStreamOf(consumerGroupId, topicId, queueId).thenCompose(streamMetadata -> {
                long retryStreamId = streamMetadata.getStreamId();
                retryStreamIds.put(consumerGroupId, retryStreamId);
                return streamStore.open(retryStreamId).thenApply(nil -> retryStreamId);
            });
        } else {
            return CompletableFuture.completedFuture(retryStreamIds.get(consumerGroupId));
        }
    }

    @Override
    public CompletableFuture<PopResult> popNormal(long consumerGroup, Filter filter, int batchSize,
        long invisibleDuration) {
        // start from consume offset
        return stateMachine.consumeOffset(consumerGroup).thenCompose(offset -> {
            return pop(consumerGroup, dataStreamId, offset, PopOperation.PopOperationType.POP_NORMAL, filter, batchSize, invisibleDuration);
        });
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
        List<FlatMessageExt> messageList = new ArrayList<>();
        long operationTimestamp = System.currentTimeMillis();
        CompletableFuture<List<FlatMessageExt>> fetchCf = fetchAndFilterMessages(streamId, startOffset, batchSize,
            fetchBatchSize, filter, messageList, 0, 0, operationTimestamp);
        CompletableFuture<Void> appendOpCf = fetchCf.thenCompose(messageExtList -> {
            // write pop operation to operation log
            long preOffset = startOffset - 1;
            List<CompletableFuture<Long>> appendOpCfs = new ArrayList<>(messageExtList.size());
            for (FlatMessageExt messageExt : messageExtList) {
                int count = (int) (messageExt.offset() - preOffset);
                preOffset = messageExt.offset();
                PopOperation popOperation = new PopOperation(
                    consumerGroupId, topicId, queueId, messageExt.offset(), count, invisibleDuration, operationTimestamp,
                    -1, operationType
                );
                appendOpCfs.add(operationLogService.logPopOperation(popOperation));
            }
            return CompletableFuture.allOf(appendOpCfs.toArray(new CompletableFuture[0]));
        });
        return fetchCf.thenCombine(appendOpCf, (messageExtList, nil) -> {
            PopResult.Status status;
            if (messageExtList.isEmpty()) {
                status = PopResult.Status.NOT_FOUND;
            } else {
                status = PopResult.Status.FOUND;
                messageExtList.forEach(messageExt -> {
                    messageExt.setReceiptHandle(SerializeUtil.encodeReceiptHandle(consumerGroupId, topicId, queueId, messageExt.offset(), operationTimestamp));
                });
            }
            inflightService.increaseInflightCount(consumerGroupId, topicId, queueId, messageExtList.size());
            return new PopResult(status, operationTimestamp, operationTimestamp, messageExtList);
        }).exceptionally(throwable -> {
            return new PopResult(PopResult.Status.ERROR, operationTimestamp, operationTimestamp, Collections.emptyList());
        });
    }

    @Override
    public CompletableFuture<PopResult> popFifo(long consumerGroup, Filter filter, int batchSize,
        long invisibleDuration) {
        // start from ack offset
        return stateMachine.ackOffset(consumerGroup).thenCompose(offset -> {
            return stateMachine.isLocked(consumerGroup, offset).thenCompose(isLocked -> {
                if (isLocked) {
                    return CompletableFuture.completedFuture(new PopResult(PopResult.Status.LOCKED, 0, 0, Collections.emptyList()));
                } else {
                    return pop(consumerGroup, dataStreamId, offset, PopOperation.PopOperationType.POP_ORDER, filter, batchSize, invisibleDuration);
                }
            });
        });
    }

    @Override
    public CompletableFuture<PopResult> popRetry(long consumerGroup, Filter filter, int batchSize,
        long invisibleDuration) {
        CompletableFuture<Long> retryStreamIdCf;
        if (!retryStreamIds.containsKey(consumerGroup)) {
            retryStreamIdCf = metadataService.retryStreamOf(consumerGroup, topicId, queueId).thenCompose(streamMetadata -> {
                long retryStreamId = streamMetadata.getStreamId();
                retryStreamIds.put(consumerGroup, retryStreamId);
                return streamStore.open(retryStreamId).thenApply(nil -> retryStreamId);
            });
        } else {
            retryStreamIdCf = CompletableFuture.completedFuture(retryStreamIds.get(consumerGroup));
        }
        // start from retry offset
        return retryStreamIdCf.thenCompose(streamId -> stateMachine.retryOffset(consumerGroup).thenCompose(offset -> {
            return pop(consumerGroup, streamId, offset, PopOperation.PopOperationType.POP_RETRY, filter, batchSize, invisibleDuration);
        }));
    }

    public CompletableFuture<List<FlatMessageExt>> fetchMessages(long streamId, long offset, int batchSize) {
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
    public CompletableFuture<List<FlatMessageExt>> fetchAndFilterMessages(long streamId,
        long offset, int batchSize, int fetchBatchSize, Filter filter, List<FlatMessageExt> messageList,
        int fetchCount, long fetchBytes, long operationTimestamp) {
        // Fetch more messages.
        return fetchMessages(streamId, offset, fetchBatchSize)
            .thenCompose(fetchResult -> {
                // Add filter result to message list.
                messageList.addAll(filter.doFilter(fetchResult));

                // If not enough messages after applying filter, fetch more messages.
                boolean needToFetch = messageList.size() < batchSize;
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
                        messageList, newFetchCount, newFetchBytes, operationTimestamp);
                } else {
                    return CompletableFuture.completedFuture(messageList.subList(0, Math.min(messageList.size(), batchSize)));
                }
            });
    }

    @Override
    public CompletableFuture<AckResult> ack(String receiptHandle) {
        ReceiptHandle handle = decodeReceiptHandle(receiptHandle);
        AckOperation operation = new AckOperation(handle.consumerGroupId(), topicId, queueId, handle.messageOffset(), handle.operationId(),
            System.nanoTime(), AckOperation.AckOperationType.ACK_NORMAL);
        return operationLogService.logAckOperation(operation).thenApply(nil -> {
            inflightService.decreaseInflightCount(handle.consumerGroupId(), topicId, queueId, 1);
            return new AckResult(AckResult.Status.SUCCESS);
        }).exceptionally(throwable -> {
            return new AckResult(AckResult.Status.ERROR);
        });
    }

    @Override
    public CompletableFuture<Void> ackTimeout(String receiptHandle) {
        ReceiptHandle handle = decodeReceiptHandle(receiptHandle);
        AckOperation operation = new AckOperation(handle.consumerGroupId(), topicId, queueId, handle.messageOffset(), handle.operationId(),
            System.nanoTime(), AckOperation.AckOperationType.ACK_TIMEOUT);
        return operationLogService.logAckOperation(operation).thenApply(nil -> {
            inflightService.decreaseInflightCount(handle.consumerGroupId(), topicId, queueId, 1);
            return null;
        });
    }

    @Override
    public CompletableFuture<ChangeInvisibleDurationResult> changeInvisibleDuration(String receiptHandle,
        long invisibleDuration) {
        ReceiptHandle handle = decodeReceiptHandle(receiptHandle);
        ChangeInvisibleDurationOperation operation = new ChangeInvisibleDurationOperation(handle.consumerGroupId(), topicId, queueId,
            handle.messageOffset(), handle.operationId(), System.nanoTime(), invisibleDuration);
        return operationLogService.logChangeInvisibleDurationOperation(operation).thenApply(nil -> {
            return new ChangeInvisibleDurationResult(ChangeInvisibleDurationResult.Status.SUCCESS);
        }).exceptionally(throwable -> {
            return new ChangeInvisibleDurationResult(ChangeInvisibleDurationResult.Status.ERROR);
        });
    }

    @Override
    public CompletableFuture<QueueOffsetRange> getOffsetRange() {
        return null;
    }

    @Override
    public CompletableFuture<Integer> getInflightStats(long consumerGroupId) {
        return CompletableFuture.completedFuture(inflightService.getInflightCount(consumerGroupId, topicId, queueId));
    }

    @Override
    public CompletableFuture<PullResult> pullNormal(long consumerGroupId, Filter filter, long startOffset,
        int batchSize) {
        int fetchBatchSize;
        if (filter.needApply()) {
            // If filter is applied, fetch more messages to apply filter.
            fetchBatchSize = batchSize * config.fetchBatchSizeFactor();
        } else {
            // If filter is not applied, fetch batchSize messages.
            fetchBatchSize = batchSize;
        }
        List<FlatMessageExt> messageList = new ArrayList<>();
        long operationTimestamp = System.nanoTime();
        CompletableFuture<List<FlatMessageExt>> fetchCf = fetchAndFilterMessages(dataStreamId, startOffset, batchSize,
            fetchBatchSize, filter, messageList, 0, 0, operationTimestamp);
        return fetchCf.thenApply(messageExtList -> {
            // TODO: correct offset, ugly logic
            PullResult.Status status;
            long firstOffset = -1;
            long endOffset = -1;
            long nextOffset = -1;
            if (messageExtList.isEmpty()) {
                status = PullResult.Status.NO_MATCHED_MSG;
                nextOffset = startOffset + 1;
            } else {
                status = PullResult.Status.FOUND;
                firstOffset = messageExtList.get(0).offset();
                endOffset = messageExtList.get(messageExtList.size() - 1).offset();
                nextOffset = endOffset + 1;
            }
            return new PullResult(status, nextOffset, firstOffset, endOffset, messageExtList);
        }).exceptionally(throwable -> {
            return new PullResult(PullResult.Status.OFFSET_ILLEGAL, -1, -1, -1, Collections.emptyList());
        });
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
        List<FlatMessageExt> messageList = new ArrayList<>();
        long operationTimestamp = System.nanoTime();
        CompletableFuture<List<FlatMessageExt>> fetchCf = fetchAndFilterMessages(streamId, startOffset, batchSize,
            fetchBatchSize, filter, messageList, 0, 0, operationTimestamp);
        return fetchCf.thenApply(messageExtList -> {
            // TODO: correct offset, ugly logic
            PullResult.Status status;
            long firstOffset = -1;
            long endOffset = -1;
            long nextOffset = -1;
            if (messageExtList.isEmpty()) {
                status = PullResult.Status.NO_MATCHED_MSG;
                nextOffset = startOffset + 1;
            } else {
                status = PullResult.Status.FOUND;
                firstOffset = messageExtList.get(0).offset();
                endOffset = messageExtList.get(messageExtList.size() - 1).offset();
                nextOffset = endOffset + 1;
            }
            return new PullResult(status, nextOffset, firstOffset, endOffset, messageExtList);
        }).exceptionally(throwable -> {
            return new PullResult(PullResult.Status.OFFSET_ILLEGAL, -1, -1, -1, Collections.emptyList());
        });

    }

    @Override
    public CompletableFuture<PullResult> pullRetry(long consumerGroupId, Filter filter, long startOffset,
        int batchSize) {
        CompletableFuture<Long> retryStreamIdCf = retryStreamId(consumerGroupId);
        return retryStreamIdCf.thenCompose(streamId -> {
            return pull(streamId, consumerGroupId, filter, startOffset, batchSize);
        });
    }

    @Override
    public CompletableFuture<Long> getConsumeOffset(long consumerGroupId) {
        return stateMachine.consumeOffset(consumerGroupId);
    }
}
