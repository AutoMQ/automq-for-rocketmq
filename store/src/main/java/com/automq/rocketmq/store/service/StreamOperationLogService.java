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

package com.automq.rocketmq.store.service;

import com.automq.rocketmq.common.config.StoreConfig;
import com.automq.rocketmq.store.api.MessageStateMachine;
import com.automq.rocketmq.store.api.StreamStore;
import com.automq.rocketmq.store.exception.StoreErrorCode;
import com.automq.rocketmq.store.exception.StoreException;
import com.automq.rocketmq.store.model.StoreContext;
import com.automq.rocketmq.store.model.operation.AckOperation;
import com.automq.rocketmq.store.model.operation.ChangeInvisibleDurationOperation;
import com.automq.rocketmq.store.model.operation.Operation;
import com.automq.rocketmq.store.model.operation.PopOperation;
import com.automq.rocketmq.store.model.operation.ResetConsumeOffsetOperation;
import com.automq.rocketmq.store.model.stream.SingleRecord;
import com.automq.rocketmq.store.service.api.OperationLogService;
import com.automq.rocketmq.store.util.SerializeUtil;
import com.automq.stream.api.AppendResult;
import com.automq.stream.api.RecordBatchWithContext;
import com.automq.stream.utils.FutureUtil;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StreamOperationLogService implements OperationLogService {
    public static final Logger LOGGER = LoggerFactory.getLogger(StreamOperationLogService.class);
    private final StreamStore streamStore;
    private final SnapshotService snapshotService;
    private final StoreConfig storeConfig;

    public StreamOperationLogService(StreamStore streamStore, SnapshotService snapshotService,
        StoreConfig storeConfig) {
        this.streamStore = streamStore;
        this.snapshotService = snapshotService;
        this.storeConfig = storeConfig;
    }

    @Override
    public CompletableFuture<Void> recover(MessageStateMachine stateMachine, long operationStreamId,
        long snapshotStreamId) {
        // 1. get snapshot
        SnapshotService.SnapshotStatus snapshotStatus = snapshotService.getSnapshotStatus(stateMachine.topicId(), stateMachine.queueId());
        long snapStartOffset = streamStore.startOffset(snapshotStreamId);
        snapshotStatus.snapshotEndOffset().set(streamStore.nextOffset(snapshotStreamId));
        long startOffset = streamStore.startOffset(operationStreamId);
        snapshotStatus.operationStartOffset().set(startOffset);
        long endOffset = streamStore.nextOffset(operationStreamId);
        CompletableFuture<Long/*op replay start offset*/> snapshotFetch;
        long snapEndOffset = snapshotStatus.snapshotEndOffset().get();
        if (snapStartOffset == snapEndOffset) {
            // no snapshot
            snapshotFetch = CompletableFuture.completedFuture(startOffset);
        } else {
            snapshotFetch = streamStore.fetch(StoreContext.EMPTY, snapshotStreamId, snapEndOffset - 1, 1)
                .thenApply(result -> SerializeUtil.decodeOperationSnapshot(result.recordBatchList().get(0).rawPayload()))
                .thenApply(snapshot -> {
                    stateMachine.loadSnapshot(snapshot);
                    return snapshot.getSnapshotEndOffset() + 1;
                });
        }

        if (startOffset == endOffset) {
            // no operation
            return snapshotFetch.thenAccept(offset -> {
            });
        }

        // 2. get all operations
        // TODO: batch fetch, fetch all at once may cause some problems
        return snapshotFetch.thenCompose(offset -> streamStore.fetch(StoreContext.EMPTY, operationStreamId, offset, (int) (endOffset - offset))
            .thenAccept(result -> {
                // load operations
                for (RecordBatchWithContext batchWithContext : result.recordBatchList()) {
                    try {
                        // TODO: assume that a batch only contains one operation
                        Operation operation = SerializeUtil.decodeOperation(batchWithContext.rawPayload(), stateMachine,
                            operationStreamId, snapshotStreamId);

                        // TODO: operation may be null
                        replay(batchWithContext.baseOffset(), operation);
                    } catch (StoreException e) {
                        LOGGER.error("Topic {}, queue: {}, operation stream id: {}, offset: {}: replay operation failed when recover", stateMachine.topicId(), stateMachine.queueId(), operationStreamId, batchWithContext.baseOffset(), e);
                        if (e.code() != StoreErrorCode.ILLEGAL_ARGUMENT) {
                            throw new CompletionException(e);
                        }
                    }
                }
            }));
    }

    @Override
    public CompletableFuture<LogResult> logPopOperation(PopOperation operation) {
        return streamStore.append(StoreContext.EMPTY, operation.operationStreamId(),
                new SingleRecord(ByteBuffer.wrap(SerializeUtil.encodePopOperation(operation))))
            .thenApply(result -> {
                try {
                    return doReplay(result, operation);
                } catch (StoreException e) {
                    LOGGER.error("Topic {}, queue: {}: Replay pop operation: {} failed", operation.topicId(), operation.queueId(), operation, e);
                    throw new CompletionException(e);
                }
            });
    }

    @Override
    public CompletableFuture<LogResult> logAckOperation(AckOperation operation) {
        return streamStore.append(StoreContext.EMPTY, operation.operationStreamId(),
                new SingleRecord(ByteBuffer.wrap(SerializeUtil.encodeAckOperation(operation))))
            .thenApply(result -> {
                try {
                    return doReplay(result, operation);
                } catch (StoreException e) {
                    LOGGER.error("Topic {}, queue: {}: Replay ack operation: {} failed", operation.topicId(), operation.queueId(), operation, e);
                    throw new CompletionException(e);
                }
            });
    }

    @Override
    public CompletableFuture<LogResult> logChangeInvisibleDurationOperation(
        ChangeInvisibleDurationOperation operation) {
        return streamStore.append(StoreContext.EMPTY, operation.operationStreamId(),
                new SingleRecord(ByteBuffer.wrap(SerializeUtil.encodeChangeInvisibleDurationOperation(operation))))
            .thenApply(result -> {
                try {
                    return doReplay(result, operation);
                } catch (StoreException e) {
                    LOGGER.error("Topic {}, queue: {}: Replay change invisible duration operation: {} failed", operation.topicId(), operation.queueId(), operation, e);
                    throw new CompletionException(e);
                }
            });
    }

    @Override
    public CompletableFuture<LogResult> logResetConsumeOffsetOperation(ResetConsumeOffsetOperation operation) {
        return streamStore.append(StoreContext.EMPTY, operation.operationStreamId(),
                new SingleRecord(ByteBuffer.wrap(SerializeUtil.encodeResetConsumeOffsetOperation(operation))))
            .thenApply(result -> {
                try {
                    return doReplay(result, operation);
                } catch (StoreException e) {
                    LOGGER.error("Topic {}, queue: {}: Replay reset consume offset operation: {} failed", operation.topicId(), operation.queueId(), operation, e);
                    throw new CompletionException(e);
                }
            });
    }

    private void notifySnapshot(Operation operation) {
        MessageStateMachine stateMachine = operation.stateMachine();
        SnapshotService.SnapshotStatus snapshotStatus = snapshotService.getSnapshotStatus(stateMachine.topicId(), stateMachine.queueId());
        if (snapshotStatus.takingSnapshot().compareAndSet(false, true)) {
            CompletableFuture<SnapshotService.TakeSnapshotResult> taskCf = snapshotService.addSnapshotTask(new SnapshotService.SnapshotTask(
                operation.topicId(), operation.queueId(), operation.operationStreamId(), operation.snapshotStreamId(),
                stateMachine::takeSnapshot));
            taskCf.thenAccept(takeSnapshotResult -> {
                snapshotStatus.takingSnapshot().set(false);
                if (takeSnapshotResult.success()) {
                    snapshotStatus.operationStartOffset().set(takeSnapshotResult.newOpStartOffset());
                } else {
                    LOGGER.warn("Topic {}, queue: {}: Take snapshot task is aborted", operation.topicId(), operation.queueId());
                }
            }).exceptionally(e -> {
                Throwable cause = FutureUtil.cause(e);
                LOGGER.error("Topic {}, queue: {}: Take snapshot failed", operation.topicId(), operation.queueId(), cause);
                snapshotStatus.takingSnapshot().set(false);
                return null;
            });
        }
    }

    private LogResult doReplay(AppendResult appendResult, Operation operation) throws StoreException {
        long operationOffset = appendResult.baseOffset();
        LogResult logResult = replay(operationOffset, operation);
        MessageStateMachine stateMachine = operation.stateMachine();
        SnapshotService.SnapshotStatus snapshotStatus = snapshotService.getSnapshotStatus(stateMachine.topicId(), stateMachine.queueId());
        if (operationOffset - snapshotStatus.operationStartOffset().get() + 1 >= storeConfig.operationSnapshotInterval()) {
            notifySnapshot(operation);
        }
        return logResult;
    }

    private LogResult replay(long operationOffset, Operation operation) throws StoreException {
        LogResult logResult = new LogResult(operationOffset);
        switch (operation.operationType()) {
            case POP -> {
                MessageStateMachine.ReplayPopResult replayPopResult = operation.stateMachine().replayPopOperation(operationOffset, (PopOperation) operation);
                logResult.setPopTimes(replayPopResult.getPopTimes());
            }
            case ACK -> operation.stateMachine().replayAckOperation(operationOffset, (AckOperation) operation);
            case CHANGE_INVISIBLE_DURATION ->
                operation.stateMachine().replayChangeInvisibleDurationOperation(operationOffset, (ChangeInvisibleDurationOperation) operation);
            case RESET_CONSUME_OFFSET ->
                operation.stateMachine().replayResetConsumeOffsetOperation(operationOffset, (ResetConsumeOffsetOperation) operation);
            default -> throw new IllegalStateException("Unexpected value: " + operation.operationType());
        }

        return logResult;
    }
}
