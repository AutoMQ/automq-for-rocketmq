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

package com.automq.rocketmq.store.service;

import com.automq.rocketmq.common.config.StoreConfig;
import com.automq.rocketmq.store.api.MessageStateMachine;
import com.automq.rocketmq.store.api.StreamStore;
import com.automq.rocketmq.store.exception.StoreErrorCode;
import com.automq.rocketmq.store.exception.StoreException;
import com.automq.rocketmq.store.model.StoreContext;
import com.automq.rocketmq.store.model.generated.CheckPoint;
import com.automq.rocketmq.store.model.operation.AckOperation;
import com.automq.rocketmq.store.model.operation.ChangeInvisibleDurationOperation;
import com.automq.rocketmq.store.model.operation.Operation;
import com.automq.rocketmq.store.model.operation.OperationSnapshot;
import com.automq.rocketmq.store.model.operation.PopOperation;
import com.automq.rocketmq.store.model.operation.ResetConsumeOffsetOperation;
import com.automq.rocketmq.store.model.stream.SingleRecord;
import com.automq.rocketmq.store.service.api.OperationLogService;
import com.automq.rocketmq.store.util.SerializeUtil;
import com.automq.stream.api.AppendResult;
import com.automq.stream.api.RecordBatchWithContext;
import com.automq.stream.utils.FutureUtil;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicReference;
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

        // Keep a reference to the snapshot so that we can write the remaining checkpoints to the kv service after replaying all operations
        AtomicReference<OperationSnapshot> operationSnapshot = new AtomicReference<>();
        if (snapStartOffset == snapEndOffset) {
            // no snapshot
            snapshotFetch = CompletableFuture.completedFuture(startOffset);
        } else {
            snapshotFetch = streamStore.fetch(StoreContext.EMPTY, snapshotStreamId, snapEndOffset - 1, 1)
                .thenApply(result -> SerializeUtil.decodeOperationSnapshot(result.recordBatchList().get(0).rawPayload()))
                .thenApply(snapshot -> {
                    stateMachine.loadSnapshot(snapshot);
                    operationSnapshot.set(snapshot);
                    return snapshot.getSnapshotEndOffset() + 1;
                });
        }

        TreeMap<Long, Operation> treeMap = new TreeMap<>();
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
                            annihilate(batchWithContext.baseOffset(), operation, operationSnapshot.get(), treeMap);
                        } catch (StoreException e) {
                            LOGGER.error("Topic {}, queue: {}, operation stream id: {}, offset: {}: replay operation failed when recover", stateMachine.topicId(), stateMachine.queueId(), operationStreamId, batchWithContext.baseOffset(), e);
                            if (e.code() != StoreErrorCode.ILLEGAL_ARGUMENT) {
                                throw new CompletionException(e);
                            }
                        }
                    }
                }))
            .whenComplete((v, throwable) -> {
                // Write the remaining checkpoints to the kv service and replay the remaining operations
                try {
                    OperationSnapshot snapshot = operationSnapshot.get();
                    if (null != snapshot) {
                        List<CheckPoint> checkpoints = snapshot.getCheckPoints();
                        if (null != operationSnapshot.get() && null != checkpoints && !checkpoints.isEmpty()) {
                            stateMachine.writeCheckPointsAndRelatedStates(checkpoints);
                        }
                    }
                    for (Map.Entry<Long, Operation> entry : treeMap.entrySet()) {
                        replay(entry.getKey(), entry.getValue());
                    }
                } catch (StoreException e) {
                    throw new CompletionException(e);
                }
            });
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

    private void annihilate(long operationOffset, Operation operation, OperationSnapshot snapshot,
        Map<Long, Operation> operationMap) {
        switch (operation.operationType()) {
            case POP -> {
                if (null != operationMap) {
                    operationMap.put(operationOffset, operation);
                }
            }
            case ACK -> {
                boolean annihilated = false;
                if (null != operationMap) {
                    annihilated = operationMap.remove(((AckOperation) operation).operationId()) != null;
                }

                if (!annihilated && null != snapshot) {
                    List<CheckPoint> checkPoints = snapshot.getCheckPoints();
                    if (null != checkPoints && !checkPoints.isEmpty()) {
                        annihilated = checkPoints.removeIf(checkPoint -> checkPoint.operationId() == ((AckOperation) operation).operationId());
                    }
                }

                if (!annihilated) {
                    // Something is wrong, we should not have an ack operation without a corresponding pop operation
                    LOGGER.warn("Topic {}, queue: {}: Ack operation: {} is not annihilated", operation.topicId(), operation.queueId(), operation);
                }
            }
            default -> operationMap.put(operationOffset, operation);
        }
    }
}
