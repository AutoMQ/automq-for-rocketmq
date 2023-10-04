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
import com.automq.rocketmq.metadata.api.StoreMetadataService;
import com.automq.rocketmq.store.api.MessageStateMachine;
import com.automq.rocketmq.store.api.StreamStore;
import com.automq.rocketmq.store.exception.StoreException;
import com.automq.rocketmq.store.model.operation.AckOperation;
import com.automq.rocketmq.store.model.operation.ChangeInvisibleDurationOperation;
import com.automq.rocketmq.store.model.operation.Operation;
import com.automq.rocketmq.store.model.operation.OperationSnapshot;
import com.automq.rocketmq.store.model.operation.PopOperation;
import com.automq.rocketmq.store.model.stream.SingleRecord;
import com.automq.rocketmq.store.service.api.OperationLogService;
import com.automq.rocketmq.store.util.SerializeUtil;
import com.automq.stream.api.AppendResult;
import com.automq.stream.api.RecordBatchWithContext;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class StreamOperationLogService implements OperationLogService {
    private final long topicId;
    private final int queueId;
    private final long opStreamId;
    private final long snapshotStreamId;
    private final StreamStore streamStore;
    private final StoreMetadataService metadataService;
    private final MessageStateMachine stateMachine;
    private final SnapshotService snapshotService;
    private final StoreConfig storeConfig;
    private final AtomicLong snapshotEndOffset = new AtomicLong(-1);
    private final AtomicLong opStartOffset = new AtomicLong(-1);
    private final AtomicBoolean takingSnapshot = new AtomicBoolean(false);

    public StreamOperationLogService(long topicId, int queueId,
        long opStreamId, long snapshotStreamId, StreamStore streamStore, StoreMetadataService metadataService,
        MessageStateMachine stateMachine, SnapshotService snapshotService, StoreConfig storeConfig) {
        this.topicId = topicId;
        this.queueId = queueId;
        this.opStreamId = opStreamId;
        this.snapshotStreamId = snapshotStreamId;
        this.streamStore = streamStore;
        this.metadataService = metadataService;
        this.stateMachine = stateMachine;
        this.snapshotService = snapshotService;
        this.storeConfig = storeConfig;
    }

    @Override
    public CompletableFuture<Void> start() {
        // TODO: clear all states of this queue before start

        // 1. get snapshot
        long snapStartOffset = streamStore.startOffset(snapshotStreamId);
        snapshotEndOffset.set(streamStore.nextOffset(snapshotStreamId));
        CompletableFuture<OperationSnapshot> snapshotFetch;
        long snapEndOffset = snapshotEndOffset.get();
        if (snapStartOffset == snapEndOffset) {
            // no snapshot
            snapshotFetch = CompletableFuture.completedFuture(null);
        } else {
            snapshotFetch = streamStore.fetch(snapshotStreamId, snapEndOffset - 1, 1)
                .thenApply(result -> {
                    return SerializeUtil.decodeOperationSnapshot(result.recordBatchList().get(0).rawPayload());
                });
        }
        // 2. get all operations
        long startOffset = streamStore.startOffset(opStreamId);
        long endOffset = streamStore.nextOffset(opStreamId);
        opStartOffset.set(startOffset);
        // TODO: batch fetch, fetch all at once may case some problems
        return snapshotFetch.thenCombine(streamStore.fetch(opStreamId, startOffset, (int) (endOffset - startOffset)), (snapshot, result) -> {
            // load snapshot first
            if (snapshot != null) {
                try {
                    loadSnapshot(snapshot);
                } catch (StoreException e) {
                    throw new RuntimeException(e);
                }
            }
            // load operations
            for (RecordBatchWithContext batchWithContext : result.recordBatchList()) {
                // TODO: assume that a batch only contains one operation
                Operation operation = SerializeUtil.decodeOperation(batchWithContext.rawPayload());
                try {
                    replay(batchWithContext.baseOffset(), operation);
                } catch (StoreException e) {
                    throw new RuntimeException(e);
                }
            }
            return null;
        });
    }

    @Override
    public CompletableFuture<Long> logPopOperation(PopOperation operation) {
        CompletableFuture<AppendResult> append = streamStore.append(opStreamId, new SingleRecord(
            ByteBuffer.wrap(SerializeUtil.encodePopOperation(operation))));
        return append.thenApply(result -> {
            // TODO: think about append return order
            try {
                replay(result.baseOffset(), operation);
            } catch (StoreException e) {
                throw new RuntimeException(e);
            }
            if (result.baseOffset() - opStartOffset.get() + 1 >= storeConfig.operationSnapshotInterval()) {
                notifySnapshot();
            }
            return result.baseOffset();
        });
    }

    private void notifySnapshot() {
        if (this.takingSnapshot.compareAndSet(false, true)) {
            CompletableFuture<Long> taskCf = snapshotService.addSnapshotTask(new SnapshotService.SnapshotTask(
                topicId,
                queueId,
                opStreamId,
                snapshotStreamId,
                stateMachine::takeSnapshot));
            taskCf.thenAccept(newStartOffset -> {
                this.takingSnapshot.set(false);
                this.opStartOffset.set(newStartOffset);
            }).exceptionally(e -> {
                // TODO: log it
                this.takingSnapshot.set(false);
                return null;
            });
        }
    }

    private void loadSnapshot(OperationSnapshot snapshot) throws StoreException {
        stateMachine.loadSnapshot(snapshot);
    }

    private void replay(long operationOffset, Operation operation) throws StoreException {
        // TODO: optimize concurrent control
        switch (operation.getOperationType()) {
            case POP -> stateMachine.replayPopOperation(operationOffset, (PopOperation) operation);
            case ACK -> stateMachine.replayAckOperation(operationOffset, (AckOperation) operation);
            case CHANGE_INVISIBLE_DURATION -> stateMachine.replayChangeInvisibleDurationOperation(operationOffset, (ChangeInvisibleDurationOperation) operation);
            default -> throw new IllegalStateException("Unexpected value: " + operation.getOperationType());
        }
    }

    @Override
    public CompletableFuture<Long> logAckOperation(AckOperation operation) {
        CompletableFuture<AppendResult> append = streamStore.append(opStreamId, new SingleRecord(
            ByteBuffer.wrap(SerializeUtil.encodeAckOperation(operation))));
        return append.thenApply(result -> {
            try {
                replay(result.baseOffset(), operation);
            } catch (StoreException e) {
                throw new RuntimeException(e);
            }
            if (result.baseOffset() - opStartOffset.get() + 1 >= storeConfig.operationSnapshotInterval()) {
                notifySnapshot();
            }
            return result.baseOffset();
        });
    }


    @Override
    public CompletableFuture<Long> logChangeInvisibleDurationOperation(ChangeInvisibleDurationOperation operation) {
        CompletableFuture<AppendResult> append = streamStore.append(opStreamId, new SingleRecord(
            ByteBuffer.wrap(SerializeUtil.encodeChangeInvisibleDurationOperation(operation))));
        return append.thenApply(result -> {
            try {
                replay(result.baseOffset(), operation);
            } catch (StoreException e) {
                throw new RuntimeException(e);
            }
            if (result.baseOffset() - opStartOffset.get() + 1 >= storeConfig.operationSnapshotInterval()) {
                notifySnapshot();
            }
            return result.baseOffset();
        });
    }
}
