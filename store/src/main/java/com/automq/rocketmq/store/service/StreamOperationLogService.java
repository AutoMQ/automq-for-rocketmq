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

import com.automq.rocketmq.metadata.StoreMetadataService;
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

public class StreamOperationLogService implements OperationLogService {

    private final long opStreamId;
    private final long snapshotStreamId;
    private final StreamStore streamStore;
    private final StoreMetadataService metadataService;
    private final MessageStateMachine stateMachine;

    public StreamOperationLogService(long opStreamId, long snapshotStreamId, StreamStore streamStore, StoreMetadataService metadataService,
        MessageStateMachine stateMachine) {
        this.opStreamId = opStreamId;
        this.snapshotStreamId = snapshotStreamId;
        this.streamStore = streamStore;
        this.metadataService = metadataService;
        this.stateMachine = stateMachine;
    }

    @Override
    public CompletableFuture<Void> start() {
        // TODO: clear all states of this queue before start

        // 1. get snapshot
        long snapshotStartOffset = streamStore.startOffset(snapshotStreamId);
        long snapshotEndOffset = streamStore.nextOffset(snapshotStreamId);
        CompletableFuture<OperationSnapshot> snapshotFetch;
        if (snapshotStartOffset == snapshotEndOffset) {
            // no snapshot
            snapshotFetch = CompletableFuture.completedFuture(null);
        } else {
            snapshotFetch = streamStore.fetch(snapshotStreamId, snapshotEndOffset - 1, 1)
                .thenApply(result -> {
                    return SerializeUtil.decodeOperationSnapshot(result.recordBatchList().get(0).rawPayload());
                });
        }
        // 2. get all operations
        long startOffset = streamStore.startOffset(opStreamId);
        long endOffset = streamStore.nextOffset(opStreamId);
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
                    replay(operation);
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
                replay(operation);
            } catch (StoreException e) {
                throw new RuntimeException(e);
            }
            return operation.getOperationTimestamp();
        });
    }

    private void loadSnapshot(OperationSnapshot snapshot) throws StoreException {
        stateMachine.loadSnapshot(snapshot);
    }

    private void replay(Operation operation) throws StoreException {
        // TODO: optimize concurrent control
        switch (operation.getOperationType()) {
            case POP -> stateMachine.replayPopOperation((PopOperation) operation);
            case ACK -> stateMachine.replayAckOperation((AckOperation) operation);
            case CHANGE_INVISIBLE_DURATION -> stateMachine.replayChangeInvisibleDurationOperation((ChangeInvisibleDurationOperation) operation);
            default -> throw new IllegalStateException("Unexpected value: " + operation.getOperationType());
        }
    }

    @Override
    public CompletableFuture<Long> logAckOperation(AckOperation operation) {
        CompletableFuture<AppendResult> append = streamStore.append(opStreamId, new SingleRecord(
            ByteBuffer.wrap(SerializeUtil.encodeAckOperation(operation))));
        return append.thenApply(result -> {
            try {
                replay(operation);
            } catch (StoreException e) {
                throw new RuntimeException(e);
            }
            return operation.getOperationTimestamp();
        });
    }


    @Override
    public CompletableFuture<Long> logChangeInvisibleDurationOperation(ChangeInvisibleDurationOperation operation) {
        CompletableFuture<AppendResult> append = streamStore.append(opStreamId, new SingleRecord(
            ByteBuffer.wrap(SerializeUtil.encodeChangeInvisibleDurationOperation(operation))));
        return append.thenApply(result -> {
            try {
                replay(operation);
            } catch (StoreException e) {
                throw new RuntimeException(e);
            }
            return operation.getOperationTimestamp();
        });
    }
}
