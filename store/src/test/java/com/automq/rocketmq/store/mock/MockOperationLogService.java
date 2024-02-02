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

package com.automq.rocketmq.store.mock;

import com.automq.rocketmq.store.api.MessageStateMachine;
import com.automq.rocketmq.store.model.operation.AckOperation;
import com.automq.rocketmq.store.model.operation.ChangeInvisibleDurationOperation;
import com.automq.rocketmq.store.model.operation.PopOperation;
import com.automq.rocketmq.store.model.operation.ResetConsumeOffsetOperation;
import com.automq.rocketmq.store.service.api.OperationLogService;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class MockOperationLogService implements OperationLogService {
    AtomicLong operationCount = new AtomicLong(0);

    AtomicBoolean recoverFailed = new AtomicBoolean(false);

    @Override
    public CompletableFuture<LogResult> logPopOperation(PopOperation operation) {
        return null;
    }

    @Override
    public CompletableFuture<LogResult> logAckOperation(AckOperation operation) {
        return null;
    }

    @Override
    public CompletableFuture<LogResult> logChangeInvisibleDurationOperation(ChangeInvisibleDurationOperation operation) {
        return null;
    }

    @Override
    public CompletableFuture<LogResult> logResetConsumeOffsetOperation(ResetConsumeOffsetOperation operation) {
        return null;
    }

    @Override
    public CompletableFuture<Void> recover(MessageStateMachine stateMachine, long operationStreamId,
        long snapshotStreamId) {
        if (recoverFailed.get()) {
            return CompletableFuture.failedFuture(new RuntimeException("recover failed"));
        }
        return CompletableFuture.completedFuture(null);
    }

    public AtomicBoolean recoverFailed() {
        return recoverFailed;
    }

    public void setRecoverFailed(boolean recoverFailed) {
        this.recoverFailed.set(recoverFailed);
    }
}
