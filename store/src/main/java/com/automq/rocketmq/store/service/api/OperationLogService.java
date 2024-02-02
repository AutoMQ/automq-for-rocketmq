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

package com.automq.rocketmq.store.service.api;

import com.automq.rocketmq.store.api.MessageStateMachine;
import com.automq.rocketmq.store.model.operation.AckOperation;
import com.automq.rocketmq.store.model.operation.ChangeInvisibleDurationOperation;
import com.automq.rocketmq.store.model.operation.PopOperation;
import com.automq.rocketmq.store.model.operation.ResetConsumeOffsetOperation;
import java.util.concurrent.CompletableFuture;

public interface OperationLogService {
    /**
     * Log pop operation to WAL.
     * Each queue has its own operation log.
     */
    CompletableFuture<LogResult> logPopOperation(PopOperation operation);

    /**
     * Log ack operation to WAL.
     * Each queue has its own operation log.
     */
    CompletableFuture<LogResult> logAckOperation(AckOperation operation);

    /**
     * Log change invisible time operation to WAL.
     * Each queue has its own operation log.
     */
    CompletableFuture<LogResult> logChangeInvisibleDurationOperation(ChangeInvisibleDurationOperation operation);

    /**
     * Log reset consume offset operation to WAL.
     * Each queue has its own operation log.
     */
    CompletableFuture<LogResult> logResetConsumeOffsetOperation(ResetConsumeOffsetOperation operation);

    /**
     * Recover.
     * Each queue has its own operation log.
     *
     * @return monotonic serial number
     */
    CompletableFuture<Void> recover(MessageStateMachine stateMachine, long operationStreamId, long snapshotStreamId);

    class LogResult {
        private final long operationOffset;
        // only for pop operation
        private int popTimes = -1;
        public LogResult(long operationOffset) {
            this.operationOffset = operationOffset;
        }

        public long getOperationOffset() {
            return operationOffset;
        }

        public void setPopTimes(int popTimes) {
            this.popTimes = popTimes;
        }

        public int getPopTimes() {
            return popTimes;
        }

        @Override
        public String toString() {
            return "LogResult{" +
                "operationOffset=" + operationOffset +
                ", popTimes=" + popTimes +
                '}';
        }
    }
}
