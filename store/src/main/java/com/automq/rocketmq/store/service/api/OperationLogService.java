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

package com.automq.rocketmq.store.service.api;

import com.automq.rocketmq.store.api.MessageStateMachine;
import com.automq.rocketmq.store.model.StoreContext;
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
    CompletableFuture<LogResult> logPopOperation(StoreContext context, PopOperation operation);

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
