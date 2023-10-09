/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.automq.rocketmq.store.mock;

import com.automq.rocketmq.store.api.MessageStateMachine;
import com.automq.rocketmq.store.model.operation.AckOperation;
import com.automq.rocketmq.store.model.operation.ChangeInvisibleDurationOperation;
import com.automq.rocketmq.store.model.operation.PopOperation;
import com.automq.rocketmq.store.service.api.OperationLogService;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class MockOperationLogService implements OperationLogService {
    AtomicLong operationCount = new AtomicLong(0);

    AtomicBoolean recoverFailed = new AtomicBoolean(false);

    @Override
    public CompletableFuture<Long> logPopOperation(PopOperation operation) {
        return null;
    }

    @Override
    public CompletableFuture<Long> logAckOperation(AckOperation operation) {
        return null;
    }

    @Override
    public CompletableFuture<Long> logChangeInvisibleDurationOperation(ChangeInvisibleDurationOperation operation) {
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
