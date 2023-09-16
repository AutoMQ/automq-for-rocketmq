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

import com.automq.rocketmq.store.model.generated.ReceiptHandle;
import com.automq.rocketmq.store.service.OperationLogService;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;

public class MockOperationLogService implements OperationLogService {
    AtomicLong operationCount = new AtomicLong(0);

    @Override
    public CompletableFuture<Long> logPopOperation(long consumeGroupId, long topicId, int queueId, long offset,
        int batchSize, boolean isOrder, long invisibleDuration, long operationTimestamp) {
        return CompletableFuture.completedFuture(operationCount.incrementAndGet());
    }

    @Override
    public CompletableFuture<Long> logAckOperation(ReceiptHandle receiptHandle, long operationTimestamp) {
        return CompletableFuture.completedFuture(operationCount.incrementAndGet());
    }

    @Override
    public CompletableFuture<Long> logChangeInvisibleDurationOperation(ReceiptHandle receiptHandle,
        long invisibleDuration, long operationTimestamp) {
        return CompletableFuture.completedFuture(operationCount.incrementAndGet());
    }
}
