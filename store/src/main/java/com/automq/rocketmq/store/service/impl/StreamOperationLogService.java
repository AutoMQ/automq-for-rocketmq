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

package com.automq.rocketmq.store.service.impl;

import com.automq.rocketmq.store.StreamStore;
import com.automq.rocketmq.store.model.generated.ReceiptHandle;
import com.automq.rocketmq.store.model.stream.SingleRecord;
import com.automq.rocketmq.store.service.OperationLogService;
import com.automq.rocketmq.store.util.SerializeUtil;
import com.automq.rocketmq.stream.api.AppendResult;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;

public class StreamOperationLogService implements OperationLogService {
    private final StreamStore streamStore;

    public StreamOperationLogService(StreamStore streamStore) {
        this.streamStore = streamStore;
    }

    @Override
    public CompletableFuture<Long> logPopOperation(long consumeGroupId, long topicId, int queueId, long offset,
        int batchSize, boolean isOrder, long invisibleDuration, long operationTimestamp) {
        // TODO: get the stream id from metadata server.
        CompletableFuture<AppendResult> append = streamStore.append(0, new SingleRecord(operationTimestamp,
            ByteBuffer.wrap(SerializeUtil.encodePopOperation(consumeGroupId, topicId, queueId, offset, batchSize, isOrder, invisibleDuration, operationTimestamp))));
        return append.thenApply(AppendResult::baseOffset);
    }

    @Override
    public CompletableFuture<Long> logAckOperation(ReceiptHandle receiptHandle,
        long operationTimestamp) {
        // TODO: get the stream id from metadata server.
        CompletableFuture<AppendResult> append = streamStore.append(0, new SingleRecord(operationTimestamp,
            ByteBuffer.wrap(SerializeUtil.encodeAckOperation(receiptHandle, operationTimestamp))));
        return append.thenApply(AppendResult::baseOffset);
    }

    @Override
    public CompletableFuture<Long> logChangeInvisibleDurationOperation(ReceiptHandle receiptHandle,
        long invisibleDuration, long operationTimestamp) {
        // TODO: get the stream id from metadata server.
        CompletableFuture<AppendResult> append = streamStore.append(0, new SingleRecord(operationTimestamp,
            ByteBuffer.wrap(SerializeUtil.encodeChangeInvisibleDurationOperation(receiptHandle, invisibleDuration, operationTimestamp))));
        return append.thenApply(AppendResult::baseOffset);
    }
}
