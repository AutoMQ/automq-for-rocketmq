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
import com.automq.rocketmq.store.api.StreamStore;
import com.automq.rocketmq.store.model.generated.ReceiptHandle;
import com.automq.rocketmq.store.model.stream.SingleRecord;
import com.automq.rocketmq.store.service.api.OperationLogService;
import com.automq.rocketmq.store.util.SerializeUtil;
import com.automq.stream.api.AppendResult;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.concurrent.CompletableFuture;

public class StreamOperationLogService implements OperationLogService {
    private final StreamStore streamStore;

    private final StoreMetadataService metadataService;

    public StreamOperationLogService(StreamStore streamStore, StoreMetadataService metadataService) {
        this.streamStore = streamStore;
        this.metadataService = metadataService;
    }

    @Override
    public CompletableFuture<Long> logPopOperation(long consumerGroupId, long topicId, int queueId, long offset,
        int batchSize, boolean isOrder, long invisibleDuration, long operationTimestamp) {
        long streamId = metadataService.getOperationLogStreamId(topicId, queueId);
        CompletableFuture<AppendResult> append = streamStore.append(streamId, new SingleRecord(new HashMap<>(),
            ByteBuffer.wrap(SerializeUtil.encodePopOperation(consumerGroupId, topicId, queueId, offset, batchSize, isOrder, invisibleDuration, operationTimestamp))));
        return append.thenApply(AppendResult::baseOffset);
    }

    @Override
    public CompletableFuture<Long> logAckOperation(ReceiptHandle receiptHandle,
        long operationTimestamp) {
        long streamId = metadataService.getOperationLogStreamId(receiptHandle.topicId(), receiptHandle.queueId());
        CompletableFuture<AppendResult> append = streamStore.append(streamId, new SingleRecord(new HashMap<>(),
            ByteBuffer.wrap(SerializeUtil.encodeAckOperation(receiptHandle, operationTimestamp))));
        return append.thenApply(AppendResult::baseOffset);
    }

    @Override
    public CompletableFuture<Long> logChangeInvisibleDurationOperation(ReceiptHandle receiptHandle,
        long invisibleDuration, long operationTimestamp) {
        long streamId = metadataService.getOperationLogStreamId(receiptHandle.topicId(), receiptHandle.queueId());
        CompletableFuture<AppendResult> append = streamStore.append(streamId, new SingleRecord(new HashMap<>(),
            ByteBuffer.wrap(SerializeUtil.encodeChangeInvisibleDurationOperation(receiptHandle, invisibleDuration, operationTimestamp))));
        return append.thenApply(AppendResult::baseOffset);
    }
}
