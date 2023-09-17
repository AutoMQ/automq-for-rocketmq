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

import com.automq.rocketmq.metadata.StoreMetadataService;
import com.automq.rocketmq.store.StreamStore;
import com.automq.rocketmq.store.impl.StreamStoreImpl;
import com.automq.rocketmq.store.mock.MockStoreMetadataService;
import com.automq.rocketmq.store.model.generated.AckOperation;
import com.automq.rocketmq.store.model.generated.ChangeInvisibleDurationOperation;
import com.automq.rocketmq.store.model.generated.Operation;
import com.automq.rocketmq.store.model.generated.OperationLogItem;
import com.automq.rocketmq.store.model.generated.PopOperation;
import com.automq.rocketmq.store.service.OperationLogService;
import com.automq.rocketmq.store.util.SerializeUtil;
import com.automq.rocketmq.stream.api.FetchResult;
import com.automq.rocketmq.stream.api.RecordBatchWithContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static com.automq.rocketmq.store.util.SerializeUtilTest.BATCH_SIZE;
import static com.automq.rocketmq.store.util.SerializeUtilTest.CONSUMER_GROUP_ID;
import static com.automq.rocketmq.store.util.SerializeUtilTest.INVISIBLE_DURATION;
import static com.automq.rocketmq.store.util.SerializeUtilTest.IS_ORDER;
import static com.automq.rocketmq.store.util.SerializeUtilTest.OFFSET;
import static com.automq.rocketmq.store.util.SerializeUtilTest.OPERATION_TIMESTAMP;
import static com.automq.rocketmq.store.util.SerializeUtilTest.QUEUE_ID;
import static com.automq.rocketmq.store.util.SerializeUtilTest.RECEIPT_HANDLE;
import static com.automq.rocketmq.store.util.SerializeUtilTest.TOPIC_ID;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class StreamOperationLogServiceTest {
    private static StreamStore streamStore;
    private static StoreMetadataService metadataService;
    private static OperationLogService operationLogService;

    @BeforeEach
    public void setUp() {
        metadataService = new MockStoreMetadataService();
        streamStore = new StreamStoreImpl();
        operationLogService = new StreamOperationLogService(streamStore, metadataService);
    }

    @Test
    void logPopOperation() {
        long operationId = operationLogService.logPopOperation(CONSUMER_GROUP_ID, TOPIC_ID, QUEUE_ID, OFFSET, BATCH_SIZE, IS_ORDER, INVISIBLE_DURATION, OPERATION_TIMESTAMP).join();
        long streamId = metadataService.getOperationLogStreamId(TOPIC_ID, QUEUE_ID);
        FetchResult fetchResult = streamStore.fetch(streamId, operationId, 1).join();
        assertEquals(1, fetchResult.recordBatchList().size());

        RecordBatchWithContext recordBatch = fetchResult.recordBatchList().get(0);
        assertEquals(1, recordBatch.count());
        assertEquals(operationId, recordBatch.baseOffset());
        assertEquals(operationId, recordBatch.lastOffset());

        OperationLogItem operationLogItem = OperationLogItem.getRootAsOperationLogItem(recordBatch.rawPayload());
        assertEquals(Operation.PopOperation, operationLogItem.operationType());

        PopOperation operation = (PopOperation) operationLogItem.operation(new PopOperation());
        assertNotNull(operation);
        assertEquals(CONSUMER_GROUP_ID, operation.consumerGroupId());
        assertEquals(TOPIC_ID, operation.topicId());
        assertEquals(QUEUE_ID, operation.queueId());
        assertEquals(OFFSET, operation.offset());
        assertEquals(BATCH_SIZE, operation.batchSize());
        assertEquals(IS_ORDER, operation.isOrder());
        assertEquals(INVISIBLE_DURATION, operation.invisibleDuration());
        assertEquals(OPERATION_TIMESTAMP, operation.operationTimestamp());
    }

    @Test
    void logAckOperation() {
        long operationId = operationLogService.logAckOperation(SerializeUtil.decodeReceiptHandle(RECEIPT_HANDLE), OPERATION_TIMESTAMP).join();
        long streamId = metadataService.getOperationLogStreamId(TOPIC_ID, QUEUE_ID);
        FetchResult fetchResult = streamStore.fetch(streamId, operationId, 1).join();
        assertEquals(1, fetchResult.recordBatchList().size());

        RecordBatchWithContext recordBatch = fetchResult.recordBatchList().get(0);
        assertEquals(1, recordBatch.count());
        assertEquals(operationId, recordBatch.baseOffset());
        assertEquals(operationId, recordBatch.lastOffset());

        OperationLogItem operationLogItem = OperationLogItem.getRootAsOperationLogItem(recordBatch.rawPayload());
        assertEquals(Operation.AckOperation, operationLogItem.operationType());

        AckOperation operation = (AckOperation) operationLogItem.operation(new AckOperation());
        assertNotNull(operation);
        assertEquals(TOPIC_ID, operation.receiptHandle().topicId());
        assertEquals(QUEUE_ID, operation.receiptHandle().queueId());
        assertEquals(OFFSET, operation.receiptHandle().messageOffset());
        assertEquals(OPERATION_TIMESTAMP, operation.operationTimestamp());
    }

    @Test
    void logChangeInvisibleDurationOperation() {
        long operationId = operationLogService.logChangeInvisibleDurationOperation(SerializeUtil.decodeReceiptHandle(RECEIPT_HANDLE), INVISIBLE_DURATION, OPERATION_TIMESTAMP).join();
        long streamId = metadataService.getOperationLogStreamId(TOPIC_ID, QUEUE_ID);
        FetchResult fetchResult = streamStore.fetch(streamId, operationId, 1).join();
        assertEquals(1, fetchResult.recordBatchList().size());

        RecordBatchWithContext recordBatch = fetchResult.recordBatchList().get(0);
        assertEquals(1, recordBatch.count());
        assertEquals(operationId, recordBatch.baseOffset());
        assertEquals(operationId, recordBatch.lastOffset());

        OperationLogItem operationLogItem = OperationLogItem.getRootAsOperationLogItem(recordBatch.rawPayload());
        assertEquals(Operation.ChangeInvisibleDurationOperation, operationLogItem.operationType());
        ChangeInvisibleDurationOperation operation = (ChangeInvisibleDurationOperation) operationLogItem.operation(new ChangeInvisibleDurationOperation());
        assertNotNull(operation);

        assertEquals(TOPIC_ID, operation.receiptHandle().topicId());
        assertEquals(QUEUE_ID, operation.receiptHandle().queueId());
        assertEquals(OFFSET, operation.receiptHandle().messageOffset());
        assertEquals(INVISIBLE_DURATION, operation.invisibleDuration());
        assertEquals(OPERATION_TIMESTAMP, operation.operationTimestamp());
    }
}