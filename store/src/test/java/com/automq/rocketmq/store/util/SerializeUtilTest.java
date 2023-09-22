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

package com.automq.rocketmq.store.util;

import com.automq.rocketmq.store.model.generated.AckOperation;
import com.automq.rocketmq.store.model.generated.ChangeInvisibleDurationOperation;
import com.automq.rocketmq.store.model.generated.CheckPoint;
import com.automq.rocketmq.store.model.generated.Operation;
import com.automq.rocketmq.store.model.generated.OperationLogItem;
import com.automq.rocketmq.store.model.generated.PopOperation;
import com.automq.rocketmq.store.model.generated.ReceiptHandle;
import java.nio.ByteBuffer;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class SerializeUtilTest {
    public static final long TOPIC_ID = 0L;
    public static final int QUEUE_ID = 1;
    public static final long OFFSET = 2L;
    public static final long OPERATION_ID = 3L;
    public static final long CONSUMER_GROUP_ID = 4L;
    public static final boolean IS_ORDER = true;
    public static final boolean IS_RETRY = false;
    public static final long DELIVERY_TIMESTAMP = 5L;
    public static final long NEXT_VISIBLE_TIMESTAMP = 6L;
    public static final int RECONSUME_COUNT = 7;
    public static final int BATCH_SIZE = 8;
    public static final long INVISIBLE_DURATION = 9L;
    public static final long OPERATION_TIMESTAMP = 10L;
    public static final String RECEIPT_HANDLE = "EAAAAAwAGAAAAAQACAAQAAwAAAABAAAAAgAAAAAAAAADAAAAAAAAAA==";

    @Test
    void buildCheckPointKey() {
        byte[] key = SerializeUtil.buildCheckPointKey(TOPIC_ID, QUEUE_ID, OFFSET, OPERATION_ID);
        assertEquals(28, key.length);
    }

    @Test
    void buildCheckPointValue() {
        byte[] value = SerializeUtil.buildCheckPointValue(TOPIC_ID, QUEUE_ID, OFFSET, CONSUMER_GROUP_ID, OPERATION_ID, IS_ORDER, IS_RETRY, DELIVERY_TIMESTAMP, NEXT_VISIBLE_TIMESTAMP, RECONSUME_COUNT);
        CheckPoint checkPoint = CheckPoint.getRootAsCheckPoint(ByteBuffer.wrap(value));
        assertEquals(TOPIC_ID, checkPoint.topicId());
        assertEquals(QUEUE_ID, checkPoint.queueId());
        assertEquals(OFFSET, checkPoint.messageOffset());
        assertEquals(CONSUMER_GROUP_ID, checkPoint.consumerGroupId());
        assertEquals(OPERATION_ID, checkPoint.operationId());
        assertEquals(IS_ORDER, checkPoint.fifo());
        assertEquals(DELIVERY_TIMESTAMP, checkPoint.deliveryTimestamp());
        assertEquals(NEXT_VISIBLE_TIMESTAMP, checkPoint.nextVisibleTimestamp());
        assertEquals(RECONSUME_COUNT, checkPoint.reconsumeCount());
    }

    @Test
    void buildTimerTagKey() {
        byte[] key = SerializeUtil.buildTimerTagKey(NEXT_VISIBLE_TIMESTAMP, TOPIC_ID, QUEUE_ID, OFFSET, OPERATION_ID);
        assertEquals(36, key.length);
    }

    @Test
    void buildOrderIndexKey() {
        byte[] key = SerializeUtil.buildOrderIndexKey(CONSUMER_GROUP_ID, TOPIC_ID, QUEUE_ID, OFFSET);
        assertEquals(28, key.length);
    }

    @Test
    void buildOrderIndexValue() {
        byte[] value = SerializeUtil.buildOrderIndexValue(OPERATION_ID);
        assertEquals(8, value.length);
    }

    @Test
    void encodeReceiptHandle() {
        String receiptHandle = SerializeUtil.encodeReceiptHandle(TOPIC_ID, QUEUE_ID, OFFSET, OPERATION_ID);
        assertEquals(RECEIPT_HANDLE, receiptHandle);
    }

    @Test
    void decodeReceiptHandle() {
        ReceiptHandle receiptHandle = SerializeUtil.decodeReceiptHandle(RECEIPT_HANDLE);
        assertEquals(TOPIC_ID, receiptHandle.topicId());
        assertEquals(QUEUE_ID, receiptHandle.queueId());
        assertEquals(OFFSET, receiptHandle.messageOffset());
        assertEquals(OPERATION_ID, receiptHandle.operationId());
    }

    @Test
    void encodePopOperation() {
        byte[] bytes = SerializeUtil.encodePopOperation(CONSUMER_GROUP_ID, TOPIC_ID, QUEUE_ID, OFFSET, BATCH_SIZE, IS_ORDER, INVISIBLE_DURATION, OPERATION_TIMESTAMP);
        OperationLogItem operationLogItem = OperationLogItem.getRootAsOperationLogItem(ByteBuffer.wrap(bytes));
        assertEquals(Operation.PopOperation, operationLogItem.operationType());

        PopOperation operation = (PopOperation) operationLogItem.operation(new PopOperation());
        assertNotNull(operation);
        assertEquals(CONSUMER_GROUP_ID, operation.consumerGroupId());
        assertEquals(TOPIC_ID, operation.topicId());
        assertEquals(QUEUE_ID, operation.queueId());
        assertEquals(OFFSET, operation.offset());
        assertEquals(BATCH_SIZE, operation.batchSize());
        assertEquals(IS_ORDER, operation.fifo());
        assertEquals(INVISIBLE_DURATION, operation.invisibleDuration());
        assertEquals(OPERATION_TIMESTAMP, operation.operationTimestamp());
    }

    @Test
    void encodeAckOperation() {
        byte[] bytes = SerializeUtil.encodeAckOperation(SerializeUtil.decodeReceiptHandle(RECEIPT_HANDLE), OPERATION_TIMESTAMP);
        OperationLogItem operationLogItem = OperationLogItem.getRootAsOperationLogItem(ByteBuffer.wrap(bytes));
        assertEquals(Operation.AckOperation, operationLogItem.operationType());

        AckOperation operation = (AckOperation) operationLogItem.operation(new AckOperation());
        assertNotNull(operation);
        assertEquals(TOPIC_ID, operation.receiptHandle().topicId());
        assertEquals(QUEUE_ID, operation.receiptHandle().queueId());
        assertEquals(OFFSET, operation.receiptHandle().messageOffset());
        assertEquals(OPERATION_ID, operation.receiptHandle().operationId());
        assertEquals(OPERATION_TIMESTAMP, operation.operationTimestamp());
    }

    @Test
    void encodeChangeInvisibleDurationOperation() {
        byte[] bytes = SerializeUtil.encodeChangeInvisibleDurationOperation(SerializeUtil.decodeReceiptHandle(RECEIPT_HANDLE), INVISIBLE_DURATION, OPERATION_TIMESTAMP);
        OperationLogItem operationLogItem = OperationLogItem.getRootAsOperationLogItem(ByteBuffer.wrap(bytes));
        assertEquals(Operation.ChangeInvisibleDurationOperation, operationLogItem.operationType());

        ChangeInvisibleDurationOperation operation = (ChangeInvisibleDurationOperation) operationLogItem.operation(new ChangeInvisibleDurationOperation());
        assertNotNull(operation);
        assertEquals(TOPIC_ID, operation.receiptHandle().topicId());
        assertEquals(QUEUE_ID, operation.receiptHandle().queueId());
        assertEquals(OFFSET, operation.receiptHandle().messageOffset());
        assertEquals(OPERATION_ID, operation.receiptHandle().operationId());
        assertEquals(INVISIBLE_DURATION, operation.invisibleDuration());
        assertEquals(OPERATION_TIMESTAMP, operation.operationTimestamp());
    }
}