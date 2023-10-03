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

import com.automq.rocketmq.store.model.generated.ChangeInvisibleDurationOperation;
import com.automq.rocketmq.store.model.generated.CheckPoint;
import com.automq.rocketmq.store.model.generated.Operation;
import com.automq.rocketmq.store.model.generated.OperationLogItem;
import com.automq.rocketmq.store.model.generated.ReceiptHandle;
import com.automq.rocketmq.store.model.operation.PopOperation.PopOperationType;
import com.automq.rocketmq.store.model.operation.AckOperation.AckOperationType;
import java.nio.ByteBuffer;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class SerializeUtilTest {
    public static final long TOPIC_ID = 0L;
    public static final int QUEUE_ID = 1;
    public static final long OFFSET = 2L;
    public static final int COUNT = 1;
    public static final long OPERATION_ID = 3L;
    public static final long CONSUMER_GROUP_ID = 4L;
    public static final PopOperationType POP_OPERATION_TYPE = PopOperationType.POP_ORDER;
    public static final AckOperationType ACK_OPERATION_TYPE = AckOperationType.ACK_NORMAL;
    public static final boolean IS_END_MARK = false;
    public static final boolean IS_ORDER = true;
    public static final boolean IS_RETRY = false;
    public static final long DELIVERY_TIMESTAMP = 5L;
    public static final long NEXT_VISIBLE_TIMESTAMP = 6L;
    public static final int DELIVERY_ATTEMPTS = 7;
    public static final int BATCH_SIZE = 8;
    public static final long INVISIBLE_DURATION = 9L;
    public static final long OPERATION_TIMESTAMP = 10L;
    public static final long RETRY_OFFSET = 11L;
    public static final String RECEIPT_HANDLE = "EAAAAAwAGAAIAAAABAAQAAwAAAABAAAABAAAAAAAAAADAAAAAAAAAA==";

    @Test
    void buildCheckPointKey() {
        byte[] key = SerializeUtil.buildCheckPointKey(TOPIC_ID, QUEUE_ID, OPERATION_ID);
        assertEquals(20, key.length);
    }

    @Test
    void buildCheckPointValue() {
        byte[] value = SerializeUtil.buildCheckPointValue(TOPIC_ID, QUEUE_ID, OFFSET, COUNT, CONSUMER_GROUP_ID, OPERATION_ID, POP_OPERATION_TYPE, DELIVERY_TIMESTAMP, NEXT_VISIBLE_TIMESTAMP);
        CheckPoint checkPoint = CheckPoint.getRootAsCheckPoint(ByteBuffer.wrap(value));
        assertEquals(TOPIC_ID, checkPoint.topicId());
        assertEquals(QUEUE_ID, checkPoint.queueId());
        assertEquals(OFFSET, checkPoint.messageOffset());
        assertEquals(COUNT, checkPoint.count());
        assertEquals(CONSUMER_GROUP_ID, checkPoint.consumerGroupId());
        assertEquals(OPERATION_ID, checkPoint.operationId());
        assertEquals(POP_OPERATION_TYPE.value(), checkPoint.popOperationType());
        assertEquals(DELIVERY_TIMESTAMP, checkPoint.deliveryTimestamp());
        assertEquals(NEXT_VISIBLE_TIMESTAMP, checkPoint.nextVisibleTimestamp());
    }

    @Test
    void buildTimerTagKey() {
        byte[] key = SerializeUtil.buildTimerTagKey(NEXT_VISIBLE_TIMESTAMP, TOPIC_ID, QUEUE_ID, OPERATION_ID);
        assertEquals(28, key.length);
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
        String receiptHandle = SerializeUtil.encodeReceiptHandle(CONSUMER_GROUP_ID, TOPIC_ID, QUEUE_ID, OPERATION_ID);
        assertEquals(RECEIPT_HANDLE, receiptHandle);
    }

    @Test
    void decodeReceiptHandle() {
        ReceiptHandle receiptHandle = SerializeUtil.decodeReceiptHandle(RECEIPT_HANDLE);
        assertEquals(CONSUMER_GROUP_ID, receiptHandle.consumerGroupId());
        assertEquals(TOPIC_ID, receiptHandle.topicId());
        assertEquals(QUEUE_ID, receiptHandle.queueId());
        assertEquals(OPERATION_ID, receiptHandle.operationId());
    }

    @Test
    void encodePopOperation() {
        com.automq.rocketmq.store.model.operation.PopOperation popOperation = new com.automq.rocketmq.store.model.operation.PopOperation(
            CONSUMER_GROUP_ID, TOPIC_ID, QUEUE_ID, OFFSET, BATCH_SIZE, INVISIBLE_DURATION, OPERATION_TIMESTAMP, IS_END_MARK, POP_OPERATION_TYPE
        );
        byte[] bytes = SerializeUtil.encodePopOperation(popOperation);
        com.automq.rocketmq.store.model.operation.Operation decodedOperation = SerializeUtil.decodeOperation(ByteBuffer.wrap(bytes));
        assertEquals(popOperation, decodedOperation);
    }

    @Test
    void encodeAckOperation() {
        com.automq.rocketmq.store.model.operation.AckOperation ackOperation = new com.automq.rocketmq.store.model.operation.AckOperation(
            CONSUMER_GROUP_ID, TOPIC_ID, QUEUE_ID, OPERATION_ID, OPERATION_TIMESTAMP, ACK_OPERATION_TYPE
        );
        byte[] bytes = SerializeUtil.encodeAckOperation(ackOperation);
        com.automq.rocketmq.store.model.operation.Operation decodedOperation = SerializeUtil.decodeOperation(ByteBuffer.wrap(bytes));
        assertEquals(ackOperation, decodedOperation);
    }

    @Test
    void encodeChangeInvisibleDurationOperation() {
        byte[] bytes = SerializeUtil.encodeChangeInvisibleDurationOperation(new com.automq.rocketmq.store.model.operation.ChangeInvisibleDurationOperation(
            CONSUMER_GROUP_ID, TOPIC_ID, QUEUE_ID, OPERATION_ID, INVISIBLE_DURATION, OPERATION_TIMESTAMP
        ));
        OperationLogItem operationLogItem = OperationLogItem.getRootAsOperationLogItem(ByteBuffer.wrap(bytes));
        assertEquals(Operation.ChangeInvisibleDurationOperation, operationLogItem.operationType());

        ChangeInvisibleDurationOperation operation = (ChangeInvisibleDurationOperation) operationLogItem.operation(new ChangeInvisibleDurationOperation());
        assertNotNull(operation);
        assertEquals(TOPIC_ID, operation.receiptHandle().topicId());
        assertEquals(QUEUE_ID, operation.receiptHandle().queueId());
        assertEquals(OPERATION_ID, operation.receiptHandle().operationId());
        assertEquals(INVISIBLE_DURATION, operation.invisibleDuration());
        assertEquals(OPERATION_TIMESTAMP, operation.operationTimestamp());
    }
}