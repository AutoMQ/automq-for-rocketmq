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
import com.automq.rocketmq.store.model.generated.PopOperation;
import com.automq.rocketmq.store.model.generated.ReceiptHandle;
import java.nio.ByteBuffer;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class SerializeUtilTest {
    private static final long TOPIC_ID = 0L;
    private static final int QUEUE_ID = 1;
    private static final long OFFSET = 2L;
    private static final long OPERATION_ID = 3L;
    private static final long CONSUME_GROUP_ID = 4L;
    private static final boolean IS_ORDER = true;
    private static final long DELIVERY_TIMESTAMP = 5L;
    private static final long NEXT_VISIBLE_TIMESTAMP = 6L;
    private static final int RECONSUME_COUNT = 7;
    private static final int BATCH_SIZE = 8;
    private static final long INVISIBLE_DURATION = 9L;
    private static final long OPERATION_TIMESTAMP = 10L;
    private static final String RECEIPT_HANDLE = "EAAAAAwAGAAAAAQACAAQAAwAAAABAAAAAgAAAAAAAAADAAAAAAAAAA==";

    @Test
    void buildCheckPointKey() {
        byte[] key = SerializeUtil.buildCheckPointKey(TOPIC_ID, QUEUE_ID, OFFSET, OPERATION_ID);
        assertEquals(28, key.length);
    }

    @Test
    void buildCheckPointValue() {
        byte[] value = SerializeUtil.buildCheckPointValue(TOPIC_ID, QUEUE_ID, OFFSET, CONSUME_GROUP_ID, OPERATION_ID, IS_ORDER, DELIVERY_TIMESTAMP, NEXT_VISIBLE_TIMESTAMP, RECONSUME_COUNT);
        CheckPoint checkPoint = CheckPoint.getRootAsCheckPoint(ByteBuffer.wrap(value));
        assertEquals(TOPIC_ID, checkPoint.topicId());
        assertEquals(QUEUE_ID, checkPoint.queueId());
        assertEquals(OFFSET, checkPoint.messageOffset());
        assertEquals(CONSUME_GROUP_ID, checkPoint.consumerGroupId());
        assertEquals(OPERATION_ID, checkPoint.operationId());
        assertEquals(IS_ORDER, checkPoint.isOrder());
        assertEquals(DELIVERY_TIMESTAMP, checkPoint.deliveryTimestamp());
        assertEquals(NEXT_VISIBLE_TIMESTAMP, checkPoint.nextVisibleTimestamp());
        assertEquals(RECONSUME_COUNT, checkPoint.reconsumeCount());
    }

    @Test
    void buildTimerTagKey() {
        byte[] key = SerializeUtil.buildTimerTagKey(NEXT_VISIBLE_TIMESTAMP, TOPIC_ID, QUEUE_ID, OPERATION_ID);
        assertEquals(28, key.length);
    }

    @Test
    void buildOrderIndexKey() {
        byte[] key = SerializeUtil.buildOrderIndexKey(CONSUME_GROUP_ID, TOPIC_ID, QUEUE_ID, OFFSET);
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
        byte[] bytes = SerializeUtil.encodePopOperation(CONSUME_GROUP_ID, TOPIC_ID, QUEUE_ID, OFFSET, BATCH_SIZE, IS_ORDER, INVISIBLE_DURATION, OPERATION_TIMESTAMP);
        PopOperation operation = PopOperation.getRootAsPopOperation(ByteBuffer.wrap(bytes));
        assertEquals(CONSUME_GROUP_ID, operation.consumerGroupId());
        assertEquals(TOPIC_ID, operation.topicId());
        assertEquals(QUEUE_ID, operation.queueId());
        assertEquals(OFFSET, operation.offset());
        assertEquals(BATCH_SIZE, operation.batchSize());
        assertEquals(IS_ORDER, operation.isOrder());
        assertEquals(INVISIBLE_DURATION, operation.invisibleDuration());
        assertEquals(OPERATION_TIMESTAMP, operation.operationTimestamp());
    }

    @Test
    void encodeAckOperation() {
        byte[] bytes = SerializeUtil.encodeAckOperation(SerializeUtil.decodeReceiptHandle(RECEIPT_HANDLE), OPERATION_TIMESTAMP);
        AckOperation operation = AckOperation.getRootAsAckOperation(ByteBuffer.wrap(bytes));
        assertEquals(TOPIC_ID, operation.receiptHandle().topicId());
        assertEquals(QUEUE_ID, operation.receiptHandle().queueId());
        assertEquals(OFFSET, operation.receiptHandle().messageOffset());
        assertEquals(OPERATION_ID, operation.receiptHandle().operationId());
        assertEquals(OPERATION_TIMESTAMP, operation.operationTimestamp());
    }

    @Test
    void encodeChangeInvisibleDurationOperation() {
        byte[] bytes = SerializeUtil.encodeChangeInvisibleDurationOperation(SerializeUtil.decodeReceiptHandle(RECEIPT_HANDLE), INVISIBLE_DURATION, OPERATION_TIMESTAMP);
        ChangeInvisibleDurationOperation operation = ChangeInvisibleDurationOperation.getRootAsChangeInvisibleDurationOperation(ByteBuffer.wrap(bytes));
        assertEquals(TOPIC_ID, operation.receiptHandle().topicId());
        assertEquals(QUEUE_ID, operation.receiptHandle().queueId());
        assertEquals(OFFSET, operation.receiptHandle().messageOffset());
        assertEquals(OPERATION_ID, operation.receiptHandle().operationId());
        assertEquals(INVISIBLE_DURATION, operation.invisibleDuration());
        assertEquals(OPERATION_TIMESTAMP, operation.operationTimestamp());
    }
}