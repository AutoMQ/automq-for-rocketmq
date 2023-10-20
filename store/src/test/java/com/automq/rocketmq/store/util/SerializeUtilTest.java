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

import com.automq.rocketmq.store.model.generated.CheckPoint;
import com.automq.rocketmq.store.model.generated.ReceiptHandle;
import com.automq.rocketmq.store.model.operation.AckOperation.AckOperationType;
import com.automq.rocketmq.store.model.operation.ChangeInvisibleDurationOperation;
import com.automq.rocketmq.store.model.operation.OperationSnapshot;
import com.automq.rocketmq.store.model.operation.PopOperation.PopOperationType;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.ConcurrentSkipListMap;
import org.junit.jupiter.api.Test;
import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class SerializeUtilTest {
    public static final long TOPIC_ID = 0L;
    public static final int QUEUE_ID = 1;
    public static final long OPERATION_STREAM_ID = 100L;
    public static final long SNAPSHOT_STREAM_ID = 101L;
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
            TOPIC_ID, QUEUE_ID, OPERATION_STREAM_ID, SNAPSHOT_STREAM_ID, null, CONSUMER_GROUP_ID, OFFSET,
            BATCH_SIZE, INVISIBLE_DURATION, OPERATION_TIMESTAMP, IS_END_MARK, POP_OPERATION_TYPE
        );
        byte[] bytes = SerializeUtil.encodePopOperation(popOperation);
        com.automq.rocketmq.store.model.operation.Operation decodedOperation = SerializeUtil.decodeOperation(ByteBuffer.wrap(bytes), null, OPERATION_STREAM_ID, SNAPSHOT_STREAM_ID);
        assertEquals(popOperation, decodedOperation);
    }

    @Test
    void encodeAckOperation() {
        com.automq.rocketmq.store.model.operation.AckOperation ackOperation = new com.automq.rocketmq.store.model.operation.AckOperation(
            TOPIC_ID, QUEUE_ID, OPERATION_STREAM_ID, SNAPSHOT_STREAM_ID, null, CONSUMER_GROUP_ID,
            OPERATION_ID, OPERATION_TIMESTAMP, ACK_OPERATION_TYPE
        );
        byte[] bytes = SerializeUtil.encodeAckOperation(ackOperation);
        com.automq.rocketmq.store.model.operation.Operation decodedOperation = SerializeUtil.decodeOperation(ByteBuffer.wrap(bytes), null, OPERATION_STREAM_ID, SNAPSHOT_STREAM_ID);
        assertEquals(ackOperation, decodedOperation);
    }

    @Test
    void encodeChangeInvisibleDurationOperation() {
        ChangeInvisibleDurationOperation changeInvisibleDurationOperation = new ChangeInvisibleDurationOperation(
            TOPIC_ID, QUEUE_ID, OPERATION_STREAM_ID, SNAPSHOT_STREAM_ID, null, CONSUMER_GROUP_ID, OPERATION_ID, INVISIBLE_DURATION, OPERATION_TIMESTAMP
        );
        byte[] bytes = SerializeUtil.encodeChangeInvisibleDurationOperation(changeInvisibleDurationOperation);
        com.automq.rocketmq.store.model.operation.Operation decodedOperation = SerializeUtil.decodeOperation(ByteBuffer.wrap(bytes), null, OPERATION_STREAM_ID, SNAPSHOT_STREAM_ID);
        assertEquals(changeInvisibleDurationOperation, decodedOperation);
    }

    @Test
    void encodeOperationSnapshot() {
        ConcurrentSkipListMap<Long, Integer> consumeTimes = new ConcurrentSkipListMap<>();
        consumeTimes.put(1L, 2);
        consumeTimes.put(3L, 4);
        consumeTimes.put(5L, 6);
        RoaringBitmap ackBitmap = new RoaringBitmap();
        ackBitmap.add(1, 3, 5);
        int ackBitmapLength = ackBitmap.serializedSizeInBytes();
        ByteBuffer ackBitmapBuffer = ByteBuffer.allocate(ackBitmapLength);
        ackBitmap.serialize(ackBitmapBuffer);
        ackBitmapBuffer.flip();

        RoaringBitmap retryAckBitmap = new RoaringBitmap();
        retryAckBitmap.add(2, 4, 6);
        int retryAckBitmapLength = retryAckBitmap.serializedSizeInBytes();
        ByteBuffer retryAckBitmapBuffer = ByteBuffer.allocate(retryAckBitmapLength);
        retryAckBitmap.serialize(retryAckBitmapBuffer);
        retryAckBitmapBuffer.flip();

        OperationSnapshot.ConsumerGroupMetadataSnapshot consumerGroupMetadataSnapshot = new OperationSnapshot.ConsumerGroupMetadataSnapshot(
            CONSUMER_GROUP_ID, 1, 2, 3, 4, ackBitmapBuffer.array(), retryAckBitmapBuffer.array(),
            consumeTimes
        );
        byte[] checkPointValue = SerializeUtil.buildCheckPointValue(TOPIC_ID, QUEUE_ID, OFFSET, COUNT, CONSUMER_GROUP_ID, OPERATION_ID, POP_OPERATION_TYPE, DELIVERY_TIMESTAMP, NEXT_VISIBLE_TIMESTAMP);
        CheckPoint checkPoint = CheckPoint.getRootAsCheckPoint(ByteBuffer.wrap(checkPointValue));
        OperationSnapshot operationSnapshot = new OperationSnapshot(
            13, List.of(consumerGroupMetadataSnapshot), List.of(checkPoint)
        );
        byte[] bytes = SerializeUtil.encodeOperationSnapshot(operationSnapshot);
        OperationSnapshot decodedOperationSnapshot = SerializeUtil.decodeOperationSnapshot(ByteBuffer.wrap(bytes));
        assertEquals(operationSnapshot.getSnapshotEndOffset(), decodedOperationSnapshot.getSnapshotEndOffset());
        assertEquals(operationSnapshot.getConsumerGroupMetadataList().get(0).getConsumerGroupId(), decodedOperationSnapshot.getConsumerGroupMetadataList().get(0).getConsumerGroupId());
        assertEquals(operationSnapshot.getConsumerGroupMetadataList().get(0).getConsumeOffset(), decodedOperationSnapshot.getConsumerGroupMetadataList().get(0).getConsumeOffset());
        assertEquals(operationSnapshot.getConsumerGroupMetadataList().get(0).getRetryConsumeOffset(), decodedOperationSnapshot.getConsumerGroupMetadataList().get(0).getRetryConsumeOffset());
        assertEquals(operationSnapshot.getConsumerGroupMetadataList().get(0).getAckOffset(), decodedOperationSnapshot.getConsumerGroupMetadataList().get(0).getAckOffset());
        assertEquals(operationSnapshot.getConsumerGroupMetadataList().get(0).getRetryAckOffset(), decodedOperationSnapshot.getConsumerGroupMetadataList().get(0).getRetryAckOffset());

        assertEquals(operationSnapshot.getCheckPoints().get(0).operationId(), decodedOperationSnapshot.getCheckPoints().get(0).operationId());
        assertEquals(operationSnapshot.getCheckPoints().get(0).topicId(), decodedOperationSnapshot.getCheckPoints().get(0).topicId());
        assertEquals(operationSnapshot.getCheckPoints().get(0).queueId(), decodedOperationSnapshot.getCheckPoints().get(0).queueId());
        assertEquals(operationSnapshot.getCheckPoints().get(0).messageOffset(), decodedOperationSnapshot.getCheckPoints().get(0).messageOffset());
        assertEquals(operationSnapshot.getCheckPoints().get(0).count(), decodedOperationSnapshot.getCheckPoints().get(0).count());
        assertEquals(operationSnapshot.getCheckPoints().get(0).consumerGroupId(), decodedOperationSnapshot.getCheckPoints().get(0).consumerGroupId());
        assertEquals(operationSnapshot.getCheckPoints().get(0).popOperationType(), decodedOperationSnapshot.getCheckPoints().get(0).popOperationType());
        assertEquals(operationSnapshot.getCheckPoints().get(0).deliveryTimestamp(), decodedOperationSnapshot.getCheckPoints().get(0).deliveryTimestamp());
        assertEquals(operationSnapshot.getCheckPoints().get(0).nextVisibleTimestamp(), decodedOperationSnapshot.getCheckPoints().get(0).nextVisibleTimestamp());

        assertEquals(operationSnapshot.getConsumerGroupMetadataList(), decodedOperationSnapshot.getConsumerGroupMetadataList());
        byte[] decodedAckBitmapBuffer = decodedOperationSnapshot.getConsumerGroupMetadataList().get(0).getAckOffsetBitmapBuffer();
        RoaringBitmap decodedAckBitmap = new RoaringBitmap(new ImmutableRoaringBitmap(ByteBuffer.wrap(decodedAckBitmapBuffer)));
        assertEquals(ackBitmap, decodedAckBitmap);
        byte[] decodedRetryAckBitmapBuffer = decodedOperationSnapshot.getConsumerGroupMetadataList().get(0).getRetryAckOffsetBitmapBuffer();
        RoaringBitmap decodedRetryAckBitmap = new RoaringBitmap(new ImmutableRoaringBitmap(ByteBuffer.wrap(decodedRetryAckBitmapBuffer)));
        assertEquals(retryAckBitmap, decodedRetryAckBitmap);
        assertEquals(operationSnapshot.getConsumerGroupMetadataList().get(0).getConsumeTimes(), decodedOperationSnapshot.getConsumerGroupMetadataList().get(0).getConsumeTimes());
    }
}