/*
 * Copyright 2024, AutoMQ CO.,LTD.
 *
 * Use of this software is governed by the Business Source License
 * included in the file BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

package com.automq.rocketmq.store.util;

import com.automq.rocketmq.common.model.generated.FlatMessage;
import com.automq.rocketmq.store.exception.StoreException;
import com.automq.rocketmq.store.mock.MockMessageUtil;
import com.automq.rocketmq.store.model.generated.CheckPoint;
import com.automq.rocketmq.store.model.generated.ReceiptHandle;
import com.automq.rocketmq.store.model.metadata.ConsumerGroupMetadata;
import com.automq.rocketmq.store.model.operation.AckOperation;
import com.automq.rocketmq.store.model.operation.AckOperation.AckOperationType;
import com.automq.rocketmq.store.model.operation.ChangeInvisibleDurationOperation;
import com.automq.rocketmq.store.model.operation.Operation;
import com.automq.rocketmq.store.model.operation.OperationSnapshot;
import com.automq.rocketmq.store.model.operation.PopOperation;
import com.automq.rocketmq.store.model.operation.PopOperation.PopOperationType;
import com.automq.rocketmq.store.model.operation.ResetConsumeOffsetOperation;
import java.nio.ByteBuffer;
import java.util.List;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

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
    public static final int CONSUMER_GROUP_VERSION = 13;
    public static final String RECEIPT_HANDLE = "EAAAAAwAGAAIAAAABAAQAAwAAAABAAAABAAAAAAAAAADAAAAAAAAAA==";

    @Test
    void flatBufferToByteArray() {
        FlatMessage message = FlatMessage.getRootAsFlatMessage(MockMessageUtil.buildMessage());
        byte[] bytes = SerializeUtil.flatBufferToByteArray(message);

        FlatMessage message1 = FlatMessage.getRootAsFlatMessage(ByteBuffer.wrap(bytes));
        assertEquals(message1.topicId(), message.topicId());
        assertEquals(message1.keys(), message.keys());
        assertNotNull(message1.systemProperties());
        assertEquals(message1.systemProperties().deliveryAttempts(), message.systemProperties().deliveryAttempts());
        assertEquals(message1.systemProperties().originalQueueOffset(), message.systemProperties().originalQueueOffset());
    }

    @Test
    void buildCheckPointKey() {
        byte[] key = SerializeUtil.buildCheckPointKey(TOPIC_ID, QUEUE_ID, CONSUMER_GROUP_ID, OPERATION_ID);
        assertEquals(28, key.length);
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
        byte[] key = SerializeUtil.buildReceiptHandleKey(TOPIC_ID, QUEUE_ID, OPERATION_ID);
        assertEquals(20, key.length);
    }

    @Test
    void buildOrderIndexKey() {
        byte[] key = SerializeUtil.buildOrderIndexKey(CONSUMER_GROUP_ID, TOPIC_ID, QUEUE_ID, OFFSET);
        assertEquals(28, key.length);
    }

    @Test
    void buildOrderIndexValue() {
        byte[] value = SerializeUtil.buildOrderIndexValue(OPERATION_ID, QUEUE_ID);
        assertEquals(12, value.length);
        assertEquals(QUEUE_ID, ByteBuffer.wrap(value).getInt(8));
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
    void encodePopOperation() throws StoreException {
        com.automq.rocketmq.store.model.operation.PopOperation popOperation = new com.automq.rocketmq.store.model.operation.PopOperation(
            TOPIC_ID, QUEUE_ID, OPERATION_STREAM_ID, SNAPSHOT_STREAM_ID, null, CONSUMER_GROUP_ID, OFFSET,
            BATCH_SIZE, INVISIBLE_DURATION, OPERATION_TIMESTAMP, IS_END_MARK, POP_OPERATION_TYPE
        );
        byte[] bytes = SerializeUtil.encodePopOperation(popOperation);
        com.automq.rocketmq.store.model.operation.Operation decodedOperation = SerializeUtil.decodeOperation(ByteBuffer.wrap(bytes), null, OPERATION_STREAM_ID, SNAPSHOT_STREAM_ID);
        assertEquals(popOperation, decodedOperation);
    }

    @Test
    void encodeAckOperation() throws StoreException {
        com.automq.rocketmq.store.model.operation.AckOperation ackOperation = new com.automq.rocketmq.store.model.operation.AckOperation(
            TOPIC_ID, QUEUE_ID, OPERATION_STREAM_ID, SNAPSHOT_STREAM_ID, null, CONSUMER_GROUP_ID,
            OPERATION_ID, OPERATION_TIMESTAMP, ACK_OPERATION_TYPE
        );
        byte[] bytes = SerializeUtil.encodeAckOperation(ackOperation);
        com.automq.rocketmq.store.model.operation.Operation decodedOperation = SerializeUtil.decodeOperation(ByteBuffer.wrap(bytes), null, OPERATION_STREAM_ID, SNAPSHOT_STREAM_ID);
        assertEquals(ackOperation, decodedOperation);
    }

    @Test
    void encodeChangeInvisibleDurationOperation() throws StoreException {
        ChangeInvisibleDurationOperation changeInvisibleDurationOperation = new ChangeInvisibleDurationOperation(
            TOPIC_ID, QUEUE_ID, OPERATION_STREAM_ID, SNAPSHOT_STREAM_ID, null, CONSUMER_GROUP_ID, OPERATION_ID, INVISIBLE_DURATION, OPERATION_TIMESTAMP
        );
        byte[] bytes = SerializeUtil.encodeChangeInvisibleDurationOperation(changeInvisibleDurationOperation);
        com.automq.rocketmq.store.model.operation.Operation decodedOperation = SerializeUtil.decodeOperation(ByteBuffer.wrap(bytes), null, OPERATION_STREAM_ID, SNAPSHOT_STREAM_ID);
        assertEquals(changeInvisibleDurationOperation, decodedOperation);
    }

    @Test
    void encodeResetConsumerOffset() throws StoreException {
        ResetConsumeOffsetOperation resetConsumeOffsetOperation = new ResetConsumeOffsetOperation(
            TOPIC_ID, QUEUE_ID, OPERATION_STREAM_ID, SNAPSHOT_STREAM_ID, null, CONSUMER_GROUP_ID, 0, OPERATION_TIMESTAMP
        );
        byte[] bytes = SerializeUtil.encodeResetConsumeOffsetOperation(resetConsumeOffsetOperation);
        com.automq.rocketmq.store.model.operation.Operation decodedOperation = SerializeUtil.decodeOperation(ByteBuffer.wrap(bytes), null, OPERATION_STREAM_ID, SNAPSHOT_STREAM_ID);
        assertEquals(resetConsumeOffsetOperation, decodedOperation);
    }

    @Test
    void encodeOperationSnapshot() {
        ConsumerGroupMetadata consumerGroupMetadataSnapshot = new ConsumerGroupMetadata(
            CONSUMER_GROUP_ID, 1, 2, 3, 4, CONSUMER_GROUP_VERSION);
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
    }

    @Test
    public void testAckOperationCodec() throws StoreException {
        AckOperation op = new AckOperation(1, 2, 3, 4, null,
            5, 6, 7, AckOperationType.ACK_NORMAL);
        byte[] data = SerializeUtil.encodeAckOperation(op);
        Operation ops = SerializeUtil.decodeOperation(ByteBuffer.wrap(data), null, 3, 4);
        assertBasicOps(ops);
        Assertions.assertTrue(ops instanceof AckOperation);
        AckOperation ackOps = (AckOperation) ops;
        Assertions.assertEquals(ackOps.consumerGroupId(), 5);
        Assertions.assertEquals(ackOps.operationId(), 6);
        Assertions.assertEquals(ackOps.ackOperationType(), AckOperationType.ACK_NORMAL);
    }

    private void assertBasicOps(Operation ops) {
        Assertions.assertEquals(ops.topicId(), 1);
        Assertions.assertEquals(ops.queueId(), 2);
        Assertions.assertEquals(ops.operationStreamId(), 3);
        Assertions.assertEquals(ops.snapshotStreamId(), 4);
        Assertions.assertEquals(ops.operationTimestamp(), 7);
    }

    @Test
    public void testChangeInvisibleTimeOperationCodec() throws StoreException {
        ChangeInvisibleDurationOperation op = new ChangeInvisibleDurationOperation(1, 2, 3,
            4, null, 5, 6, 8, 7);
        byte[] data = SerializeUtil.encodeChangeInvisibleDurationOperation(op);
        Operation ops = SerializeUtil.decodeOperation(ByteBuffer.wrap(data), null, 3, 4);
        assertBasicOps(ops);
        Assertions.assertTrue(ops instanceof ChangeInvisibleDurationOperation);
        ChangeInvisibleDurationOperation cOps = (ChangeInvisibleDurationOperation) ops;
        Assertions.assertEquals(cOps.consumerGroupId(), 5);
        Assertions.assertEquals(cOps.operationId(), 6);
        Assertions.assertEquals(cOps.invisibleDuration(), 8);
    }

    @Test
    public void testPopOperationCodec() throws StoreException {
        PopOperation op = new PopOperation(1, 2, 3, 4, null, 5, 6,
            8, 9, 7, true, PopOperationType.POP_ORDER);
        byte[] data = SerializeUtil.encodePopOperation(op);
        Operation ops = SerializeUtil.decodeOperation(ByteBuffer.wrap(data), null, 3, 4);
        assertBasicOps(ops);
        Assertions.assertTrue(ops instanceof PopOperation);
        PopOperation pOps = (PopOperation) ops;
        Assertions.assertEquals(5, pOps.consumerGroupId());
        Assertions.assertEquals(6, pOps.offset());
        Assertions.assertEquals(8, pOps.count());
        Assertions.assertEquals(9, pOps.invisibleDuration());
        Assertions.assertTrue(pOps.isEndMark());
        Assertions.assertEquals(PopOperationType.POP_ORDER, pOps.popOperationType());
    }

    @Test
    public void testResetConsumeOffsetOperationCodec() throws StoreException {
        ResetConsumeOffsetOperation op = new ResetConsumeOffsetOperation(1, 2, 3,
            4, null, 5, 6, 7);
        byte[] data = SerializeUtil.encodeResetConsumeOffsetOperation(op);
        Operation ops = SerializeUtil.decodeOperation(ByteBuffer.wrap(data), null, 3, 4);
        assertBasicOps(ops);

        Assertions.assertTrue(ops instanceof ResetConsumeOffsetOperation);
        ResetConsumeOffsetOperation rOps = (ResetConsumeOffsetOperation) ops;
        Assertions.assertEquals(5, rOps.consumerGroupId());
        Assertions.assertEquals(6, rOps.offset());
    }
}