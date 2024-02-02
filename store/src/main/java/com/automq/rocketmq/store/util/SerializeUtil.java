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

import com.automq.rocketmq.store.api.MessageStateMachine;
import com.automq.rocketmq.store.exception.StoreErrorCode;
import com.automq.rocketmq.store.exception.StoreException;
import com.automq.rocketmq.store.model.generated.CheckPoint;
import com.automq.rocketmq.store.model.generated.OperationLogItem;
import com.automq.rocketmq.store.model.generated.ReceiptHandle;
import com.automq.rocketmq.store.model.metadata.ConsumerGroupMetadata;
import com.automq.rocketmq.store.model.operation.AckOperation;
import com.automq.rocketmq.store.model.operation.ChangeInvisibleDurationOperation;
import com.automq.rocketmq.store.model.operation.Operation;
import com.automq.rocketmq.store.model.operation.OperationSnapshot;
import com.automq.rocketmq.store.model.operation.PopOperation;
import com.automq.rocketmq.store.model.operation.ResetConsumeOffsetOperation;
import com.automq.stream.s3.wal.util.WALUtil;
import com.google.flatbuffers.FlatBufferBuilder;
import com.google.flatbuffers.Table;
import io.netty.buffer.Unpooled;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SerializeUtil {

    private static final Logger LOGGER = LoggerFactory.getLogger(SerializeUtil.class);

    public static <T extends Table> byte[] flatBufferToByteArray(T table) {
        ByteBuffer buffer = table.getByteBuffer();
        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(buffer.position(), bytes);
        return bytes;
    }

    // <topicId><queueId>
    public static byte[] buildCheckPointQueuePrefix(long topicId, int queueId) {
        ByteBuffer buffer = ByteBuffer.allocate(12);
        buffer.putLong(topicId);
        buffer.putInt(queueId);
        return buffer.array();
    }

    // <topicId><queueId><consumerGroupId>
    public static byte[] buildCheckPointGroupPrefix(long topicId, int queueId, long consumerGroupId) {
        ByteBuffer buffer = ByteBuffer.allocate(20);
        buffer.putLong(topicId);
        buffer.putInt(queueId);
        buffer.putLong(consumerGroupId);
        return buffer.array();
    }

    // <topicId><queueId><consumerGroupId><operationId>
    public static byte[] buildCheckPointKey(long topicId, int queueId, long consumerGroupId, long operationId) {
        ByteBuffer buffer = ByteBuffer.allocate(28);
        buffer.putLong(topicId);
        buffer.putInt(queueId);
        buffer.putLong(consumerGroupId);
        buffer.putLong(operationId);
        return buffer.array();
    }

    public static byte[] buildCheckPointValue(long topicId, int queueId, long offset, int count, long consumerGroupId,
        long operationId, PopOperation.PopOperationType operationType, long deliveryTimestamp,
        long nextVisibleTimestamp) {
        FlatBufferBuilder builder = new FlatBufferBuilder();
        int root = CheckPoint.createCheckPoint(builder, topicId, queueId, offset, count, consumerGroupId, operationId, operationType.value(), deliveryTimestamp, nextVisibleTimestamp);
        builder.finish(root);
        return builder.sizedByteArray();
    }

    // <topicId><queueId><operationId>
    public static byte[] buildReceiptHandleKey(long topicId, int queueId, long operationId) {
        ByteBuffer buffer = ByteBuffer.allocate(20);
        buffer.putLong(topicId);
        buffer.putInt(queueId);
        buffer.putLong(operationId);
        return buffer.array();
    }

    // <groupId><topicId><queueId><offset>
    public static byte[] buildOrderIndexKey(long consumerGroupId, long topicId, int queueId, long offset) {
        ByteBuffer buffer = ByteBuffer.allocate(28);
        buffer.putLong(consumerGroupId);
        buffer.putLong(topicId);
        buffer.putInt(queueId);
        buffer.putLong(offset);
        return buffer.array();
    }

    // <operationId>
    public static byte[] buildOrderIndexValue(long operationId, int consumeTimes) {
        ByteBuffer buffer = ByteBuffer.allocate(12);
        buffer.putLong(operationId);
        buffer.putInt(consumeTimes);
        return buffer.array();
    }

    public static CheckPoint decodeCheckPoint(ByteBuffer buffer) {
        return CheckPoint.getRootAsCheckPoint(buffer);
    }

    public static String encodeReceiptHandle(long consumerGroupId, long topicId, int queueId, long operationId) {
        return new String(Base64.getEncoder().encode(buildReceiptHandle(consumerGroupId, topicId, queueId, operationId)));
    }

    public static String encodeReceiptHandle(ReceiptHandle receiptHandle) {
        return new String(Base64.getEncoder().encode(buildReceiptHandle(receiptHandle.consumerGroupId(), receiptHandle.topicId(),
            receiptHandle.queueId(), receiptHandle.operationId())));
    }

    public static byte[] buildReceiptHandle(long consumerGroupId, long topicId, int queueId, long operationId) {
        FlatBufferBuilder builder = new FlatBufferBuilder();
        int root = ReceiptHandle.createReceiptHandle(builder, consumerGroupId, topicId, queueId, operationId);
        builder.finish(root);
        return builder.sizedByteArray();
    }

    public static ReceiptHandle decodeReceiptHandle(String receiptHandle) {
        byte[] bytes = Base64.getDecoder().decode(receiptHandle);
        return ReceiptHandle.getRootAsReceiptHandle(ByteBuffer.wrap(bytes));
    }

    public static byte[] encodePopOperation(PopOperation popOperation) {
        FlatBufferBuilder builder = new FlatBufferBuilder();
        int operation = com.automq.rocketmq.store.model.generated.PopOperation.createPopOperation(builder,
            popOperation.consumerGroupId(), popOperation.topicId(), popOperation.queueId(),
            popOperation.offset(), popOperation.count(), popOperation.invisibleDuration(),
            popOperation.operationTimestamp(), popOperation.isEndMark(), popOperation.popOperationType().value()
        );
        int root = OperationLogItem.createOperationLogItem(builder, com.automq.rocketmq.store.model.generated.Operation.PopOperation, operation);
        builder.finish(root);
        return prependChecksum(builder.sizedByteArray());
    }

    public static Operation decodeOperation(ByteBuffer buffer, MessageStateMachine stateMachine, long operationStreamId,
        long snapshotStreamId) throws StoreException {
        if (!verifyAndStripChecksum(buffer)) {
            throw new StoreException(StoreErrorCode.DATA_CORRUPTED, "Data of encoded operation is corrupted");
        }

        OperationLogItem operationLogItem = OperationLogItem.getRootAsOperationLogItem(buffer);

        switch (operationLogItem.operationType()) {
            case com.automq.rocketmq.store.model.generated.Operation.PopOperation -> {
                com.automq.rocketmq.store.model.generated.PopOperation popOperation = (com.automq.rocketmq.store.model.generated.PopOperation) operationLogItem.operation(new com.automq.rocketmq.store.model.generated.PopOperation());
                return new PopOperation(
                    popOperation.topicId(), popOperation.queueId(), operationStreamId, snapshotStreamId, stateMachine,
                    popOperation.consumerGroupId(), popOperation.offset(), popOperation.count(), popOperation.invisibleDuration(),
                    popOperation.operationTimestamp(), popOperation.endMark(), PopOperation.PopOperationType.values()[popOperation.type()]);
            }
            case com.automq.rocketmq.store.model.generated.Operation.AckOperation -> {
                com.automq.rocketmq.store.model.generated.AckOperation ackOperation = (com.automq.rocketmq.store.model.generated.AckOperation) operationLogItem.operation(new com.automq.rocketmq.store.model.generated.AckOperation());
                return new AckOperation(ackOperation.receiptHandle().topicId(), ackOperation.receiptHandle().queueId(),
                    operationStreamId, snapshotStreamId, stateMachine, ackOperation.receiptHandle().consumerGroupId(),
                    ackOperation.receiptHandle().operationId(), ackOperation.operationTimestamp(),
                    AckOperation.AckOperationType.values()[ackOperation.type()]);
            }
            case com.automq.rocketmq.store.model.generated.Operation.ChangeInvisibleDurationOperation -> {
                com.automq.rocketmq.store.model.generated.ChangeInvisibleDurationOperation changeInvisibleDurationOperation = (com.automq.rocketmq.store.model.generated.ChangeInvisibleDurationOperation) operationLogItem.operation(new com.automq.rocketmq.store.model.generated.ChangeInvisibleDurationOperation());
                return new ChangeInvisibleDurationOperation(changeInvisibleDurationOperation.receiptHandle().topicId(), changeInvisibleDurationOperation.receiptHandle().queueId(),
                    operationStreamId, snapshotStreamId, stateMachine, changeInvisibleDurationOperation.receiptHandle().consumerGroupId(),
                    changeInvisibleDurationOperation.receiptHandle().operationId(), changeInvisibleDurationOperation.invisibleDuration(),
                    changeInvisibleDurationOperation.operationTimestamp());
            }
            case com.automq.rocketmq.store.model.generated.Operation.ResetConsumeOffsetOperation -> {
                com.automq.rocketmq.store.model.generated.ResetConsumeOffsetOperation resetConsumeOffsetOperation = (com.automq.rocketmq.store.model.generated.ResetConsumeOffsetOperation) operationLogItem.operation(new com.automq.rocketmq.store.model.generated.ResetConsumeOffsetOperation());
                return new ResetConsumeOffsetOperation(resetConsumeOffsetOperation.topicId(), resetConsumeOffsetOperation.queueId(),
                    operationStreamId, snapshotStreamId, stateMachine, resetConsumeOffsetOperation.consumerGroupId(), resetConsumeOffsetOperation.offset(),
                    resetConsumeOffsetOperation.operationTimestamp());
            }
            default ->
                throw new StoreException(StoreErrorCode.ILLEGAL_ARGUMENT, "Unexpected operation type: " + operationLogItem.operationType());
        }
    }

    public static byte[] encodeOperationSnapshot(OperationSnapshot snapshot) {
        FlatBufferBuilder builder = new FlatBufferBuilder();
        int[] consumerGroupMetadataOffsets = new int[snapshot.getConsumerGroupMetadataList().size()];
        for (int i = 0; i < snapshot.getConsumerGroupMetadataList().size(); i++) {
            ConsumerGroupMetadata consumerGroupMetadata = snapshot.getConsumerGroupMetadataList().get(i);
            int consumerGroupMetadataOffset = com.automq.rocketmq.store.model.generated.ConsumerGroupMetadata.createConsumerGroupMetadata(builder,
                consumerGroupMetadata.getConsumerGroupId(), consumerGroupMetadata.getConsumeOffset(), consumerGroupMetadata.getAckOffset(),
                consumerGroupMetadata.getRetryConsumeOffset(), consumerGroupMetadata.getRetryAckOffset(),
                consumerGroupMetadata.getVersion());
            consumerGroupMetadataOffsets[i] = consumerGroupMetadataOffset;
        }
        int consumerGroupMetadataVectorOffset = com.automq.rocketmq.store.model.generated.OperationSnapshot.createConsumerGroupMetadatasVector(builder, consumerGroupMetadataOffsets);
        int[] checkPointOffsets = new int[snapshot.getCheckPoints().size()];
        for (int i = 0; i < snapshot.getCheckPoints().size(); i++) {
            CheckPoint checkPoint = snapshot.getCheckPoints().get(i);
            int checkPointOffset = CheckPoint.createCheckPoint(builder, checkPoint.topicId(), checkPoint.queueId(), checkPoint.messageOffset(), checkPoint.count(), checkPoint.consumerGroupId(), checkPoint.operationId(),
                checkPoint.popOperationType(), checkPoint.deliveryTimestamp(), checkPoint.nextVisibleTimestamp());
            checkPointOffsets[i] = checkPointOffset;
        }
        int checkPointVectorOffset = com.automq.rocketmq.store.model.generated.OperationSnapshot.createCheckPointsVector(builder, checkPointOffsets);
        int root = com.automq.rocketmq.store.model.generated.OperationSnapshot.createOperationSnapshot(builder, snapshot.getSnapshotEndOffset(), checkPointVectorOffset, consumerGroupMetadataVectorOffset);
        builder.finish(root);
        return prependChecksum(builder.sizedByteArray());
    }

    public static OperationSnapshot decodeOperationSnapshot(ByteBuffer buffer) {
        if (!verifyAndStripChecksum(buffer)) {
            throw new RuntimeException(new StoreException(StoreErrorCode.DATA_CORRUPTED, "Operation Snapshot is corrupted"));
        }

        com.automq.rocketmq.store.model.generated.OperationSnapshot snapshot = com.automq.rocketmq.store.model.generated.OperationSnapshot.getRootAsOperationSnapshot(buffer);
        List<ConsumerGroupMetadata> consumerGroupMetadataList = new ArrayList<>(snapshot.consumerGroupMetadatasLength());
        for (int i = 0; i < snapshot.consumerGroupMetadatasLength(); i++) {
            com.automq.rocketmq.store.model.generated.ConsumerGroupMetadata consumerGroupMetadata = snapshot.consumerGroupMetadatas(i);
            consumerGroupMetadataList.add(new ConsumerGroupMetadata(consumerGroupMetadata.consumerGroupId(), consumerGroupMetadata.consumeOffset(),
                consumerGroupMetadata.ackOffset(), consumerGroupMetadata.retryConsumeOffset(), consumerGroupMetadata.retryAckOffset(), consumerGroupMetadata.version()));
        }
        List<CheckPoint> checkPointList = new ArrayList<>(snapshot.checkPointsLength());
        for (int i = 0; i < snapshot.checkPointsLength(); i++) {
            checkPointList.add(snapshot.checkPoints(i));
        }
        return new OperationSnapshot(snapshot.snapshotEndOffset(), consumerGroupMetadataList, checkPointList);
    }

    public static byte[] encodeAckOperation(AckOperation ackOperation) {
        long topicId = ackOperation.topicId();
        int queueId = ackOperation.queueId();
        long operationId = ackOperation.operationId();
        long operationTimestamp = ackOperation.operationTimestamp();
        long consumerGroupId = ackOperation.consumerGroupId();
        FlatBufferBuilder builder = new FlatBufferBuilder();
        int receiptHandleId = ReceiptHandle.createReceiptHandle(builder, consumerGroupId, topicId, queueId, operationId);
        int operation = com.automq.rocketmq.store.model.generated.AckOperation.createAckOperation(builder, receiptHandleId, operationTimestamp, (short) ackOperation.ackOperationType().ordinal());
        int root = OperationLogItem.createOperationLogItem(builder, com.automq.rocketmq.store.model.generated.Operation.AckOperation, operation);
        builder.finish(root);
        return prependChecksum(builder.sizedByteArray());
    }

    public static byte[] encodeChangeInvisibleDurationOperation(ChangeInvisibleDurationOperation durationOperation) {
        FlatBufferBuilder builder = new FlatBufferBuilder();
        int receiptHandleId = ReceiptHandle.createReceiptHandle(builder, durationOperation.consumerGroupId(), durationOperation.topicId(), durationOperation.queueId(), durationOperation.operationId());
        int operation = com.automq.rocketmq.store.model.generated.ChangeInvisibleDurationOperation.createChangeInvisibleDurationOperation(builder, receiptHandleId, durationOperation.invisibleDuration(), durationOperation.operationTimestamp());
        int root = OperationLogItem.createOperationLogItem(builder, com.automq.rocketmq.store.model.generated.Operation.ChangeInvisibleDurationOperation, operation);
        builder.finish(root);
        return prependChecksum(builder.sizedByteArray());
    }

    public static byte[] encodeResetConsumeOffsetOperation(ResetConsumeOffsetOperation resetConsumeOffsetOperation) {
        FlatBufferBuilder builder = new FlatBufferBuilder();
        int operation = com.automq.rocketmq.store.model.generated.ResetConsumeOffsetOperation.createResetConsumeOffsetOperation(builder, resetConsumeOffsetOperation.consumerGroupId(), resetConsumeOffsetOperation.topicId(), resetConsumeOffsetOperation.queueId(), resetConsumeOffsetOperation.offset(), resetConsumeOffsetOperation.operationTimestamp());
        int root = OperationLogItem.createOperationLogItem(builder, com.automq.rocketmq.store.model.generated.Operation.ResetConsumeOffsetOperation, operation);
        builder.finish(root);
        return prependChecksum(builder.sizedByteArray());
    }

    public static byte[] prependChecksum(byte[] data) {
        int crc32 = WALUtil.crc32(Unpooled.wrappedBuffer(data));
        ByteBuffer buffer = ByteBuffer.allocate(4 + data.length);
        buffer.putInt(crc32);
        buffer.put(data);
        buffer.flip();
        return buffer.array();
    }

    public static boolean verifyAndStripChecksum(ByteBuffer buffer) {
        if (buffer.remaining() < 4) {
            return false;
        }

        int crc = buffer.getInt();
        buffer.mark();
        int actual = WALUtil.crc32(Unpooled.wrappedBuffer(buffer));
        buffer.reset();
        if (crc != actual) {
            LOGGER.error("Data corrupted. CRC32 checksum failed");
        }
        return crc == actual;
    }
}
