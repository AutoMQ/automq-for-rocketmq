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

package com.automq.rocketmq.store.util;

import com.automq.rocketmq.store.api.MessageStateMachine;
import com.automq.rocketmq.store.model.generated.CheckPoint;
import com.automq.rocketmq.store.model.generated.OperationLogItem;
import com.automq.rocketmq.store.model.generated.ReceiptHandle;
import com.automq.rocketmq.store.model.operation.AckOperation;
import com.automq.rocketmq.store.model.operation.ChangeInvisibleDurationOperation;
import com.automq.rocketmq.store.model.operation.Operation;
import com.automq.rocketmq.store.model.operation.OperationSnapshot;
import com.automq.rocketmq.store.model.operation.PopOperation;
import com.google.flatbuffers.FlatBufferBuilder;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.ConcurrentSkipListMap;

public class SerializeUtil {

    // <topicId><queueId>
    public static byte[] buildCheckPointPrefix(long topicId, int queueId) {
        ByteBuffer buffer = ByteBuffer.allocate(12);
        buffer.putLong(topicId);
        buffer.putInt(queueId);
        return buffer.array();
    }

    // <topicId><queueId><operationId>
    public static byte[] buildCheckPointKey(long topicId, int queueId, long operationId) {
        ByteBuffer buffer = ByteBuffer.allocate(20);
        buffer.putLong(topicId);
        buffer.putInt(queueId);
        buffer.putLong(operationId);
        return buffer.array();
    }

    public static byte[] buildCheckPointValue(long topicId, int queueId, long offset, int count,
        long consumerGroupId, long operationId, PopOperation.PopOperationType operationType, long deliveryTimestamp,
        long nextVisibleTimestamp) {
        FlatBufferBuilder builder = new FlatBufferBuilder();
        int root = CheckPoint.createCheckPoint(builder, topicId, queueId, offset, count, consumerGroupId, operationId, operationType.value(), deliveryTimestamp, nextVisibleTimestamp);
        builder.finish(root);
        return builder.sizedByteArray();
    }

    // <deliveryTimestamp + invisibleDuration><topicId><queueId><operationId>
    public static byte[] buildTimerTagKey(long nextVisibleTimestamp, long topicId, int queueId, long operationId) {
        ByteBuffer buffer = ByteBuffer.allocate(28);
        buffer.putLong(nextVisibleTimestamp);
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
    public static byte[] buildOrderIndexValue(long operationId) {
        ByteBuffer buffer = ByteBuffer.allocate(8);
        buffer.putLong(operationId);
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
        return builder.sizedByteArray();
    }

    public static Operation decodeOperation(ByteBuffer buffer,
        MessageStateMachine stateMachine, long operationStreamId, long snapshotStreamId) {
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
            default -> throw new IllegalStateException("Unexpected value: " + operationLogItem.operationType());
        }
    }

    public static byte[] encodeOperationSnapshot(OperationSnapshot snapshot) {
        FlatBufferBuilder builder = new FlatBufferBuilder();
        int[] consumerGroupMetadataOffsets = new int[snapshot.getConsumerGroupMetadataList().size()];
        for (int i = 0; i < snapshot.getConsumerGroupMetadataList().size(); i++) {
            OperationSnapshot.ConsumerGroupMetadataSnapshot consumerGroupMetadata = snapshot.getConsumerGroupMetadataList().get(i);
            int ackOffsetBitmapOffset = builder.createByteVector(consumerGroupMetadata.getAckOffsetBitmapBuffer());
            int retryAckOffsetBitmapOffset = builder.createByteVector(consumerGroupMetadata.getRetryAckOffsetBitmapBuffer());
            List<Integer> consumeTimesOffsets = new ArrayList<>(consumerGroupMetadata.getConsumeTimes().size());
            consumerGroupMetadata.getConsumeTimes().entrySet().forEach(entry -> {
                int consumeTimeOffset = com.automq.rocketmq.store.model.generated.ConsumeTimes.createConsumeTimes(builder, entry.getKey(), entry.getValue());
                consumeTimesOffsets.add(consumeTimeOffset);
            });
            int consumeTimesVectorOffset = com.automq.rocketmq.store.model.generated.ConsumerGroupMetadata.createConsumeTimesVector(builder, consumeTimesOffsets.stream().mapToInt(Integer::intValue).toArray());
            int consumerGroupMetadataOffset = com.automq.rocketmq.store.model.generated.ConsumerGroupMetadata.createConsumerGroupMetadata(builder,
                consumerGroupMetadata.getConsumerGroupId(), consumerGroupMetadata.getConsumeOffset(), consumerGroupMetadata.getAckOffset(),
                ackOffsetBitmapOffset,
                consumerGroupMetadata.getRetryConsumeOffset(), consumerGroupMetadata.getRetryAckOffset(),
                retryAckOffsetBitmapOffset, consumeTimesVectorOffset);
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
        return builder.sizedByteArray();
    }

    public static OperationSnapshot decodeOperationSnapshot(ByteBuffer buffer) {
        com.automq.rocketmq.store.model.generated.OperationSnapshot snapshot = com.automq.rocketmq.store.model.generated.OperationSnapshot.getRootAsOperationSnapshot(buffer);
        List<OperationSnapshot.ConsumerGroupMetadataSnapshot> consumerGroupMetadataList = new ArrayList<>(snapshot.consumerGroupMetadatasLength());
        for (int i = 0; i < snapshot.consumerGroupMetadatasLength(); i++) {
            com.automq.rocketmq.store.model.generated.ConsumerGroupMetadata consumerGroupMetadata = snapshot.consumerGroupMetadatas(i);
            byte[] ackBitMap = new byte[consumerGroupMetadata.ackBitMapLength()];
            consumerGroupMetadata.ackBitMapAsByteBuffer().get(ackBitMap);
            byte[] retryAckBitMap = new byte[consumerGroupMetadata.retryAckBitMapLength()];
            ConcurrentSkipListMap<Long, Integer> consumeTimes = new ConcurrentSkipListMap<>();
            for (int j = 0; j < consumerGroupMetadata.consumeTimesLength(); j++) {
                com.automq.rocketmq.store.model.generated.ConsumeTimes consumeTime = consumerGroupMetadata.consumeTimes(j);
                consumeTimes.put(consumeTime.offset(), consumeTime.consumeTimes());
            }
            consumerGroupMetadata.retryAckBitMapAsByteBuffer().get(retryAckBitMap);
            consumerGroupMetadataList.add(new OperationSnapshot.ConsumerGroupMetadataSnapshot(consumerGroupMetadata.consumerGroupId(),
                consumerGroupMetadata.consumeOffset(), consumerGroupMetadata.ackOffset(), consumerGroupMetadata.retryConsumeOffset(), consumerGroupMetadata.retryAckOffset(),
                ackBitMap, retryAckBitMap, consumeTimes));
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
        return builder.sizedByteArray();
    }

    public static byte[] encodeChangeInvisibleDurationOperation(ChangeInvisibleDurationOperation durationOperation) {
        FlatBufferBuilder builder = new FlatBufferBuilder();
        int receiptHandleId = ReceiptHandle.createReceiptHandle(builder, durationOperation.consumerGroupId(), durationOperation.topicId(), durationOperation.queueId(), durationOperation.operationId());
        int operation = com.automq.rocketmq.store.model.generated.ChangeInvisibleDurationOperation.createChangeInvisibleDurationOperation(builder, receiptHandleId, durationOperation.invisibleDuration(), durationOperation.operationTimestamp());
        int root = OperationLogItem.createOperationLogItem(builder, com.automq.rocketmq.store.model.generated.Operation.ChangeInvisibleDurationOperation, operation);
        builder.finish(root);
        return builder.sizedByteArray();
    }
}
