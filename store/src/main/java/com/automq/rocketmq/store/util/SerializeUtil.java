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

import com.automq.rocketmq.store.model.generated.ChangeInvisibleDurationOperation;
import com.automq.rocketmq.store.model.generated.CheckPoint;
import com.automq.rocketmq.store.model.generated.ConsumeTimes;
import com.automq.rocketmq.store.model.generated.OperationLogItem;
import com.automq.rocketmq.store.model.generated.ReceiptHandle;
import com.automq.rocketmq.store.model.generated.RetryTimes;
import com.automq.rocketmq.store.model.metadata.ConsumerGroupMetadata;
import com.automq.rocketmq.store.model.operation.AckOperation;
import com.automq.rocketmq.store.model.operation.Operation;
import com.automq.rocketmq.store.model.operation.OperationSnapshot;
import com.automq.rocketmq.store.model.operation.PopOperation;
import com.google.flatbuffers.FlatBufferBuilder;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SerializeUtil {
    // <topicId><queueId><offset><operationId>
    public static byte[] buildCheckPointKey(long topicId, int queueId, long offset, long operationId) {
        ByteBuffer buffer = ByteBuffer.allocate(28);
        buffer.putLong(topicId);
        buffer.putInt(queueId);
        buffer.putLong(offset);
        buffer.putLong(operationId);
        return buffer.array();
    }

    public static byte[] buildCheckPointValue(long topicId, int queueId, long offset, int count,
        long consumerGroupId, long operationId, boolean isOrder, boolean isRetry, long deliveryTimestamp,
        long nextVisibleTimestamp) {
        FlatBufferBuilder builder = new FlatBufferBuilder();
        int root = CheckPoint.createCheckPoint(builder, topicId, queueId, offset, count, consumerGroupId, operationId, isOrder, isRetry, deliveryTimestamp, nextVisibleTimestamp);
        builder.finish(root);
        return builder.sizedByteArray();
    }

    // <deliveryTimestamp + invisibleDuration><topicId><queueId><operationId>
    public static byte[] buildTimerTagKey(long nextVisibleTimestamp, long topicId, int queueId, long offset,
        long operationId) {
        ByteBuffer buffer = ByteBuffer.allocate(36);
        buffer.putLong(nextVisibleTimestamp);
        buffer.putLong(topicId);
        buffer.putInt(queueId);
        buffer.putLong(offset);
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

    public static String encodeReceiptHandle(long consumerGroupId, long topicId, int queueId, long offset,
        long operationId) {
        return new String(Base64.getEncoder().encode(buildReceiptHandle(consumerGroupId, topicId, queueId, offset, operationId)));
    }

    public static String encodeReceiptHandle(ReceiptHandle receiptHandle) {
        return new String(Base64.getEncoder().encode(buildReceiptHandle(receiptHandle.consumerGroupId(), receiptHandle.topicId(),
            receiptHandle.queueId(), receiptHandle.messageOffset(), receiptHandle.operationId())));
    }

    public static byte[] buildReceiptHandle(long consumerGroupId, long topicId, int queueId, long offset,
        long operationId) {
        FlatBufferBuilder builder = new FlatBufferBuilder();
        int root = ReceiptHandle.createReceiptHandle(builder, consumerGroupId, topicId, queueId, offset, operationId);
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
            popOperation.getConsumerGroupId(), popOperation.getTopicId(), popOperation.getQueueId(),
            popOperation.getOffset(), popOperation.getCount(), popOperation.getInvisibleDuration(),
            popOperation.getOperationTimestamp(), popOperation.getRetryOffset(), (short) popOperation.getPopOperationType().ordinal()
        );
        int root = OperationLogItem.createOperationLogItem(builder, com.automq.rocketmq.store.model.generated.Operation.PopOperation, operation);
        builder.finish(root);
        return builder.sizedByteArray();
    }

    public static PopOperation decodePopOperation(ByteBuffer buffer) {
        com.automq.rocketmq.store.model.generated.PopOperation popOperation = com.automq.rocketmq.store.model.generated.PopOperation.getRootAsPopOperation(buffer);
        return new PopOperation(
            popOperation.consumerGroupId(), popOperation.topicId(), popOperation.queueId(),
            popOperation.offset(), popOperation.count(), popOperation.invisibleDuration(),
            popOperation.operationTimestamp(), popOperation.retryOffset(), com.automq.rocketmq.store.model.operation.PopOperation.PopOperationType.values()[popOperation.type()]
        );
    }

    public static Operation decodeOperation(ByteBuffer buffer) {
        OperationLogItem operationLogItem = OperationLogItem.getRootAsOperationLogItem(buffer);
        if (operationLogItem.operationType() == com.automq.rocketmq.store.model.generated.Operation.PopOperation) {
            com.automq.rocketmq.store.model.generated.PopOperation popOperation = (com.automq.rocketmq.store.model.generated.PopOperation) operationLogItem.operation(new com.automq.rocketmq.store.model.generated.PopOperation());
            return new PopOperation(
                popOperation.consumerGroupId(), popOperation.topicId(), popOperation.queueId(),
                popOperation.offset(), popOperation.count(), popOperation.invisibleDuration(),
                popOperation.operationTimestamp(), popOperation.retryOffset(), com.automq.rocketmq.store.model.operation.PopOperation.PopOperationType.values()[popOperation.type()]);
        } else if (operationLogItem.operationType() == com.automq.rocketmq.store.model.generated.Operation.AckOperation) {
            com.automq.rocketmq.store.model.generated.AckOperation ackOperation = (com.automq.rocketmq.store.model.generated.AckOperation) operationLogItem.operation(new com.automq.rocketmq.store.model.generated.AckOperation());
            return new AckOperation(
                ackOperation.receiptHandle().consumerGroupId(),
                ackOperation.receiptHandle().topicId(), ackOperation.receiptHandle().queueId(),
                ackOperation.receiptHandle().messageOffset(), ackOperation.receiptHandle().operationId(),
                ackOperation.operationTimestamp(), com.automq.rocketmq.store.model.operation.AckOperation.AckOperationType.values()[ackOperation.type()]);
        }
        return null;
    }

    public static OperationSnapshot decodeOperationSnapshot(ByteBuffer buffer) {
        com.automq.rocketmq.store.model.generated.OperationSnapshot snapshot = com.automq.rocketmq.store.model.generated.OperationSnapshot.getRootAsOperationSnapshot(buffer);
        List<ConsumerGroupMetadata> consumerGroupMetadataList = new ArrayList<>();
        for (int i = 0; i < snapshot.consumerGroupMetadatasLength(); i++) {
            com.automq.rocketmq.store.model.generated.ConsumerGroupMetadata consumerGroupMetadata = snapshot.consumerGroupMetadatas(i);
            Map<Long, Integer> retryTimes = new HashMap<>(consumerGroupMetadata.offsetRetryTimesLength());
            for (int j = 0; j < consumerGroupMetadata.offsetRetryTimesLength(); j++) {
                RetryTimes retryTime = consumerGroupMetadata.offsetRetryTimes(j);
                retryTimes.put(retryTime.offset(), retryTime.retryTimes());
            }
            Map<Long, Integer> consumeTimes = new HashMap<>(consumerGroupMetadata.offsetConsumeTimesLength());
            for (int j = 0; j < consumerGroupMetadata.offsetConsumeTimesLength(); j++) {
                ConsumeTimes consumeTime = consumerGroupMetadata.offsetConsumeTimes(j);
                consumeTimes.put(consumeTime.offset(), consumeTime.consumeTimes());
            }
            consumerGroupMetadataList.add(new ConsumerGroupMetadata(consumerGroupMetadata.consumerGroupId(),
                consumerGroupMetadata.consumeOffset(), consumerGroupMetadata.ackOffset(), consumerGroupMetadata.retryOffset(), retryTimes, consumeTimes));
        }
        List<PopOperation> popOperations = new ArrayList<>();
        for (int i = 0; i < snapshot.popOperationsLength(); i++) {
            com.automq.rocketmq.store.model.generated.PopOperation popOperation = snapshot.popOperations(i);
            popOperations.add(new PopOperation(
                popOperation.consumerGroupId(), popOperation.topicId(), popOperation.queueId(),
                popOperation.offset(), popOperation.count(), popOperation.invisibleDuration(),
                popOperation.operationTimestamp(), popOperation.retryOffset(), com.automq.rocketmq.store.model.operation.PopOperation.PopOperationType.values()[popOperation.type()]));
        }

        return new OperationSnapshot(snapshot.trimOffset(), popOperations, consumerGroupMetadataList);
    }

    public static byte[] encodeAckOperation(AckOperation ackOperation) {
        long topicId = ackOperation.getTopicId();
        int queueId = ackOperation.getQueueId();
        long offset = ackOperation.getOffset();
        long operationId = ackOperation.getOperationId();
        long operationTimestamp = ackOperation.getOperationTimestamp();
        long consumerGroupId = ackOperation.getConsumerGroupId();
        FlatBufferBuilder builder = new FlatBufferBuilder();
        int receiptHandleId = ReceiptHandle.createReceiptHandle(builder, consumerGroupId, topicId, queueId, offset, operationId);
        int operation = com.automq.rocketmq.store.model.generated.AckOperation.createAckOperation(builder, receiptHandleId, operationTimestamp, (short) ackOperation.getAckOperationType().ordinal());
        int root = OperationLogItem.createOperationLogItem(builder, com.automq.rocketmq.store.model.generated.Operation.AckOperation, operation);
        builder.finish(root);
        return builder.sizedByteArray();
    }

    public static byte[] encodeChangeInvisibleDurationOperation(
        com.automq.rocketmq.store.model.operation.ChangeInvisibleDurationOperation durationOperation) {
        FlatBufferBuilder builder = new FlatBufferBuilder();
        int receiptHandleId = ReceiptHandle.createReceiptHandle(builder, durationOperation.getConsumerGroupId(), durationOperation.getTopicId(), durationOperation.getQueueId(), durationOperation.getOffset(), durationOperation.getOperationId());
        int operation = ChangeInvisibleDurationOperation.createChangeInvisibleDurationOperation(builder, receiptHandleId, durationOperation.getInvisibleDuration(), durationOperation.getOperationTimestamp());
        int root = OperationLogItem.createOperationLogItem(builder, com.automq.rocketmq.store.model.generated.Operation.ChangeInvisibleDurationOperation, operation);
        builder.finish(root);
        return builder.sizedByteArray();
    }

    public static com.automq.rocketmq.store.model.operation.ChangeInvisibleDurationOperation decodeChangeInvisibleDurationOperation(
        ByteBuffer buffer) {
        ChangeInvisibleDurationOperation durationOperation = ChangeInvisibleDurationOperation.getRootAsChangeInvisibleDurationOperation(buffer);
        return new com.automq.rocketmq.store.model.operation.ChangeInvisibleDurationOperation(
            durationOperation.receiptHandle().consumerGroupId(), durationOperation.receiptHandle().topicId(), durationOperation.receiptHandle().queueId(),
            durationOperation.receiptHandle().messageOffset(), durationOperation.receiptHandle().operationId(),
            durationOperation.invisibleDuration(), durationOperation.operationTimestamp());
    }
}
