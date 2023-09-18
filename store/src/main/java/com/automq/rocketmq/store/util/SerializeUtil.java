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

import com.automq.rocketmq.store.model.generated.AckOperation;
import com.automq.rocketmq.store.model.generated.ChangeInvisibleDurationOperation;
import com.automq.rocketmq.store.model.generated.CheckPoint;
import com.automq.rocketmq.store.model.generated.Operation;
import com.automq.rocketmq.store.model.generated.OperationLogItem;
import com.automq.rocketmq.store.model.generated.PopOperation;
import com.automq.rocketmq.store.model.generated.ReceiptHandle;
import com.automq.rocketmq.store.model.generated.TimerTag;
import com.google.flatbuffers.FlatBufferBuilder;
import java.nio.ByteBuffer;
import java.util.Base64;

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

    public static byte[] buildCheckPointValue(long topicId, int queueId, long offset,
        long consumerGroupId, long operationId, boolean isOrder, boolean isRetry, long deliveryTimestamp,
        long nextVisibleTimestamp, int reconsumeCount) {
        FlatBufferBuilder builder = new FlatBufferBuilder();
        int root = CheckPoint.createCheckPoint(builder, topicId, queueId, offset, consumerGroupId, operationId, isOrder, isRetry, deliveryTimestamp, nextVisibleTimestamp, reconsumeCount);
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

    public static byte[] buildTimerTagValue(long nextVisibleTimestamp, long consumerGroupId, long topicId, int queueId,
        long streamId, long offset, long operationId) {
        FlatBufferBuilder builder = new FlatBufferBuilder();
        int root = TimerTag.createTimerTag(builder, nextVisibleTimestamp, consumerGroupId, topicId, queueId, streamId, offset, operationId);
        builder.finish(root);
        return builder.sizedByteArray();
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

    public static String encodeReceiptHandle(long topicId, int queueId, long offset, long operationId) {
        FlatBufferBuilder builder = new FlatBufferBuilder();
        int root = ReceiptHandle.createReceiptHandle(builder, topicId, queueId, offset, operationId);
        builder.finish(root);
        return new String(Base64.getEncoder().encode(builder.sizedByteArray()));
    }

    public static ReceiptHandle decodeReceiptHandle(String receiptHandle) {
        byte[] bytes = Base64.getDecoder().decode(receiptHandle);
        return ReceiptHandle.getRootAsReceiptHandle(ByteBuffer.wrap(bytes));
    }

    public static byte[] encodePopOperation(long consumerGroupId, long topicId, int queueId, long offset, int batchSize,
        boolean isOrder, long invisibleDuration, long operationTimestamp) {
        FlatBufferBuilder builder = new FlatBufferBuilder();
        int operation = PopOperation.createPopOperation(builder, consumerGroupId, topicId, queueId, offset, batchSize, isOrder, invisibleDuration, operationTimestamp);
        int root = OperationLogItem.createOperationLogItem(builder, Operation.PopOperation, operation);
        builder.finish(root);
        return builder.sizedByteArray();
    }

    public static byte[] encodeAckOperation(ReceiptHandle receiptHandle, long operationTimestamp) {
        FlatBufferBuilder builder = new FlatBufferBuilder();
        int receiptHandleId = ReceiptHandle.createReceiptHandle(builder, receiptHandle.topicId(), receiptHandle.queueId(), receiptHandle.messageOffset(), receiptHandle.operationId());
        int operation = AckOperation.createAckOperation(builder, receiptHandleId, operationTimestamp);
        int root = OperationLogItem.createOperationLogItem(builder, Operation.AckOperation, operation);
        builder.finish(root);
        return builder.sizedByteArray();
    }

    public static byte[] encodeChangeInvisibleDurationOperation(ReceiptHandle receiptHandle, long invisibleDuration,
        long operationTimestamp) {
        FlatBufferBuilder builder = new FlatBufferBuilder();
        int receiptHandleId = ReceiptHandle.createReceiptHandle(builder, receiptHandle.topicId(), receiptHandle.queueId(), receiptHandle.messageOffset(), receiptHandle.operationId());
        int operation = ChangeInvisibleDurationOperation.createChangeInvisibleDurationOperation(builder, receiptHandleId, invisibleDuration, operationTimestamp);
        int root = OperationLogItem.createOperationLogItem(builder, Operation.ChangeInvisibleDurationOperation, operation);
        builder.finish(root);
        return builder.sizedByteArray();
    }
}
