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

package com.automq.rocketmq.store.model.operation;

import java.util.Objects;

public class PopOperation implements Operation {

    private final long consumerGroupId;
    private final long topicId;
    private final int queueId;
    private final long offset;
    private final int count;
    private final long invisibleDuration;
    private final long operationTimestamp;
    private final long retryOffset;
    private final PopOperationType popOperationType;

    public PopOperation(long consumerGroupId, long topicId, int queueId, long offset, int count,
        long invisibleDuration, long operationTimestamp, long retryOffset, PopOperationType popOperationType) {
        this.consumerGroupId = consumerGroupId;
        this.topicId = topicId;
        this.queueId = queueId;
        this.offset = offset;
        this.count = count;
        this.invisibleDuration = invisibleDuration;
        this.operationTimestamp = operationTimestamp;
        this.retryOffset = retryOffset;
        this.popOperationType = popOperationType;
    }

    public long getConsumerGroupId() {
        return consumerGroupId;
    }

    public long getTopicId() {
        return topicId;
    }

    public int getQueueId() {
        return queueId;
    }

    public long getOffset() {
        return offset;
    }

    public int getCount() {
        return count;
    }

    public PopOperationType getPopOperationType() {
        return popOperationType;
    }

    public long getInvisibleDuration() {
        return invisibleDuration;
    }

    public long getOperationTimestamp() {
        return operationTimestamp;
    }

    @Override
    public OperationType getOperationType() {
        return OperationType.POP;
    }

    public long getRetryOffset() {
        return retryOffset;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        PopOperation that = (PopOperation) o;
        return consumerGroupId == that.consumerGroupId && topicId == that.topicId && queueId == that.queueId && offset == that.offset && count == that.count && invisibleDuration == that.invisibleDuration && operationTimestamp == that.operationTimestamp && retryOffset == that.retryOffset && popOperationType == that.popOperationType;
    }

    @Override
    public int hashCode() {
        return Objects.hash(consumerGroupId, topicId, queueId, offset, count, invisibleDuration, operationTimestamp, retryOffset, popOperationType);
    }

    public enum PopOperationType {
        POP_NORMAL,
        POP_RETRY,
        POP_ORDER,
        POP_LAST
    }

}
