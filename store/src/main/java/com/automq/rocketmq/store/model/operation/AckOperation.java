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

public class AckOperation implements Operation {
    private final long consumerGroupId;
    private final long topicId;
    private final int queueId;
    private final long offset;
    private final long operationId;
    private final long operationTimestamp;
    private final AckOperationType ackOperationType;

    public AckOperation(long consumerGroupId, long topicId, int queueId, long offset, long operationId, long operationTimestamp, AckOperationType ackOperationType) {
        this.consumerGroupId = consumerGroupId;
        this.topicId = topicId;
        this.queueId = queueId;
        this.offset = offset;
        this.operationId = operationId;
        this.operationTimestamp = operationTimestamp;
        this.ackOperationType = ackOperationType;
    }

    @Override
    public OperationType getOperationType() {
        return OperationType.ACK;
    }

    public enum AckOperationType {
        ACK_NORMAL,
        ACK_TIMEOUT
    }

    public long getTopicId() {
        return topicId;
    }

    public long getOperationTimestamp() {
        return operationTimestamp;
    }

    public int getQueueId() {
        return queueId;
    }

    public long getOffset() {
        return offset;
    }

    public long getOperationId() {
        return operationId;
    }

    public AckOperationType getAckOperationType() {
        return ackOperationType;
    }

    public long getConsumerGroupId() {
        return consumerGroupId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        AckOperation operation = (AckOperation) o;
        return consumerGroupId == operation.consumerGroupId && topicId == operation.topicId && queueId == operation.queueId && offset == operation.offset && operationId == operation.operationId && operationTimestamp == operation.operationTimestamp && ackOperationType == operation.ackOperationType;
    }

    @Override
    public int hashCode() {
        return Objects.hash(consumerGroupId, topicId, queueId, offset, operationId, operationTimestamp, ackOperationType);
    }
}
