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

public class ChangeInvisibleDurationOperation implements Operation {

    private final long consumerGroupId;
    private final long topicId;
    private final int queueId;
    private final long offset;
    private final long operationId;
    private final long invisibleDuration;
    private final long operationTimestamp;

    public ChangeInvisibleDurationOperation(long consumerGroupId, long topicId, int queueId, long offset, long operationId, long invisibleDuration, long operationTimestamp) {
        this.consumerGroupId = consumerGroupId;
        this.topicId = topicId;
        this.queueId = queueId;
        this.offset = offset;
        this.operationId = operationId;
        this.invisibleDuration = invisibleDuration;
        this.operationTimestamp = operationTimestamp;
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

    public long getOperationId() {
        return operationId;
    }

    public long getInvisibleDuration() {
        return invisibleDuration;
    }

    public long getOperationTimestamp() {
        return operationTimestamp;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        ChangeInvisibleDurationOperation operation = (ChangeInvisibleDurationOperation) o;
        return consumerGroupId == operation.consumerGroupId && topicId == operation.topicId && queueId == operation.queueId && offset == operation.offset && operationId == operation.operationId && invisibleDuration == operation.invisibleDuration && operationTimestamp == operation.operationTimestamp;
    }

    @Override
    public int hashCode() {
        return Objects.hash(consumerGroupId, topicId, queueId, offset, operationId, invisibleDuration, operationTimestamp);
    }

    @Override
    public OperationType getOperationType() {
        return OperationType.CHANGE_INVISIBLE_DURATION;
    }
}
