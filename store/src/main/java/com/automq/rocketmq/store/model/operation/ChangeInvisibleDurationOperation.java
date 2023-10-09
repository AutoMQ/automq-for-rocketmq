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

import com.automq.rocketmq.store.api.MessageStateMachine;
import java.util.Objects;

public class ChangeInvisibleDurationOperation extends Operation {
    private final long consumerGroupId;
    private final long operationId;
    private final long invisibleDuration;
    private final long operationTimestamp;

    public ChangeInvisibleDurationOperation(long topicId, int queueId, long operationStreamId, long snapshotStreamId,
        MessageStateMachine messageStateMachine, long consumerGroupId, long operationId, long invisibleDuration,
        long operationTimestamp) {
        this.topicId = topicId;
        this.queueId = queueId;
        this.operationStreamId = operationStreamId;
        this.snapshotStreamId = snapshotStreamId;
        this.stateMachine = messageStateMachine;
        this.consumerGroupId = consumerGroupId;
        this.operationId = operationId;
        this.invisibleDuration = invisibleDuration;
        this.operationTimestamp = operationTimestamp;
    }

    public long consumerGroupId() {
        return consumerGroupId;
    }

    public long operationId() {
        return operationId;
    }

    public long invisibleDuration() {
        return invisibleDuration;
    }

    public long operationTimestamp() {
        return operationTimestamp;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        ChangeInvisibleDurationOperation operation = (ChangeInvisibleDurationOperation) o;
        return consumerGroupId == operation.consumerGroupId && topicId == operation.topicId && queueId == operation.queueId && operationId == operation.operationId && invisibleDuration == operation.invisibleDuration && operationTimestamp == operation.operationTimestamp;
    }

    @Override
    public int hashCode() {
        return Objects.hash(consumerGroupId, topicId, queueId, operationId, invisibleDuration, operationTimestamp);
    }

    @Override
    public OperationType operationType() {
        return OperationType.CHANGE_INVISIBLE_DURATION;
    }
}
