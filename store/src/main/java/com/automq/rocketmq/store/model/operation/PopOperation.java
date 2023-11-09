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

public class PopOperation extends Operation {
    private final long consumerGroupId;
    private final long offset;
    private final int count;
    private final long invisibleDuration;
    private final boolean endMark;
    private final PopOperationType popOperationType;

    public PopOperation(long topicId, int queueId, long operationStreamId, long snapshotStreamId,
        MessageStateMachine stateMachine, long consumerGroupId, long offset, int count, long invisibleDuration,
        long operationTimestamp, boolean endMark, PopOperationType popOperationType) {
        this.topicId = topicId;
        this.queueId = queueId;
        this.operationStreamId = operationStreamId;
        this.snapshotStreamId = snapshotStreamId;
        this.stateMachine = stateMachine;
        this.consumerGroupId = consumerGroupId;
        this.offset = offset;
        this.count = count;
        this.invisibleDuration = invisibleDuration;
        this.operationTimestamp = operationTimestamp;
        this.endMark = endMark;
        this.popOperationType = popOperationType;
    }

    public long consumerGroupId() {
        return consumerGroupId;
    }

    public long offset() {
        return offset;
    }

    public int count() {
        return count;
    }

    public PopOperationType popOperationType() {
        return popOperationType;
    }

    public long invisibleDuration() {
        return invisibleDuration;
    }

    public long operationTimestamp() {
        return operationTimestamp;
    }

    @Override
    public OperationType operationType() {
        return OperationType.POP;
    }

    public boolean isEndMark() {
        return endMark;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        PopOperation that = (PopOperation) o;
        return consumerGroupId == that.consumerGroupId && topicId == that.topicId && queueId == that.queueId
            && offset == that.offset && count == that.count && invisibleDuration == that.invisibleDuration
            && operationTimestamp == that.operationTimestamp && endMark == that.endMark
            && popOperationType == that.popOperationType;
    }

    @Override
    public int hashCode() {
        return Objects.hash(consumerGroupId, topicId, queueId, offset, count, invisibleDuration, operationTimestamp,
            endMark, popOperationType);
    }

    public enum PopOperationType {
        POP_NORMAL,
        POP_RETRY,
        POP_ORDER;

        public short value() {
            return (short) ordinal();
        }

        public static PopOperationType valueOf(short value) {
            return values()[value];
        }
    }

    @Override
    public String toString() {
        return "PopOperation{" +
            "consumerGroupId=" + consumerGroupId +
            ", offset=" + offset +
            ", count=" + count +
            ", invisibleDuration=" + invisibleDuration +
            ", endMark=" + endMark +
            ", popOperationType=" + popOperationType +
            '}';
    }
}
