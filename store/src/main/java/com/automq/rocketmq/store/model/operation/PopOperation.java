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
