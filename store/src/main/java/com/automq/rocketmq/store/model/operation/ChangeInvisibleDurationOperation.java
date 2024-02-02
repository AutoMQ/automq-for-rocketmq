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

public class ChangeInvisibleDurationOperation extends Operation {
    private final long consumerGroupId;
    private final long operationId;
    private final long invisibleDuration;

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
