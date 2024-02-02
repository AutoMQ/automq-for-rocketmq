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

public class AckOperation extends Operation {
    private final long consumerGroupId;
    private final long operationId;
    private final AckOperationType ackOperationType;

    public AckOperation(long topicId, int queueId, long operationStreamId, long snapshotStreamId,
        MessageStateMachine messageStateMachine, long consumerGroupId, long operationId, long operationTimestamp,
        AckOperationType ackOperationType) {
        this.topicId = topicId;
        this.queueId = queueId;
        this.operationStreamId = operationStreamId;
        this.snapshotStreamId = snapshotStreamId;
        this.stateMachine = messageStateMachine;
        this.consumerGroupId = consumerGroupId;
        this.operationId = operationId;
        this.operationTimestamp = operationTimestamp;
        this.ackOperationType = ackOperationType;
    }

    @Override
    public OperationType operationType() {
        return OperationType.ACK;
    }

    public enum AckOperationType {
        ACK_NORMAL,
        ACK_TIMEOUT
    }

    public long operationId() {
        return operationId;
    }

    public AckOperationType ackOperationType() {
        return ackOperationType;
    }

    public long consumerGroupId() {
        return consumerGroupId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        AckOperation operation = (AckOperation) o;
        return consumerGroupId == operation.consumerGroupId && topicId == operation.topicId && queueId == operation.queueId && operationId == operation.operationId && operationTimestamp == operation.operationTimestamp && ackOperationType == operation.ackOperationType;
    }

    @Override
    public int hashCode() {
        return Objects.hash(consumerGroupId, topicId, queueId, operationId, operationTimestamp, ackOperationType);
    }
}
