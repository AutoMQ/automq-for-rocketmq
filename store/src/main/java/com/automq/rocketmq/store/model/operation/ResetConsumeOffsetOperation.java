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

public class ResetConsumeOffsetOperation extends Operation {

    private final long consumerGroupId;
    private final long offset;


    public ResetConsumeOffsetOperation(long topicId, int queueId, long operationStreamId, long snapshotStreamId,
        MessageStateMachine stateMachine, long consumerGroupId, long offset, long operationTimestamp) {
        this.topicId = topicId;
        this.queueId = queueId;
        this.operationStreamId = operationStreamId;
        this.snapshotStreamId = snapshotStreamId;
        this.stateMachine = stateMachine;
        this.operationTimestamp = operationTimestamp;
        this.consumerGroupId = consumerGroupId;
        this.offset = offset;
    }

    @Override
    public OperationType operationType() {
        return OperationType.RESET_CONSUME_OFFSET;
    }

    public long consumerGroupId() {
        return consumerGroupId;
    }

    public long offset() {
        return offset;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        ResetConsumeOffsetOperation operation = (ResetConsumeOffsetOperation) o;
        return consumerGroupId == operation.consumerGroupId && topicId == operation.topicId && queueId == operation.queueId && offset == operation.offset() && operationTimestamp == operation.operationTimestamp;
    }

    @Override
    public int hashCode() {
        return Objects.hash(consumerGroupId, topicId, queueId, offset, operationTimestamp);
    }
}
