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

public abstract class Operation {
    protected long topicId;
    protected int queueId;
    protected long operationStreamId;
    protected long snapshotStreamId;
    protected MessageStateMachine stateMachine;
    protected long operationTimestamp;

    public abstract OperationType operationType();

    public enum OperationType {
        POP,
        ACK,
        CHANGE_INVISIBLE_DURATION,
        RESET_CONSUME_OFFSET
    }

    public long topicId() {
        return topicId;
    }

    public int queueId() {
        return queueId;
    }

    public long operationStreamId() {
        return operationStreamId;
    }

    public long snapshotStreamId() {
        return snapshotStreamId;
    }

    public MessageStateMachine stateMachine() {
        return stateMachine;
    }

    public long operationTimestamp() {
        return operationTimestamp;
    }
}
