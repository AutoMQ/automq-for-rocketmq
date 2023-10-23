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

}
