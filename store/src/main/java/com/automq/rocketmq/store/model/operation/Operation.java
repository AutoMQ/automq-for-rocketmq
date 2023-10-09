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

public abstract class Operation {
    protected long topicId;
    protected int queueId;
    protected long operationStreamId;
    protected long snapshotStreamId;
    protected MessageStateMachine stateMachine;

    public abstract OperationType operationType();

    public enum OperationType {
        POP,
        ACK,
        CHANGE_INVISIBLE_DURATION,
        SNAPSHOT
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
}
