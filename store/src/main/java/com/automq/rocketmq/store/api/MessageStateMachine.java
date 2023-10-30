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

package com.automq.rocketmq.store.api;

import com.automq.rocketmq.store.exception.StoreException;
import com.automq.rocketmq.store.model.operation.AckOperation;
import com.automq.rocketmq.store.model.operation.ChangeInvisibleDurationOperation;
import com.automq.rocketmq.store.model.operation.OperationSnapshot;
import com.automq.rocketmq.store.model.operation.PopOperation;
import com.automq.rocketmq.store.model.operation.ResetConsumeOffsetOperation;

public interface MessageStateMachine {
    long topicId();

    int queueId();

    ReplayPopResult replayPopOperation(long operationOffset, PopOperation operation) throws StoreException;

    void replayAckOperation(long operationOffset, AckOperation operation) throws StoreException;

    void replayChangeInvisibleDurationOperation(long operationOffset, ChangeInvisibleDurationOperation operation);

    void replayResetConsumeOffsetOperation(long operationOffset, ResetConsumeOffsetOperation operation);

    OperationSnapshot takeSnapshot() throws StoreException;

    void loadSnapshot(OperationSnapshot snapshot);

    void clear() throws StoreException;

    long consumeOffset(long consumerGroupId);

    long ackOffset(long consumerGroupId);

    long retryConsumeOffset(long consumerGroupId);

    long retryAckOffset(long consumerGroupId);

    boolean isLocked(long consumerGroupId, long offset) throws StoreException;

    int consumeTimes(long consumerGroupId, long offset);

    void registerAckOffsetListener(OffsetListener listener);

    void registerRetryAckOffsetListener(OffsetListener listener);

    class ReplayPopResult {
        private final int popTimes;

        private ReplayPopResult(int popTimes) {
            this.popTimes = popTimes;
        }

        public static ReplayPopResult empty() {
            return new ReplayPopResult(-1);
        }

        public static ReplayPopResult of(int popTimes) {
            return new ReplayPopResult(popTimes);
        }

        public int getPopTimes() {
            return popTimes;
        }
    }

    interface OffsetListener {
        void onOffset(long consumerGroupId, long offset);
    }

}
