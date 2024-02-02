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

package com.automq.rocketmq.store.api;

import com.automq.rocketmq.store.exception.StoreException;
import com.automq.rocketmq.store.model.generated.CheckPoint;
import com.automq.rocketmq.store.model.operation.AckOperation;
import com.automq.rocketmq.store.model.operation.ChangeInvisibleDurationOperation;
import com.automq.rocketmq.store.model.operation.OperationSnapshot;
import com.automq.rocketmq.store.model.operation.PopOperation;
import com.automq.rocketmq.store.model.operation.ResetConsumeOffsetOperation;
import java.util.List;

public interface MessageStateMachine {
    long topicId();

    int queueId();

    void writeCheckPointsAndRelatedStates(List<CheckPoint> checkPointList) throws StoreException;

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
