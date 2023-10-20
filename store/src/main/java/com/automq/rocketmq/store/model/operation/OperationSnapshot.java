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

import com.automq.rocketmq.store.model.generated.CheckPoint;
import com.automq.rocketmq.store.model.metadata.ConsumerGroupMetadata;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentSkipListMap;

public class OperationSnapshot {

    private final long snapshotEndOffset;
    private List<CheckPoint> checkPoints;
    private long kvServiceSnapshotVersion;
    private final List<ConsumerGroupMetadataSnapshot> consumerGroupMetadataList;

    public OperationSnapshot(long snapshotEndOffset, long kvServiceSnapshotVersion, List<ConsumerGroupMetadataSnapshot> consumerGroupMetadataList) {
        this.snapshotEndOffset = snapshotEndOffset;
        this.kvServiceSnapshotVersion = kvServiceSnapshotVersion;
        this.consumerGroupMetadataList = consumerGroupMetadataList;
    }

    public OperationSnapshot(long snapshotEndOffset, List<ConsumerGroupMetadataSnapshot> consumerGroupMetadataList, List<CheckPoint> checkPoints) {
        this.snapshotEndOffset = snapshotEndOffset;
        this.consumerGroupMetadataList = consumerGroupMetadataList;
        this.checkPoints = checkPoints;
    }

    public long getSnapshotEndOffset() {
        return snapshotEndOffset;
    }

    public List<CheckPoint> getCheckPoints() {
        return checkPoints;
    }

    public void setCheckPoints(List<CheckPoint> checkPoints) {
        this.checkPoints = checkPoints;
    }

    public long getKvServiceSnapshotVersion() {
        return kvServiceSnapshotVersion;
    }

    public List<ConsumerGroupMetadataSnapshot> getConsumerGroupMetadataList() {
        return consumerGroupMetadataList;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        OperationSnapshot that = (OperationSnapshot) o;
        return snapshotEndOffset == that.snapshotEndOffset && kvServiceSnapshotVersion == that.kvServiceSnapshotVersion && Objects.equals(checkPoints, that.checkPoints) && Objects.equals(consumerGroupMetadataList, that.consumerGroupMetadataList);
    }

    @Override
    public int hashCode() {
        return Objects.hash(snapshotEndOffset, checkPoints, kvServiceSnapshotVersion, consumerGroupMetadataList);
    }

    @Override
    public String toString() {
        return "OperationSnapshot{" +
            "snapshotEndOffset=" + snapshotEndOffset +
            '}';
    }

    public static class ConsumerGroupMetadataSnapshot extends ConsumerGroupMetadata {
        private final byte[] ackOffsetBitmapBuffer;
        private final byte[] retryAckOffsetBitmapBuffer;

        public ConsumerGroupMetadataSnapshot(long consumerGroupId, long consumeOffset, long ackOffset,
            long retryConsumeOffset, long retryAckOffset,
            byte[] ackOffsetBitmapBuffer, byte[] retryAckOffsetBitmapBuffer,
            ConcurrentSkipListMap<Long, Integer> consumeTimes) {
            super(consumerGroupId, consumeOffset, ackOffset, retryConsumeOffset, retryAckOffset, consumeTimes);
            this.ackOffsetBitmapBuffer = ackOffsetBitmapBuffer;
            this.retryAckOffsetBitmapBuffer = retryAckOffsetBitmapBuffer;
        }

        public byte[] getAckOffsetBitmapBuffer() {
            return ackOffsetBitmapBuffer;
        }

        public byte[] getRetryAckOffsetBitmapBuffer() {
            return retryAckOffsetBitmapBuffer;
        }

    }

}
