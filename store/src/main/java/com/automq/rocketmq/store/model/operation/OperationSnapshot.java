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
import java.nio.ByteBuffer;
import java.util.List;

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

    public long getKvServiceSnapshotVersion() {
        return kvServiceSnapshotVersion;
    }

    public List<ConsumerGroupMetadataSnapshot> getConsumerGroupMetadataList() {
        return consumerGroupMetadataList;
    }

    public static class ConsumerGroupMetadataSnapshot extends ConsumerGroupMetadata {
        private final ByteBuffer ackOffsetBitmapBuffer;
        private final ByteBuffer retryAckOffsetBitmapBuffer;

        public ConsumerGroupMetadataSnapshot(long consumerGroupId, long consumeOffset, long ackOffset,
            long retryConsumeOffset, long retryAckOffset,
            ByteBuffer ackOffsetBitmapBuffer, ByteBuffer retryAckOffsetBitmapBuffer) {
            super(consumerGroupId, consumeOffset, ackOffset, retryConsumeOffset, retryAckOffset);
            this.ackOffsetBitmapBuffer = ackOffsetBitmapBuffer;
            this.retryAckOffsetBitmapBuffer = retryAckOffsetBitmapBuffer;
        }

        public ByteBuffer getAckOffsetBitmapBuffer() {
            return ackOffsetBitmapBuffer;
        }

        public ByteBuffer getRetryAckOffsetBitmapBuffer() {
            return retryAckOffsetBitmapBuffer;
        }
    }

}
