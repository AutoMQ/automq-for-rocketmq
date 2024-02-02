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

import com.automq.rocketmq.store.model.generated.CheckPoint;
import com.automq.rocketmq.store.model.metadata.ConsumerGroupMetadata;
import java.util.List;
import java.util.Objects;

public class OperationSnapshot {

    private final long snapshotEndOffset;
    private List<CheckPoint> checkPoints;
    private long kvServiceSnapshotVersion;
    private final List<ConsumerGroupMetadata> consumerGroupMetadataList;

    public OperationSnapshot(long snapshotEndOffset, long kvServiceSnapshotVersion,
        List<ConsumerGroupMetadata> consumerGroupMetadataList) {
        this.snapshotEndOffset = snapshotEndOffset;
        this.kvServiceSnapshotVersion = kvServiceSnapshotVersion;
        this.consumerGroupMetadataList = consumerGroupMetadataList;
    }

    public OperationSnapshot(long snapshotEndOffset, List<ConsumerGroupMetadata> consumerGroupMetadataList,
        List<CheckPoint> checkPoints) {
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

    public List<ConsumerGroupMetadata> getConsumerGroupMetadataList() {
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

}
