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

package com.automq.rocketmq.metadata.dao;

import java.util.Date;
import java.util.Objects;

public class S3StreamSetObject {

    Long objectId;

    Integer nodeId;

    Long objectSize;

    Long sequenceId;

    String subStreams;

    Date baseDataTimestamp;

    Date committedTimestamp;

    Date createdTimestamp = new Date();

    public Long getObjectId() {
        return objectId;
    }

    public void setObjectId(Long objectId) {
        this.objectId = objectId;
    }

    public Integer getNodeId() {
        return nodeId;
    }

    public void setNodeId(Integer nodeId) {
        this.nodeId = nodeId;
    }

    public Long getObjectSize() {
        return objectSize;
    }

    public void setObjectSize(Long objectSize) {
        this.objectSize = objectSize;
    }

    public Long getSequenceId() {
        return sequenceId;
    }

    public void setSequenceId(Long sequenceId) {
        this.sequenceId = sequenceId;
    }

    public String getSubStreams() {
        return subStreams;
    }

    public void setSubStreams(String subStreams) {
        this.subStreams = subStreams;
    }

    public Date getBaseDataTimestamp() {
        return baseDataTimestamp;
    }

    public void setBaseDataTimestamp(Date baseDataTimestamp) {
        this.baseDataTimestamp = baseDataTimestamp;
    }

    public Date getCommittedTimestamp() {
        return committedTimestamp;
    }

    public void setCommittedTimestamp(Date committedTimestamp) {
        this.committedTimestamp = committedTimestamp;
    }

    public Date getCreatedTimestamp() {
        return createdTimestamp;
    }

    public void setCreatedTimestamp(Date createdTimestamp) {
        this.createdTimestamp = createdTimestamp;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        S3StreamSetObject that = (S3StreamSetObject) o;
        return Objects.equals(objectId, that.objectId) && Objects.equals(nodeId, that.nodeId) && Objects.equals(objectSize, that.objectSize) && Objects.equals(sequenceId, that.sequenceId) && Objects.equals(subStreams, that.subStreams) && Objects.equals(baseDataTimestamp, that.baseDataTimestamp) && Objects.equals(committedTimestamp, that.committedTimestamp) && Objects.equals(createdTimestamp, that.createdTimestamp);
    }

    @Override
    public int hashCode() {
        return Objects.hash(objectId, nodeId, objectSize, sequenceId, subStreams, baseDataTimestamp, committedTimestamp, createdTimestamp);
    }
}
