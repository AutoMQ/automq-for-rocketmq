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

public class S3StreamObject {

    private Long id;

    private Long objectId;

    private Long streamId;

    private Long startOffset;

    private Long endOffset;

    private Long objectSize;

    private Date baseDataTimestamp;

    private Date committedTimestamp;

    private Date createdTimestamp = new Date();

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public Long getObjectId() {
        return objectId;
    }

    public void setObjectId(Long objectId) {
        this.objectId = objectId;
    }

    public Long getStreamId() {
        return streamId;
    }

    public void setStreamId(Long streamId) {
        this.streamId = streamId;
    }

    public Long getStartOffset() {
        return startOffset;
    }

    public void setStartOffset(Long startOffset) {
        this.startOffset = startOffset;
    }

    public Long getEndOffset() {
        return endOffset;
    }

    public void setEndOffset(Long endOffset) {
        this.endOffset = endOffset;
    }

    public Long getObjectSize() {
        return objectSize;
    }

    public void setObjectSize(Long objectSize) {
        this.objectSize = objectSize;
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
        S3StreamObject that = (S3StreamObject) o;
        return Objects.equals(id, that.id) && Objects.equals(objectId, that.objectId) && Objects.equals(streamId, that.streamId) && Objects.equals(startOffset, that.startOffset) && Objects.equals(endOffset, that.endOffset) && Objects.equals(objectSize, that.objectSize) && Objects.equals(baseDataTimestamp, that.baseDataTimestamp) && Objects.equals(committedTimestamp, that.committedTimestamp) && Objects.equals(createdTimestamp, that.createdTimestamp);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, objectId, streamId, startOffset, endOffset, objectSize, baseDataTimestamp, committedTimestamp, createdTimestamp);
    }
}
