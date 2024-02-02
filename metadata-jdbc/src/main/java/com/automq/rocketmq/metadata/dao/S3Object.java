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

import apache.rocketmq.controller.v1.S3ObjectState;

import java.util.Date;
import java.util.Objects;

public class S3Object {

    Long id;

    Long objectSize;

    Long streamId;
    
    Date preparedTimestamp = new Date();

    Date committedTimestamp;

    Date expiredTimestamp;

    Date markedForDeletionTimestamp;

    S3ObjectState state = S3ObjectState.BOS_PREPARED;

    public S3Object() {
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public Long getObjectSize() {
        return objectSize;
    }

    public void setObjectSize(Long objectSize) {
        this.objectSize = objectSize;
    }

    public Long getStreamId() {
        return streamId;
    }

    public void setStreamId(Long streamId) {
        this.streamId = streamId;
    }

    public Date getPreparedTimestamp() {
        return preparedTimestamp;
    }

    public void setPreparedTimestamp(Date preparedTimestamp) {
        this.preparedTimestamp = preparedTimestamp;
    }

    public Date getCommittedTimestamp() {
        return committedTimestamp;
    }

    public void setCommittedTimestamp(Date committedTimestamp) {
        this.committedTimestamp = committedTimestamp;
    }

    public Date getExpiredTimestamp() {
        return expiredTimestamp;
    }

    public void setExpiredTimestamp(Date expiredTimestamp) {
        this.expiredTimestamp = expiredTimestamp;
    }

    public Date getMarkedForDeletionTimestamp() {
        return markedForDeletionTimestamp;
    }

    public void setMarkedForDeletionTimestamp(Date markedForDeletionTimestamp) {
        this.markedForDeletionTimestamp = markedForDeletionTimestamp;
    }

    public S3ObjectState getState() {
        return state;
    }

    public void setState(S3ObjectState state) {
        this.state = state;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        S3Object s3Object = (S3Object) o;
        return Objects.equals(id, s3Object.id) && Objects.equals(objectSize, s3Object.objectSize) && Objects.equals(streamId, s3Object.streamId) && state == s3Object.state;
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, objectSize, streamId, state);
    }
}
