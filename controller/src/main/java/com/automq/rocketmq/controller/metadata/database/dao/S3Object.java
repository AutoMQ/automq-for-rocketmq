/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package com.automq.rocketmq.controller.metadata.database.dao;

import apache.rocketmq.controller.v1.S3ObjectState;

import java.util.Objects;

public class S3Object {

    Long id;

    Long objectId;

    Long objectSize;
    
    Long preparedTimestamp;

    Long committedTimestamp;

    Long expiredTimestamp;

    Long markedForDeletionTimestamp;

    S3ObjectState state;

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

    public Long getObjectSize() {
        return objectSize;
    }

    public void setObjectSize(Long objectSize) {
        this.objectSize = objectSize;
    }

    public Long getPreparedTimestamp() {
        return preparedTimestamp;
    }

    public void setPreparedTimestamp(Long preparedTimestamp) {
        this.preparedTimestamp = preparedTimestamp;
    }

    public Long getCommittedTimestamp() {
        return committedTimestamp;
    }

    public void setCommittedTimestamp(Long committedTimestamp) {
        this.committedTimestamp = committedTimestamp;
    }

    public Long getExpiredTimestamp() {
        return expiredTimestamp;
    }

    public void setExpiredTimestamp(Long expiredTimestamp) {
        this.expiredTimestamp = expiredTimestamp;
    }

    public Long getMarkedForDeletionTimestamp() {
        return markedForDeletionTimestamp;
    }

    public void setMarkedForDeletionTimestamp(Long markedForDeletionTimestamp) {
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
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        S3Object s3Object = (S3Object) o;
        return Objects.equals(id, s3Object.id) && Objects.equals(objectId, s3Object.objectId) && Objects.equals(objectSize, s3Object.objectSize) && Objects.equals(preparedTimestamp, s3Object.preparedTimestamp) && Objects.equals(committedTimestamp, s3Object.committedTimestamp) && Objects.equals(expiredTimestamp, s3Object.expiredTimestamp) && Objects.equals(markedForDeletionTimestamp, s3Object.markedForDeletionTimestamp) && state == s3Object.state;
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, objectId, objectSize, preparedTimestamp, committedTimestamp, expiredTimestamp, markedForDeletionTimestamp, state);
    }
}
