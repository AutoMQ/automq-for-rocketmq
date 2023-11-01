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
