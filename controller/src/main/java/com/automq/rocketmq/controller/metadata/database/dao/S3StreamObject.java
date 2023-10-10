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

import java.util.Objects;

public class S3StreamObject {

    private Long id;

    private Long objectId;

    private Long streamId;

    private Long startOffset;

    private Long endOffset;

    private Long objectSize;

    private long baseDataTimestamp;

    private long committedTimestamp;

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

    public long getBaseDataTimestamp() {
        return baseDataTimestamp;
    }

    public void setBaseDataTimestamp(long baseDataTimestamp) {
        this.baseDataTimestamp = baseDataTimestamp;
    }

    public long getCommittedTimestamp() {
        return committedTimestamp;
    }

    public void setCommittedTimestamp(long committedTimestamp) {
        this.committedTimestamp = committedTimestamp;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        S3StreamObject object = (S3StreamObject) o;
        return baseDataTimestamp == object.baseDataTimestamp && committedTimestamp == object.committedTimestamp
            && Objects.equals(id, object.id) && Objects.equals(objectId, object.objectId)
            && Objects.equals(streamId, object.streamId) && Objects.equals(startOffset, object.startOffset)
            && Objects.equals(endOffset, object.endOffset) && Objects.equals(objectSize, object.objectSize);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, objectId, streamId, startOffset, endOffset, objectSize, baseDataTimestamp, committedTimestamp);
    }
}
