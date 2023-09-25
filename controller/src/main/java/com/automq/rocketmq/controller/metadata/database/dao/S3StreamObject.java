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

    long id;

    long objectId;

    long streamId;

    long startOffset;

    long endOffset;

    long objectSize;

    long baseDataTimestamp;

    long committedTimestamp;

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public long getObjectId() {
        return objectId;
    }

    public void setObjectId(long objectId) {
        this.objectId = objectId;
    }

    public long getStreamId() {
        return streamId;
    }

    public void setStreamId(long streamId) {
        this.streamId = streamId;
    }

    public long getStartOffset() {
        return startOffset;
    }

    public void setStartOffset(long startOffset) {
        this.startOffset = startOffset;
    }

    public long getEndOffset() {
        return endOffset;
    }

    public void setEndOffset(long endOffset) {
        this.endOffset = endOffset;
    }

    public long getObjectSize() {
        return objectSize;
    }

    public void setObjectSize(long objectSize) {
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
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        S3StreamObject that = (S3StreamObject) o;
        return id == that.id && objectId == that.objectId && streamId == that.streamId && startOffset == that.startOffset && endOffset == that.endOffset && objectSize == that.objectSize && baseDataTimestamp == that.baseDataTimestamp && committedTimestamp == that.committedTimestamp;
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, objectId, streamId, startOffset, endOffset, objectSize, baseDataTimestamp, committedTimestamp);
    }
}
