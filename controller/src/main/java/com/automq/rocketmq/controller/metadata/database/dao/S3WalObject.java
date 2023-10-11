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

public class S3WalObject {

    Long objectId;

    Integer nodeId;

    Long objectSize;

    Long sequenceId;

    String subStreams;

    long baseDataTimestamp;

    long committedTimestamp;

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
        S3WalObject object = (S3WalObject) o;
        return baseDataTimestamp == object.baseDataTimestamp && committedTimestamp == object.committedTimestamp
            && objectId.equals(object.objectId) && nodeId.equals(object.nodeId)
            && objectSize.equals(object.objectSize) && sequenceId.equals(object.sequenceId)
            && Objects.equals(subStreams, object.subStreams);
    }

    @Override
    public int hashCode() {
        return Objects.hash(objectId, nodeId, objectSize, sequenceId, subStreams, baseDataTimestamp, committedTimestamp);
    }
}
