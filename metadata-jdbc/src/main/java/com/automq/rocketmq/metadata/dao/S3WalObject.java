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

public class S3WalObject {

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
        S3WalObject that = (S3WalObject) o;
        return Objects.equals(objectId, that.objectId) && Objects.equals(nodeId, that.nodeId) && Objects.equals(objectSize, that.objectSize) && Objects.equals(sequenceId, that.sequenceId) && Objects.equals(subStreams, that.subStreams) && Objects.equals(baseDataTimestamp, that.baseDataTimestamp) && Objects.equals(committedTimestamp, that.committedTimestamp) && Objects.equals(createdTimestamp, that.createdTimestamp);
    }

    @Override
    public int hashCode() {
        return Objects.hash(objectId, nodeId, objectSize, sequenceId, subStreams, baseDataTimestamp, committedTimestamp, createdTimestamp);
    }
}
