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

public class S3WALObject {

    long objectId;

    int brokerId;

    long sequenceId;

    String subStreams;

    long baseDataTimestamp;

    long committedTimestamp;

    public long getObjectId() {
        return objectId;
    }

    public void setObjectId(long objectId) {
        this.objectId = objectId;
    }

    public int getBrokerId() {
        return brokerId;
    }

    public void setBrokerId(int brokerId) {
        this.brokerId = brokerId;
    }

    public long getSequenceId() {
        return sequenceId;
    }

    public void setSequenceId(long sequenceId) {
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
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        S3WALObject that = (S3WALObject) o;
        return objectId == that.objectId && brokerId == that.brokerId && sequenceId == that.sequenceId && baseDataTimestamp == that.baseDataTimestamp && committedTimestamp == that.committedTimestamp && Objects.equals(subStreams, that.subStreams);
    }

    @Override
    public int hashCode() {
        return Objects.hash(objectId, brokerId, sequenceId, subStreams, baseDataTimestamp, committedTimestamp);
    }
}
