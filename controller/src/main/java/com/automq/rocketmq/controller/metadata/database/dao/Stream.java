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


package com.automq.rocketmq.controller.metadata.database.dao;

import apache.rocketmq.controller.v1.StreamRole;
import apache.rocketmq.controller.v1.StreamState;
import java.util.Date;

public class Stream {

    private long id;
    /**
     * Epoch of the current stream. Each registration increase it by one when open stream.
     */
    private long epoch;

    private int rangeId;

    private long startOffset;

    private long topicId;

    private int queueId;

    private StreamRole streamRole;

    /**
     * If {@link #streamRole} is {@link StreamRole#STREAM_ROLE_RETRY}, this field represents owner of this retry queue.
     */
    private Long groupId;

    private Integer srcNodeId;

    private Integer dstNodeId;

    private StreamState state;

    private Date createTime;

    private Date updateTime;

    public Stream() {
        epoch = -1L;
        rangeId = -1;
        state = StreamState.UNINITIALIZED;
        startOffset = 0L;
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public long getEpoch() {
        return epoch;
    }

    public void setEpoch(long epoch) {
        this.epoch = epoch;
    }

    public int getRangeId() {
        return rangeId;
    }

    public void setRangeId(int rangeId) {
        this.rangeId = rangeId;
    }

    public long getStartOffset() {
        return startOffset;
    }

    public void setStartOffset(long startOffset) {
        this.startOffset = startOffset;
    }

    public long getTopicId() {
        return topicId;
    }

    public void setTopicId(long topicId) {
        this.topicId = topicId;
    }

    public int getQueueId() {
        return queueId;
    }

    public void setQueueId(int queueId) {
        this.queueId = queueId;
    }

    public StreamRole getStreamRole() {
        return streamRole;
    }

    public void setStreamRole(StreamRole streamRole) {
        this.streamRole = streamRole;
    }

    public Long getGroupId() {
        return groupId;
    }

    public void setGroupId(Long groupId) {
        this.groupId = groupId;
    }

    public Integer getSrcNodeId() {
        return srcNodeId;
    }

    public void setSrcNodeId(Integer srcNodeId) {
        this.srcNodeId = srcNodeId;
    }

    public Integer getDstNodeId() {
        return dstNodeId;
    }

    public void setDstNodeId(Integer dstNodeId) {
        this.dstNodeId = dstNodeId;
    }

    public StreamState getState() {
        return state;
    }

    public void setState(StreamState state) {
        this.state = state;
    }

    public Date getCreateTime() {
        return createTime;
    }

    public void setCreateTime(Date createTime) {
        this.createTime = createTime;
    }

    public Date getUpdateTime() {
        return updateTime;
    }

    public void setUpdateTime(Date updateTime) {
        this.updateTime = updateTime;
    }
}
