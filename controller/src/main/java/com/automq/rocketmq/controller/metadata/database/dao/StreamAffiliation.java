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

import java.util.Date;

/**
 * Manage stream/queue relationship and its assignment to node.
 *
 * When migrating queues among nodes on demand, the affiliated streams should be reconciled to the same node where its
 * data/ops streams live.
 */
public class StreamAffiliation {
    private long streamId;

    private long topicId;

    private int queueId;

    private StreamRole streamRole;

    /**
     * If {@link #streamRole} is {@link StreamRole#RETRY}, this field represents owner of this retry queue.
     */
    private long groupId;

    private int srcNodeId;

    private int dstNodeId;

    private AssignmentStatus status;

    private Date createTime;

    private Date updateTime;

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

    public long getStreamId() {
        return streamId;
    }

    public void setStreamId(long streamId) {
        this.streamId = streamId;
    }

    public StreamRole getStreamRole() {
        return streamRole;
    }

    public void setStreamRole(StreamRole streamRole) {
        this.streamRole = streamRole;
    }

    public long getGroupId() {
        return groupId;
    }

    public void setGroupId(long groupId) {
        this.groupId = groupId;
    }

    public int getSrcNodeId() {
        return srcNodeId;
    }

    public void setSrcNodeId(int srcNodeId) {
        this.srcNodeId = srcNodeId;
    }

    public int getDstNodeId() {
        return dstNodeId;
    }

    public void setDstNodeId(int dstNodeId) {
        this.dstNodeId = dstNodeId;
    }

    public AssignmentStatus getStatus() {
        return status;
    }

    public void setStatus(AssignmentStatus status) {
        this.status = status;
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
