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

import apache.rocketmq.controller.v1.AssignmentStatus;
import java.util.Date;

public class QueueAssignment {
    private long topicId;

    private int queueId;

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
