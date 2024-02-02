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

import apache.rocketmq.controller.v1.StreamRole;
import apache.rocketmq.controller.v1.StreamState;
import java.util.Date;

public class Stream {

    private Long id;

    /**
     * Epoch of the current stream. Each registration increase it by one when open stream.
     */
    private Long epoch;

    private Integer rangeId;

    private Long startOffset;

    private Long topicId;

    private Integer queueId;

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

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public Long getEpoch() {
        return epoch;
    }

    public void setEpoch(Long epoch) {
        this.epoch = epoch;
    }

    public Integer getRangeId() {
        return rangeId;
    }

    public void setRangeId(Integer rangeId) {
        this.rangeId = rangeId;
    }

    public Long getStartOffset() {
        return startOffset;
    }

    public void setStartOffset(Long startOffset) {
        this.startOffset = startOffset;
    }

    public Long getTopicId() {
        return topicId;
    }

    public void setTopicId(Long topicId) {
        this.topicId = topicId;
    }

    public Integer getQueueId() {
        return queueId;
    }

    public void setQueueId(Integer queueId) {
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
