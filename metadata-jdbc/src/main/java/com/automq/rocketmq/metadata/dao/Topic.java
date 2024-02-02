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

import apache.rocketmq.controller.v1.TopicStatus;
import java.util.Date;
import java.util.Objects;

public class Topic {
    private long id;
    private String name;
    private Integer queueNum;

    private Integer retentionHours;

    private TopicStatus status = TopicStatus.TOPIC_STATUS_ACTIVE;
    private Date createTime;
    private Date updateTime;

    private String acceptMessageTypes;

    public Topic() {
        retentionHours = 72;
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Integer getQueueNum() {
        return queueNum;
    }

    public void setQueueNum(Integer queueNum) {
        this.queueNum = queueNum;
    }

    public Integer getRetentionHours() {
        return retentionHours;
    }

    public void setRetentionHours(Integer retentionHours) {
        this.retentionHours = retentionHours;
    }

    public TopicStatus getStatus() {
        return status;
    }

    public void setStatus(TopicStatus status) {
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

    public String getAcceptMessageTypes() {
        return acceptMessageTypes;
    }

    public void setAcceptMessageTypes(String acceptMessageTypes) {
        this.acceptMessageTypes = acceptMessageTypes;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        Topic topic = (Topic) o;
        return id == topic.id && Objects.equals(queueNum, topic.queueNum) && name.equals(topic.name) && status == topic.status;
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, name, queueNum, status);
    }
}
