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

import apache.rocketmq.controller.v1.GroupStatus;
import apache.rocketmq.controller.v1.GroupType;
import apache.rocketmq.controller.v1.SubscriptionMode;
import java.util.Date;

public class Group {
    private Long id;
    private String name;

    private GroupStatus status = GroupStatus.GROUP_STATUS_ACTIVE;

    private Long deadLetterTopicId;

    private int maxDeliveryAttempt;

    private GroupType groupType = GroupType.GROUP_TYPE_STANDARD;

    private SubscriptionMode subMode = SubscriptionMode.SUB_MODE_UNSPECIFIED;

    private Date createTime;
    private Date updateTime;

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public GroupStatus getStatus() {
        return status;
    }

    public void setStatus(GroupStatus status) {
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

    public Long getDeadLetterTopicId() {
        return deadLetterTopicId;
    }

    public void setDeadLetterTopicId(Long deadLetterTopicId) {
        this.deadLetterTopicId = deadLetterTopicId;
    }

    public int getMaxDeliveryAttempt() {
        return maxDeliveryAttempt;
    }

    public void setMaxDeliveryAttempt(int maxDeliveryAttempt) {
        this.maxDeliveryAttempt = maxDeliveryAttempt;
    }

    public GroupType getGroupType() {
        return groupType;
    }

    public void setGroupType(GroupType groupType) {
        this.groupType = groupType;
    }

    public SubscriptionMode getSubMode() {
        return subMode;
    }

    public void setSubMode(SubscriptionMode subMode) {
        this.subMode = subMode;
    }
}
