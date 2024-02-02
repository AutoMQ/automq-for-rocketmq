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

public class GroupCriteria {
    private Long id;
    private String name;
    private GroupStatus status;
    private Long topicId;
    private Date updateTime;

    private SubscriptionMode subMode;

    private GroupType groupType;

    public static class GroupCriteriaBuilder {
        private final GroupCriteria criteria = new GroupCriteria();

        public GroupCriteriaBuilder setGroupId(Long id) {
            criteria.setId(id);
            return this;
        }

        public GroupCriteriaBuilder setDeadLetterTopicId(Long topicId) {
            criteria.setTopicId(topicId);
            return this;
        }

        public GroupCriteriaBuilder setGroupName(String name) {
            criteria.setName(name);
            return this;
        }

        public GroupCriteriaBuilder setStatus(GroupStatus status) {
            criteria.setStatus(status);
            return this;
        }

        public GroupCriteriaBuilder setLastUpdateTime(Date date) {
            criteria.setUpdateTime(date);
            return this;
        }

        public GroupCriteriaBuilder setSubMode(SubscriptionMode subMode) {
            criteria.subMode = subMode;
            return this;
        }

        public GroupCriteriaBuilder setGroupType(GroupType groupType) {
            criteria.groupType = groupType;
            return this;
        }

        public GroupCriteria build() {
            return criteria;
        }
    }

    public static GroupCriteriaBuilder newBuilder() {
        return new GroupCriteriaBuilder();
    }

    public Long getId() {
        return id;
    }

    void setId(Long id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    void setName(String name) {
        this.name = name;
    }

    public GroupStatus getStatus() {
        return status;
    }

    void setStatus(GroupStatus status) {
        this.status = status;
    }

    public Long getTopicId() {
        return topicId;
    }

    void setTopicId(Long topicId) {
        this.topicId = topicId;
    }

    public SubscriptionMode getSubMode() {
        return subMode;
    }

    public GroupType getGroupType() {
        return groupType;
    }

    public Date getUpdateTime() {
        return updateTime;
    }

    void setUpdateTime(Date updateTime) {
        this.updateTime = updateTime;
    }
}
