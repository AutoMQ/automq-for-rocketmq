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
