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
