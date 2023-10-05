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

import apache.rocketmq.controller.v1.TopicStatus;
import java.util.Date;
import java.util.Objects;

public class Topic {
    private long id;
    private String name;
    private Integer queueNum;
    private TopicStatus status = TopicStatus.TOPIC_STATUS_ACTIVE;
    private Date createTime;
    private Date updateTime;

    private String acceptMessageTypes;

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
