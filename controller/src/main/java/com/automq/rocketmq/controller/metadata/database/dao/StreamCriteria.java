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

import apache.rocketmq.controller.v1.StreamState;
import java.util.Date;

public class StreamCriteria {
    Long id;
    Long topicId;
    Integer queueId;
    StreamState state;

    Date updateTime;

    public static class StreamCriteriaBuilder {
        private final StreamCriteria criteria = new StreamCriteria();

        StreamCriteriaBuilder() {
        }

        public StreamCriteriaBuilder withStreamId(long streamId) {
            criteria.id = streamId;
            return this;
        }

        public StreamCriteriaBuilder withTopicId(long topicId) {
            criteria.topicId = topicId;
            return this;
        }

        public StreamCriteriaBuilder withQueueId(int queueId) {
            criteria.queueId = queueId;
            return this;
        }

        public StreamCriteriaBuilder withState(StreamState state) {
            criteria.state = state;
            return this;
        }

        public StreamCriteriaBuilder withUpdateTime(Date updateTime) {
            criteria.updateTime = updateTime;
            return this;
        }

        public StreamCriteria build() {
            return criteria;
        }
    }

    public static StreamCriteriaBuilder newBuilder() {
        return new StreamCriteriaBuilder();
    }

    public Long getId() {
        return id;
    }

    public Long getTopicId() {
        return topicId;
    }

    public Integer getQueueId() {
        return queueId;
    }

    public StreamState getState() {
        return state;
    }

    public Date getUpdateTime() {
        return updateTime;
    }
}
