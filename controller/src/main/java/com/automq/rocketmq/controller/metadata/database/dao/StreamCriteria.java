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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;

public class StreamCriteria {
    List<Long> ids;

    Integer dstNodeId;

    Integer srcNodeId;

    Long groupId;

    Long topicId;

    Integer queueId;

    StreamState state;

    Date updateTime;

    public static class StreamCriteriaBuilder {
        private final StreamCriteria criteria = new StreamCriteria();

        StreamCriteriaBuilder() {
        }

        public StreamCriteriaBuilder addStreamId(long streamId) {
            if (null == criteria.ids) {
                criteria.ids = new ArrayList<>();
            }
            criteria.ids.add(streamId);
            return this;
        }

        public StreamCriteriaBuilder addBatchStreamIds(Collection<Long> ids) {
            if (null == criteria.ids) {
                criteria.ids = new ArrayList<>();
            }
            criteria.ids.addAll(ids);
            return this;
        }

        public StreamCriteriaBuilder withDstNodeId(int dstNodeId) {
            criteria.dstNodeId = dstNodeId;
            return this;
        }

        public StreamCriteriaBuilder withSrcNodeId(int srcNodeId) {
            criteria.srcNodeId = srcNodeId;
            return this;
        }

        public StreamCriteriaBuilder withGroupId(Long groupId) {
            criteria.groupId = groupId;
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

    public List<Long> getIds() {
        return ids;
    }

    public Integer getDstNodeId() {
        return dstNodeId;
    }

    public Integer getSrcNodeId() {
        return srcNodeId;
    }

    public Long getGroupId() {
        return groupId;
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
