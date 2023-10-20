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

package com.automq.rocketmq.store.model.message;

import java.util.Objects;

public class TopicQueueId {

    private final long topicId;
    private final int queueId;

    public TopicQueueId(long topicId, int queueId) {
        this.topicId = topicId;
        this.queueId = queueId;
    }

    public static TopicQueueId of(long topicId, int queueId) {
        return new TopicQueueId(topicId, queueId);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        TopicQueueId id = (TopicQueueId) o;
        return topicId == id.topicId && queueId == id.queueId;
    }

    @Override
    public int hashCode() {
        return Objects.hash(topicId, queueId);
    }

    @Override
    public String toString() {
        return "TopicQueueId{" +
            "topicId=" + topicId +
            ", queueId=" + queueId +
            '}';
    }
}
