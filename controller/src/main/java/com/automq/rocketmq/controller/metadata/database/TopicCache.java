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

package com.automq.rocketmq.controller.metadata.database;

import com.automq.rocketmq.controller.metadata.database.dao.Topic;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class TopicCache {

    private final ConcurrentMap<String, Long> names;

    private final ConcurrentMap<Long, Topic> topics;

    public TopicCache() {
        names = new ConcurrentHashMap<>();
        topics = new ConcurrentHashMap<>();
    }

    public Topic byName(String topicName) {
        if (!names.containsKey(topicName)) {
            return null;
        }

        Long topicId = names.get(topicName);
        return byId(topicId);
    }

    public Topic byId(long topicId) {
        return topics.get(topicId);
    }

    public void apply(List<Topic> topics) {
        if (null == topics || topics.isEmpty()) {
            return;
        }

        for (Topic topic : topics) {
            cacheItem(topic);
        }
    }

    private void cacheItem(Topic topic) {
        switch (topic.getStatus()) {
            case TOPIC_STATUS_ACTIVE -> {
                Long prev = names.put(topic.getName(), topic.getId());
                assert null == prev;
                Topic prevTopic = topics.put(topic.getId(), topic);
                assert null == prevTopic;
            }
            case TOPIC_STATUS_DELETED -> {
                names.remove(topic.getName());
                topics.remove(topic.getId());
            }
        }
    }
}
