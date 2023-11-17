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

package com.automq.rocketmq.controller.server.store.impl.cache;

import com.automq.rocketmq.controller.server.store.impl.Helper;
import com.automq.rocketmq.metadata.dao.Topic;
import com.google.protobuf.InvalidProtocolBufferException;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TopicCache {

    private static final Logger LOGGER = LoggerFactory.getLogger(TopicCache.class);

    private final ConcurrentMap<String, Long> names;

    private final ConcurrentMap<Long, apache.rocketmq.controller.v1.Topic> topics;

    public TopicCache() {
        names = new ConcurrentHashMap<>();
        topics = new ConcurrentHashMap<>();
    }

    public apache.rocketmq.controller.v1.Topic byName(String topicName) {
        if (!names.containsKey(topicName)) {
            return null;
        }
        long topicId = names.get(topicName);
        return byId(topicId);
    }

    public apache.rocketmq.controller.v1.Topic byId(long topicId) {
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
                names.put(topic.getName(), topic.getId());
                try {
                    topics.put(topic.getId(), Helper.buildTopic(topic, null));
                } catch (InvalidProtocolBufferException e) {
                    LOGGER.error("Failed to build topic", e);
                }
            }
            case TOPIC_STATUS_DELETED -> {
                names.remove(topic.getName());
                topics.remove(topic.getId());
            }
        }
    }

    public int topicQuantity() {
        return topics.size();
    }
}
