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
