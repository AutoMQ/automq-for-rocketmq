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

import apache.rocketmq.controller.v1.TopicStatus;
import com.automq.rocketmq.metadata.dao.Topic;
import java.util.List;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class TopicCacheTest {

    @Test
    public void testCache() {
        TopicCache cache = new TopicCache();
        Topic topic = new Topic();
        topic.setId(1);
        topic.setName("T1");
        topic.setRetentionHours(3);
        topic.setAcceptMessageTypes("{}");
        topic.setQueueNum(1);
        topic.setStatus(TopicStatus.TOPIC_STATUS_DELETED);
        cache.apply(List.of(topic));

        Assertions.assertNull(cache.byName("T2"));

        topic.setStatus(TopicStatus.TOPIC_STATUS_ACTIVE);
        cache.apply(List.of(topic));
        Assertions.assertNotNull(cache.byName("T1"));

    }

}