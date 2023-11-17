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