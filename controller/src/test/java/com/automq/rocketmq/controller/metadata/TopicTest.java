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

package com.automq.rocketmq.controller.metadata;

import com.automq.rocketmq.controller.metadata.database.dao.Topic;
import com.automq.rocketmq.controller.metadata.database.dao.TopicStatus;
import com.automq.rocketmq.controller.metadata.database.mapper.TopicMapper;
import java.io.IOException;
import java.util.List;
import org.apache.ibatis.session.SqlSession;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TopicTest extends DatabaseTestBase {

    @Test
    public void testTopicCRUD() throws IOException {
        try (SqlSession session = getSessionFactory().openSession()) {
            TopicMapper topicMapper = session.getMapper(TopicMapper.class);
            Topic topic = new Topic();
            String name = "T1";
            int queueNum = 16;
            TopicStatus status = TopicStatus.DELETED;

            topic.setName(name);
            topic.setQueueNum(queueNum);
            topic.setStatus(status);
            int affectedRows = topicMapper.create(topic);

            Assertions.assertEquals(1, affectedRows);
            Assertions.assertTrue(topic.getId() > 0);

            Topic got = topicMapper.getById(topic.getId());
            Assertions.assertEquals(topic, got);

            affectedRows = topicMapper.updateStatusById(topic.getId(), TopicStatus.ACTIVE);
            Assertions.assertEquals(1, affectedRows);

            got = topicMapper.getById(topic.getId());
            Assertions.assertEquals(TopicStatus.ACTIVE, got.getStatus());

            List<Topic> topics = topicMapper.list(null, null);
            Assertions.assertEquals(1, topics.size());

            topics = topicMapper.list(TopicStatus.ACTIVE, null);
            Assertions.assertEquals(1, topics.size());

            topics = topicMapper.list(TopicStatus.DELETED, null);
            Assertions.assertEquals(0, topics.size());
        }
    }
}
