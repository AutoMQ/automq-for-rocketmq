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

import apache.rocketmq.controller.v1.AcceptTypes;
import apache.rocketmq.controller.v1.MessageType;
import apache.rocketmq.controller.v1.TopicStatus;
import com.automq.rocketmq.controller.metadata.database.dao.Topic;
import com.automq.rocketmq.controller.metadata.database.mapper.TopicMapper;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.google.gson.Gson;
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
            TopicStatus status = TopicStatus.TOPIC_STATUS_DELETED;
            String acceptMessageTypes = "[\"NORMAL\",\"FIFO\",\"DELAY\"]";

            topic.setName(name);
            topic.setQueueNum(queueNum);
            topic.setStatus(status);
            topic.setAcceptMessageTypes(acceptMessageTypes);
            int affectedRows = topicMapper.create(topic);

            Assertions.assertEquals(1, affectedRows);
            Assertions.assertTrue(topic.getId() > 0);

            Topic got = topicMapper.get(topic.getId(), null);
            Assertions.assertEquals(topic, got);

            affectedRows = topicMapper.updateStatusById(topic.getId(), TopicStatus.TOPIC_STATUS_ACTIVE);
            Assertions.assertEquals(1, affectedRows);

            got = topicMapper.get(topic.getId(),null);
            Assertions.assertEquals(TopicStatus.TOPIC_STATUS_ACTIVE, got.getStatus());

            List<Topic> topics = topicMapper.list(null, null);
            Assertions.assertEquals(1, topics.size());

            topics = topicMapper.list(TopicStatus.TOPIC_STATUS_ACTIVE, null);
            Assertions.assertEquals(1, topics.size());

            topics = topicMapper.list(TopicStatus.TOPIC_STATUS_DELETED, null);
            Assertions.assertEquals(0, topics.size());

            Topic topic1 = topicMapper.get(topic.getId(), null);
            List<MessageType> messageTypeList = new ArrayList<>();
            messageTypeList.add(MessageType.NORMAL);
            messageTypeList.add(MessageType.FIFO);
            messageTypeList.add(MessageType.DELAY);
            Gson gson = new Gson();
            String expect = gson.toJson(messageTypeList);
            apache.rocketmq.controller.v1.Topic topic2 = apache.rocketmq.controller.v1.Topic.newBuilder()
                    .setTopicId(topic1.getId())
                    .setName(topic1.getName())
                    .setCount(topic1.getQueueNum())
                    .setAcceptTypes(AcceptTypes.newBuilder()
                        .addTypes(MessageType.NORMAL)
                        .addTypes(MessageType.FIFO)
                        .addTypes(MessageType.DELAY)
                        .build())
                    .build();

            String updateMessageType = "[\"NORMAL\",\"DELAY\"]";
            Topic topic3 = new Topic();
            topic3.setId(topic1.getId());
            topic3.setAcceptMessageTypes(updateMessageType);
            topicMapper.update(topic3);

            Topic topic4 = topicMapper.get(topic.getId(), null);
            Assertions.assertEquals(updateMessageType, topic4.getAcceptMessageTypes());
        }
    }
}
