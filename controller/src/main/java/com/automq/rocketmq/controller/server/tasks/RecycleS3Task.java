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

package com.automq.rocketmq.controller.server.tasks;

import apache.rocketmq.controller.v1.TopicStatus;
import com.automq.rocketmq.controller.exception.ControllerException;
import com.automq.rocketmq.controller.MetadataStore;
import com.automq.rocketmq.metadata.dao.Stream;
import com.automq.rocketmq.metadata.dao.StreamCriteria;
import com.automq.rocketmq.metadata.dao.Topic;
import com.automq.rocketmq.metadata.mapper.S3ObjectMapper;
import com.automq.rocketmq.metadata.mapper.S3StreamObjectMapper;
import com.automq.rocketmq.metadata.mapper.StreamMapper;
import com.automq.rocketmq.metadata.mapper.TopicMapper;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import org.apache.ibatis.session.SqlSession;

public class RecycleS3Task extends ControllerTask {
    public RecycleS3Task(MetadataStore metadataStore) {
        super(metadataStore);
    }

    @Override
    public void process() throws ControllerException {
        if (!metadataStore.isLeader()) {
            return;
        }

        try (SqlSession session = metadataStore.openSession()) {
            TopicMapper topicMapper = session.getMapper(TopicMapper.class);
            StreamMapper streamMapper = session.getMapper(StreamMapper.class);
            S3StreamObjectMapper streamObjectMapper = session.getMapper(S3StreamObjectMapper.class);
            S3ObjectMapper s3ObjectMapper = session.getMapper(S3ObjectMapper.class);

            List<Topic> topics = topicMapper.list(TopicStatus.TOPIC_STATUS_ACTIVE, null);

            List<Long> recyclable = new ArrayList<>();

            for (Topic topic : topics) {
                Calendar calendar = Calendar.getInstance();
                calendar.add(Calendar.HOUR, -topic.getRetentionHours());
                Date threshold = calendar.getTime();
                StreamCriteria criteria = StreamCriteria.newBuilder()
                    .withTopicId(topic.getId())
                    .build();
                List<Long> streamIds = streamMapper.byCriteria(criteria)
                    .stream()
                    .map(Stream::getId)
                    .toList();
                recyclable.addAll(streamObjectMapper.recyclable(streamIds, threshold));
            }

            metadataStore.getDataStore().batchDeleteS3Objects(recyclable)
                .whenComplete((list, e) -> {
                    if (null != e) {
                        LOGGER.error("DataStore failed to delete S3 objects", e);
                        return;
                    }
                    s3ObjectMapper.batchDelete(list);
                    streamObjectMapper.batchDelete(list);
                    session.commit();
                });
        }

    }
}
