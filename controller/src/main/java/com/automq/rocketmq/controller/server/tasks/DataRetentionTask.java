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

import apache.rocketmq.common.v1.Code;
import apache.rocketmq.controller.v1.StreamState;
import apache.rocketmq.controller.v1.TopicStatus;
import com.automq.rocketmq.controller.MetadataStore;
import com.automq.rocketmq.common.exception.ControllerException;
import com.automq.rocketmq.metadata.dao.S3StreamObject;
import com.automq.rocketmq.metadata.dao.Stream;
import com.automq.rocketmq.metadata.dao.StreamCriteria;
import com.automq.rocketmq.metadata.dao.Topic;
import com.automq.rocketmq.metadata.mapper.S3StreamObjectMapper;
import com.automq.rocketmq.metadata.mapper.StreamMapper;
import com.automq.rocketmq.metadata.mapper.TopicMapper;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.apache.ibatis.session.SqlSession;

/**
 * Scan and trim streams to enforce topic retention policy
 */
public class DataRetentionTask extends ControllerTask {
    public DataRetentionTask(MetadataStore metadataStore) {
        super(metadataStore);
    }

    @Override
    public void process() throws ControllerException {
        // Determine offset to trim stream up to
        final Map<Long, Long> trimTo = new HashMap<>();

        // Recyclable S3 Object IDs
        List<Long> recyclable = new ArrayList<>();

        try (SqlSession session = metadataStore.openSession()) {
            TopicMapper topicMapper = session.getMapper(TopicMapper.class);
            StreamMapper streamMapper = session.getMapper(StreamMapper.class);
            S3StreamObjectMapper streamObjectMapper = session.getMapper(S3StreamObjectMapper.class);
            List<Topic> topics = topicMapper.list(TopicStatus.TOPIC_STATUS_ACTIVE, null);
            for (Topic topic : topics) {
                Calendar calendar = Calendar.getInstance();
                calendar.add(Calendar.HOUR, -topic.getRetentionHours());
                Date threshold = calendar.getTime();
                StreamCriteria criteria = StreamCriteria.newBuilder()
                    .withTopicId(topic.getId())
                    .withState(StreamState.OPEN)
                    .withDstNodeId(metadataStore.config().nodeId())
                    .build();
                List<Long> streamIds = streamMapper.byCriteria(criteria)
                    .stream()
                    .map(Stream::getId)
                    .toList();

                if (streamIds.isEmpty()) {
                    continue;
                }

                // Lookup and add recyclable S3 object IDs
                List<S3StreamObject> list = streamObjectMapper.recyclable(streamIds, threshold);
                recyclable.addAll(list.stream().mapToLong(S3StreamObject::getObjectId).boxed().toList());

                list.forEach(s3StreamObject -> {
                    trimTo.computeIfAbsent(s3StreamObject.getStreamId(), streamId -> s3StreamObject.getEndOffset());
                    trimTo.computeIfPresent(s3StreamObject.getStreamId(), (streamId, prev) -> {
                        if (prev < s3StreamObject.getEndOffset()) {
                            return s3StreamObject.getEndOffset();
                        }
                        return prev;
                    });
                });
            }

            if (recyclable.isEmpty()) {
                LOGGER.debug("No recyclable S3 objects");
                return;
            }
        } catch (Exception e) {
            LOGGER.error("Failed to screen recyclable S3 Objects", e);
            throw new ControllerException(Code.INTERNAL_VALUE, e);
        }

        // Request data store to trim streams
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        trimTo.forEach((streamId, offset) -> {
            try {
                futures.add(metadataStore.getDataStore().trimStream(streamId, offset));
                LOGGER.debug("Trim stream[stream-id={}] to {}", streamId, offset);
            } catch (Throwable e) {
                LOGGER.warn("DataStore fails to trim stream[stream-id={}] to {}", streamId, offset, e);
            }
        });

        for (CompletableFuture<Void> future : futures) {
            try {
                future.join();
            } catch (Throwable e) {
                LOGGER.warn("DataStore fails to trim stream", e);
            }
        }
    }
}
