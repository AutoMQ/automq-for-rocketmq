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

package com.automq.rocketmq.controller.server.store.impl;

import com.automq.rocketmq.controller.MetadataStore;
import com.automq.rocketmq.metadata.dao.S3Object;
import com.automq.rocketmq.metadata.dao.S3ObjectCriteria;
import com.automq.rocketmq.metadata.mapper.S3ObjectMapper;
import com.automq.rocketmq.metadata.mapper.S3StreamObjectMapper;
import java.util.ArrayList;
import java.util.List;
import org.apache.ibatis.session.SqlSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StreamManager {
    private static final Logger LOGGER = LoggerFactory.getLogger(TopicManager.class);

    private final MetadataStore metadataStore;

    public StreamManager(MetadataStore metadataStore) {
        this.metadataStore = metadataStore;
    }

    public void deleteStream(long streamId) {
        try (SqlSession session = metadataStore.openSession()) {
            S3StreamObjectMapper streamObjectMapper = session.getMapper(S3StreamObjectMapper.class);
            streamObjectMapper.delete(null, streamId, null);
            S3ObjectMapper s3ObjectMapper = session.getMapper(S3ObjectMapper.class);
            List<S3Object> list = s3ObjectMapper.list(null, streamId);
            List<Long> objectIds = new ArrayList<>();
            for (S3Object s3Object : list) {
                objectIds.add(s3Object.getId());
            }

            while (!objectIds.isEmpty()) {
                List<Long> deleted = metadataStore.getDataStore().batchDeleteS3Objects(objectIds).join();
                objectIds.removeAll(deleted);
                if (!deleted.isEmpty()) {
                    LOGGER.info("DataStore batch deleted S3 objects having object-id-list={}", deleted);
                    s3ObjectMapper.deleteByCriteria(S3ObjectCriteria.newBuilder().addObjectIds(deleted).build());
                }
            }

            s3ObjectMapper.deleteByCriteria(S3ObjectCriteria.newBuilder().withStreamId(streamId).build());

            session.commit();
        }
    }
}
