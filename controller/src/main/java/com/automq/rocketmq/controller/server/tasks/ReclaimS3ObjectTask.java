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

import apache.rocketmq.controller.v1.S3ObjectState;
import com.automq.rocketmq.controller.MetadataStore;
import com.automq.rocketmq.common.exception.ControllerException;
import com.automq.rocketmq.metadata.dao.S3Object;
import com.automq.rocketmq.metadata.dao.S3ObjectCriteria;
import com.automq.rocketmq.metadata.mapper.S3ObjectMapper;
import com.automq.rocketmq.metadata.mapper.S3StreamObjectMapper;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.ibatis.session.SqlSession;

public class ReclaimS3ObjectTask extends ControllerTask {

    public ReclaimS3ObjectTask(MetadataStore metadataStore) {
        super(metadataStore);
    }

    @Override
    public void process() throws ControllerException {
        try (SqlSession session = metadataStore.openSession()) {
            if (!metadataStore.isLeader()) {
                return;
            }

            if (!metadataStore.maintainLeadershipWithSharedLock(session)) {
                return;
            }

            S3StreamObjectMapper streamObjectMapper = session.getMapper(S3StreamObjectMapper.class);
            S3ObjectMapper s3ObjectMapper = session.getMapper(S3ObjectMapper.class);
            rollbackExpiredS3Object(session, s3ObjectMapper);

            S3ObjectCriteria criteria = S3ObjectCriteria.newBuilder()
                .withState(S3ObjectState.BOS_WILL_DELETE)
                .build();
            List<S3Object> s3Objects = s3ObjectMapper.list(criteria);
            List<Long> ids = s3Objects.stream().mapToLong(S3Object::getId).boxed().toList();
            if (!ids.isEmpty()) {
                List<Long> result = metadataStore.getDataStore().batchDeleteS3Objects(ids).join();
                HashSet<Long> expired = new HashSet<>(ids);
                result.forEach(expired::remove);
                LOGGER.info("Reclaim {} S3 objects: deleted: [{}], expired but not deleted: [{}]",
                    result.size(),
                    result.stream().map(String::valueOf).collect(Collectors.joining(", ")),
                    expired.stream().map(String::valueOf).collect(Collectors.joining(", "))
                );

                if (!result.isEmpty()) {
                    s3ObjectMapper.deleteByCriteria(S3ObjectCriteria.newBuilder().addObjectIds(result).build());
                    streamObjectMapper.batchDelete(result);
                }
            }
            session.commit();
        }
    }

    public void rollbackExpiredS3Object(SqlSession session, S3ObjectMapper s3ObjectMapper) {
        S3ObjectCriteria criteria = S3ObjectCriteria.newBuilder()
            .withState(S3ObjectState.BOS_PREPARED)
            .withExpiredTimestamp(new Date())
            .build();
        List<S3Object> toRollback = s3ObjectMapper.list(criteria);
        if (!toRollback.isEmpty()) {
            List<Long> toRollbackObjectIds = toRollback.stream().map(S3Object::getId).collect(Collectors.toList());
            LOGGER.info("Going to rollback expired prepared S3 Object: {}", toRollbackObjectIds);
            try {
                List<Long> deleted = metadataStore.getDataStore().batchDeleteS3Objects(toRollbackObjectIds).join();
                if (deleted.size() < toRollbackObjectIds.size()) {
                    LOGGER.warn("DataStore failed to delete all expired prepared S3 Object. Expired={}, Deleted={}",
                        toRollbackObjectIds, deleted);
                }
                S3ObjectCriteria deleteCriteria = S3ObjectCriteria.newBuilder()
                    .withState(S3ObjectState.BOS_PREPARED)
                    .addObjectIds(deleted)
                    .build();
                int cnt = s3ObjectMapper.deleteByCriteria(deleteCriteria);
                LOGGER.info("Deleted {} expired prepared S3 Object: {}", cnt, toRollbackObjectIds);
                session.commit();
            } catch (Throwable e) {
                LOGGER.error("Failed to deleted expired prepared S3 Object: {}", toRollbackObjectIds, e);
            }
        }
    }
}
