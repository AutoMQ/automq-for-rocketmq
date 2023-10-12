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

package com.automq.rocketmq.controller.tasks;

import apache.rocketmq.controller.v1.Code;
import apache.rocketmq.controller.v1.S3ObjectState;
import com.automq.rocketmq.controller.exception.ControllerException;
import com.automq.rocketmq.controller.metadata.MetadataStore;
import com.automq.rocketmq.controller.metadata.database.dao.S3Object;
import com.automq.rocketmq.controller.metadata.database.mapper.S3ObjectMapper;
import java.util.List;
import java.util.concurrent.ExecutionException;
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

            S3ObjectMapper mapper = session.getMapper(S3ObjectMapper.class);
            int rows = mapper.rollback();
            if (rows > 0) {
                LOGGER.info("Rollback {} expired prepared S3 Object rows", rows);
            }
            List<S3Object> s3Objects = mapper.list(S3ObjectState.BOS_WILL_DELETE, null);
            List<Long> ids = s3Objects.stream().mapToLong(S3Object::getId).boxed().toList();
            if (!ids.isEmpty()) {
                List<Long> result = metadataStore.getDataStore().batchDeleteS3Objects(ids).get();
                if (null != result && !result.isEmpty()) {
                    mapper.batchDelete(result);
                }
            }
            session.commit();
        } catch (ExecutionException | InterruptedException e) {
            LOGGER.error("Failed to batch delete S3 Objects", e);
            throw new ControllerException(Code.INTERNAL_VALUE, e);
        }

    }
}
