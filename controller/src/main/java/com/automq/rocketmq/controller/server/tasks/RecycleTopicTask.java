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
import com.automq.rocketmq.common.exception.ControllerException;
import com.automq.rocketmq.controller.MetadataStore;
import com.automq.rocketmq.metadata.mapper.TopicMapper;
import java.util.Calendar;
import org.apache.ibatis.session.SqlSession;

public class RecycleTopicTask extends ControllerTask {

    public RecycleTopicTask(MetadataStore metadataStore) {
        super(metadataStore);
    }

    @Override
    public void process() throws ControllerException {
        if (!metadataStore.isLeader()) {
            return;
        }
        try (SqlSession session = metadataStore.openSession()) {
            if (!metadataStore.maintainLeadershipWithSharedLock(session)) {
                return;
            }

            TopicMapper topicMapper = session.getMapper(TopicMapper.class);
            Calendar calendar = Calendar.getInstance();
            calendar.add(Calendar.SECOND, -metadataStore.config().deletedTopicLingersInSecs());
            int rowsAffected = topicMapper.recycle(TopicStatus.TOPIC_STATUS_DELETED, calendar.getTime());
            if (rowsAffected > 0) {
                LOGGER.info("Deleted {} topics", rowsAffected);
                session.commit();
            }
        }
    }
}
