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

import apache.rocketmq.controller.v1.GroupStatus;
import com.automq.rocketmq.controller.exception.ControllerException;
import com.automq.rocketmq.controller.metadata.MetadataStore;
import com.automq.rocketmq.controller.metadata.database.mapper.GroupMapper;
import java.util.Calendar;
import org.apache.ibatis.session.SqlSession;

public class RecycleGroupTask extends ControllerTask {

    public RecycleGroupTask(MetadataStore metadataStore) {
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
            GroupMapper topicMapper = session.getMapper(GroupMapper.class);
            Calendar calendar = Calendar.getInstance();
            calendar.add(Calendar.SECOND, -metadataStore.config().deletedGroupLingersInSecs());
            int rowsAffected = topicMapper.recycle(GroupStatus.GROUP_STATUS_DELETED, calendar.getTime());
            if (rowsAffected > 0) {
                LOGGER.info("Deleted {} consumer groups", rowsAffected);
                session.commit();
            }
        }
    }
}
