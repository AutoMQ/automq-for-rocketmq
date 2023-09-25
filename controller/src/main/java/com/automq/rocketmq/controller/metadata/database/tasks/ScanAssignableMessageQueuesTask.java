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

package com.automq.rocketmq.controller.metadata.database.tasks;

import com.automq.rocketmq.controller.metadata.database.DefaultMetadataStore;
import com.automq.rocketmq.controller.metadata.database.dao.QueueAssignment;
import com.automq.rocketmq.controller.metadata.database.dao.QueueAssignmentStatus;
import com.automq.rocketmq.controller.metadata.database.mapper.QueueAssignmentMapper;
import java.util.List;
import org.apache.ibatis.session.SqlSession;

/**
 * Scan and assign the message queues that are marked assignable to alive nodes.
 */
public class ScanAssignableMessageQueuesTask extends ScanTask {

    public ScanAssignableMessageQueuesTask(DefaultMetadataStore metadataStore) {
        super(metadataStore);
    }

    @Override
    public void run() {
        try {
            if (!metadataStore.isLeader()) {
                return;
            }

            try (SqlSession session = metadataStore.getSessionFactory().openSession()) {
                QueueAssignmentMapper assignmentMapper = session.getMapper(QueueAssignmentMapper.class);
                List<QueueAssignment> assignments = assignmentMapper.list(null, null, null,
                    QueueAssignmentStatus.ASSIGNABLE, this.lastScanTime);
                if (doAssign(assignments, session)) {
                    session.commit();
                }
            }
        } catch (Throwable e) {
            LOGGER.error("Unexpected exception raised while scan assignable message queues", e);
        }
    }

    private boolean doAssign(List<QueueAssignment> assignments, SqlSession session) {
        if (null == assignments || assignments.isEmpty()) {
            return true;
        }

        if (!metadataStore.assignMessageQueues(assignments, session)) {
            return false;
        }

        for (QueueAssignment assignment : assignments) {
            if (null == this.lastScanTime || assignment.getUpdateTime().after(this.lastScanTime)) {
                this.lastScanTime = assignment.getUpdateTime();
            }
        }
        return true;
    }
}
