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

import apache.rocketmq.controller.v1.AssignmentStatus;
import com.automq.rocketmq.controller.metadata.MetadataStore;
import com.automq.rocketmq.controller.metadata.database.dao.QueueAssignment;
import com.automq.rocketmq.controller.metadata.database.mapper.QueueAssignmentMapper;
import java.util.List;
import org.apache.ibatis.session.SqlSession;

public class ScanYieldingQueueTask extends ScanTask {

    public ScanYieldingQueueTask(MetadataStore metadataStore) {
        super(metadataStore);
    }

    @Override
    public void run() {
        LOGGER.info("Start to scan yielding queues");
        try (SqlSession session = metadataStore.openSession()) {
            QueueAssignmentMapper assignmentMapper = session.getMapper(QueueAssignmentMapper.class);
            List<QueueAssignment> assignments = assignmentMapper.list(null, metadataStore.config().nodeId(), null, AssignmentStatus.ASSIGNMENT_STATUS_YIELDING, this.lastScanTime);

        } catch (Throwable e) {
            LOGGER.error("Unexpected error raised", e);
        }

        LOGGER.info("Scan-yielding-queue completed");
    }
}
