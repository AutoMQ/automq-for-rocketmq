/*
 * Copyright 2024, AutoMQ CO.,LTD.
 *
 * Use of this software is governed by the Business Source License
 * included in the file BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

package com.automq.rocketmq.controller.server.tasks;

import com.automq.rocketmq.common.exception.ControllerException;
import com.automq.rocketmq.controller.MetadataStore;
import com.automq.rocketmq.metadata.dao.QueueAssignment;
import com.automq.rocketmq.metadata.mapper.QueueAssignmentMapper;
import java.util.List;
import org.apache.ibatis.session.SqlSession;

public class ScanAssignmentTask extends ScanTask {

    public ScanAssignmentTask(MetadataStore metadataStore) {
        super(metadataStore);
    }

    @Override
    public void process() throws ControllerException {
        try (SqlSession session = metadataStore.openSession()) {
            QueueAssignmentMapper mapper = session.getMapper(QueueAssignmentMapper.class);
            List<QueueAssignment> assignments = mapper.list(null, null, null, null, lastScanTime);

            metadataStore.applyAssignmentChange(assignments);

            // Update last scan time
            if (null != assignments && !assignments.isEmpty()) {
                for (QueueAssignment topic: assignments) {
                    if (null == lastScanTime || topic.getUpdateTime().after(lastScanTime)) {
                        lastScanTime = topic.getUpdateTime();
                    }
                }
            }
        }

    }
}
