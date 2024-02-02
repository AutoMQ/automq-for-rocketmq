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
import com.automq.rocketmq.metadata.dao.Group;
import com.automq.rocketmq.metadata.dao.GroupCriteria;
import com.automq.rocketmq.metadata.mapper.GroupMapper;
import java.util.List;
import org.apache.ibatis.session.SqlSession;

public class ScanGroupTask extends ScanTask {
    public ScanGroupTask(MetadataStore metadataStore) {
        super(metadataStore);
    }

    @Override
    public void process() throws ControllerException {
        try (SqlSession session = metadataStore.openSession()) {
            GroupMapper mapper = session.getMapper(GroupMapper.class);
            List<Group> groups = mapper.byCriteria(GroupCriteria.newBuilder().setLastUpdateTime(lastScanTime).build());
            metadataStore.applyGroupChange(groups);

            // Update last scan time
            if (null != groups && !groups.isEmpty()) {
                for (Group group : groups) {
                    if (null == lastScanTime || group.getUpdateTime().after(lastScanTime)) {
                        lastScanTime = group.getUpdateTime();
                    }
                }
            }
        }
    }
}
