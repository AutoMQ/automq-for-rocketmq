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

import apache.rocketmq.controller.v1.GroupStatus;
import com.automq.rocketmq.common.exception.ControllerException;
import com.automq.rocketmq.controller.MetadataStore;
import com.automq.rocketmq.metadata.mapper.GroupMapper;
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
