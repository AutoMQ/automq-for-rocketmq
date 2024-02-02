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

package com.automq.rocketmq.metadata;

import com.automq.rocketmq.metadata.dao.GroupProgress;
import com.automq.rocketmq.metadata.mapper.GroupProgressMapper;
import java.io.IOException;
import java.util.List;
import org.apache.ibatis.session.SqlSession;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class GroupProgressTest extends DatabaseTestBase {
    @Test
    public void testCreateOrUpdate() throws IOException {
        try (SqlSession session = getSessionFactory().openSession()) {
            GroupProgressMapper mapper = session.getMapper(GroupProgressMapper.class);
            GroupProgress progress = new GroupProgress();
            progress.setGroupId(1);
            progress.setTopicId(2);
            progress.setQueueId(3);
            progress.setQueueOffset(4);
            int rowsAffected = mapper.createOrUpdate(progress);
            Assertions.assertEquals(1, rowsAffected);

            progress.setQueueOffset(5);
            mapper.createOrUpdate(progress);

            List<GroupProgress> progressList = mapper.list(1L, 2L);
            Assertions.assertEquals(1, progressList.size());
            GroupProgress got = progressList.get(0);
            Assertions.assertEquals(5, got.getQueueOffset());
        }
    }
}
