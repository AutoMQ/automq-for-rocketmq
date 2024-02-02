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

import apache.rocketmq.controller.v1.GroupStatus;
import apache.rocketmq.controller.v1.SubscriptionMode;
import com.automq.rocketmq.metadata.dao.Group;
import com.automq.rocketmq.metadata.dao.GroupCriteria;
import com.automq.rocketmq.metadata.mapper.GroupMapper;
import java.io.IOException;
import java.util.List;
import org.apache.ibatis.session.SqlSession;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class GroupTest extends DatabaseTestBase {
    @Test
    public void testGroupCRUD() throws IOException {
        try (SqlSession session = getSessionFactory().openSession()) {
            GroupMapper mapper = session.getMapper(GroupMapper.class);
            Group group = new Group();
            group.setName("G1");
            group.setStatus(GroupStatus.GROUP_STATUS_ACTIVE);
            group.setDeadLetterTopicId(1L);
            group.setSubMode(SubscriptionMode.SUB_MODE_POP);
            int rowsAffected = mapper.create(group);
            Assertions.assertEquals(1, rowsAffected);

            List<Group> groups = mapper.byCriteria(GroupCriteria.newBuilder().setGroupId(group.getId()).build());
            Assertions.assertEquals(1, groups.size());
            Assertions.assertEquals(SubscriptionMode.SUB_MODE_POP, groups.get(0).getSubMode());

            group.setStatus(GroupStatus.GROUP_STATUS_DELETED);
            group.setDeadLetterTopicId(2L);
            group.setSubMode(SubscriptionMode.SUB_MODE_PULL);
            mapper.update(group);

            groups = mapper.byCriteria(GroupCriteria.newBuilder().setGroupId(group.getId()).build());
            Assertions.assertEquals(1, groups.size());
            Group got = groups.get(0);
            Assertions.assertEquals("G1", got.getName());
            Assertions.assertEquals(GroupStatus.GROUP_STATUS_DELETED, got.getStatus());
            Assertions.assertEquals(2, got.getDeadLetterTopicId());
            Assertions.assertEquals(SubscriptionMode.SUB_MODE_PULL, got.getSubMode());

            groups = mapper.byCriteria(GroupCriteria.newBuilder().setStatus(GroupStatus.GROUP_STATUS_ACTIVE).build());
            Assertions.assertTrue(groups.isEmpty());

            rowsAffected = mapper.delete(group.getId());
            Assertions.assertEquals(1, rowsAffected);
        }
    }
}
