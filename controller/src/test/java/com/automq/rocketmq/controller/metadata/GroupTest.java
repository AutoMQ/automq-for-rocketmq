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

package com.automq.rocketmq.controller.metadata;

import apache.rocketmq.controller.v1.GroupStatus;
import apache.rocketmq.controller.v1.SubscriptionMode;
import com.automq.rocketmq.controller.metadata.database.dao.Group;
import com.automq.rocketmq.controller.metadata.database.dao.GroupCriteria;
import com.automq.rocketmq.controller.metadata.database.mapper.GroupMapper;
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
