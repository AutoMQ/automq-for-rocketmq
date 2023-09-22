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

import com.automq.rocketmq.controller.metadata.database.dao.GroupProgress;
import com.automq.rocketmq.controller.metadata.database.mapper.GroupProgressMapper;
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
