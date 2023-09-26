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

import com.automq.rocketmq.controller.metadata.database.dao.QueueAssignment;
import com.automq.rocketmq.controller.metadata.database.dao.AssignmentStatus;
import com.automq.rocketmq.controller.metadata.database.mapper.QueueAssignmentMapper;
import java.io.IOException;
import java.util.List;
import org.apache.ibatis.session.SqlSession;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class StreamAffiliationAssignmentTest extends DatabaseTestBase {

    @Test
    public void testQueueAssignmentCRUD() throws IOException {

        // By default, session auto-commit is false.
        try (SqlSession session = getSessionFactory().openSession()) {
            QueueAssignmentMapper mapper = session.getMapper(QueueAssignmentMapper.class);

            QueueAssignment assignment = new QueueAssignment();
            long topicId = 1;
            int queueId = 2;
            int srcNodeId = 1;
            int dstNodeId = 2;
            AssignmentStatus status = AssignmentStatus.ASSIGNABLE;
            assignment.setTopicId(topicId);
            assignment.setQueueId(queueId);
            assignment.setSrcNodeId(srcNodeId);
            assignment.setDstNodeId(dstNodeId);
            assignment.setStatus(status);
            int affectedRows = mapper.create(assignment);
            Assertions.assertEquals(1, affectedRows);

            List<QueueAssignment> assignments = mapper.list(null, null, null, null, null);
            Assertions.assertEquals(1, assignments.size());

            QueueAssignment got = assignments.get(0);
            Assertions.assertEquals(topicId, got.getTopicId());
            Assertions.assertEquals(queueId, got.getQueueId());
            Assertions.assertEquals(srcNodeId, got.getSrcNodeId());
            Assertions.assertEquals(dstNodeId, got.getDstNodeId());
            Assertions.assertEquals(status, got.getStatus());

            assignments = mapper.list(topicId, null, null, null, null);
            Assertions.assertEquals(1, assignments.size());

            got.setStatus(AssignmentStatus.ASSIGNED);
            affectedRows = mapper.update(got);
            Assertions.assertEquals(1, affectedRows);

            assignments = mapper.list(topicId, null, null, null, null);
            got = assignments.get(0);
            Assertions.assertEquals(got.getStatus(), AssignmentStatus.ASSIGNED);

        }
    }
}
