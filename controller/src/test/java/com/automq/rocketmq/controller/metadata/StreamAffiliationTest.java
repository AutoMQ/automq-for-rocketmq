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

import com.automq.rocketmq.controller.metadata.database.dao.AssignmentStatus;
import com.automq.rocketmq.controller.metadata.database.dao.StreamAffiliation;
import com.automq.rocketmq.controller.metadata.database.dao.StreamRole;
import com.automq.rocketmq.controller.metadata.database.mapper.StreamAffiliationMapper;
import java.io.IOException;
import java.util.List;
import org.apache.ibatis.session.SqlSession;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class StreamAffiliationTest extends DatabaseTestBase {
    @Test
    public void testQueueCRUD() throws IOException {
        try (SqlSession session = getSessionFactory().openSession()) {
            StreamAffiliationMapper mapper = session.getMapper(StreamAffiliationMapper.class);
            StreamAffiliation streamAffiliation = new StreamAffiliation();
            streamAffiliation.setTopicId(1);
            streamAffiliation.setQueueId(2);
            streamAffiliation.setStreamId(3);
            streamAffiliation.setStreamRole(StreamRole.RETRY);
            streamAffiliation.setGroupId(4);
            streamAffiliation.setSrcNodeId(5);
            streamAffiliation.setDstNodeId(6);
            streamAffiliation.setStatus(AssignmentStatus.ASSIGNED);
            int rowsAffected = mapper.create(streamAffiliation);
            Assertions.assertEquals(1, rowsAffected);
            List<StreamAffiliation> streamAffiliations = mapper.list(null, null, null, null);
            Assertions.assertEquals(1, streamAffiliations.size());
            StreamAffiliation got = streamAffiliations.get(0);
            Assertions.assertEquals(1, got.getTopicId());
            Assertions.assertEquals(2, got.getQueueId());
            Assertions.assertEquals(3, got.getStreamId());
            Assertions.assertEquals(StreamRole.RETRY, got.getStreamRole());
            Assertions.assertEquals(4, got.getGroupId());
            Assertions.assertEquals(5, got.getSrcNodeId());
            Assertions.assertEquals(6, got.getDstNodeId());
            Assertions.assertEquals(AssignmentStatus.ASSIGNED, got.getStatus());

            rowsAffected = mapper.delete(null, null);
            Assertions.assertEquals(1, rowsAffected);
        }
    }

    @Test
    public void testUpdate() throws IOException {
        try (SqlSession session = getSessionFactory().openSession()) {
            StreamAffiliationMapper mapper = session.getMapper(StreamAffiliationMapper.class);
            StreamAffiliation streamAffiliation = new StreamAffiliation();
            streamAffiliation.setTopicId(1);
            streamAffiliation.setQueueId(2);
            streamAffiliation.setStreamId(3);
            streamAffiliation.setStreamRole(StreamRole.RETRY);
            streamAffiliation.setGroupId(4);
            streamAffiliation.setSrcNodeId(5);
            streamAffiliation.setDstNodeId(6);
            streamAffiliation.setStatus(AssignmentStatus.ASSIGNED);
            mapper.create(streamAffiliation);

            mapper.update(1L, null, null, AssignmentStatus.DELETED);

            List<StreamAffiliation> streams = mapper.list(1L, null, null, null);
            for (StreamAffiliation stream : streams) {
                Assertions.assertEquals(AssignmentStatus.DELETED, stream.getStatus());
            }
        }
    }
}
