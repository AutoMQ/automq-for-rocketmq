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

import com.automq.rocketmq.controller.metadata.database.dao.Queue;
import com.automq.rocketmq.controller.metadata.database.dao.StreamRole;
import com.automq.rocketmq.controller.metadata.database.mapper.QueueMapper;
import java.io.IOException;
import java.util.List;
import org.apache.ibatis.session.SqlSession;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class QueueTest extends DatabaseTestBase {
    @Test
    public void testQueueCRUD() throws IOException {
        try (SqlSession session = getSessionFactory().openSession()) {
            QueueMapper mapper = session.getMapper(QueueMapper.class);
            Queue queue = new Queue();
            queue.setTopicId(1);
            queue.setQueueId(2);
            queue.setStreamId(3);
            queue.setStreamRole(StreamRole.OPS);
            int rowsAffected = mapper.create(queue);
            Assertions.assertEquals(1, rowsAffected);
            List<Queue> queues = mapper.list(null, null);
            Assertions.assertEquals(1, queues.size());
            Queue got = queues.get(0);
            Assertions.assertEquals(1, got.getTopicId());
            Assertions.assertEquals(2, got.getQueueId());
            Assertions.assertEquals(3, got.getStreamId());
            Assertions.assertEquals(StreamRole.OPS, got.getStreamRole());

            rowsAffected = mapper.delete(null, null);
            Assertions.assertEquals(1, rowsAffected);
        }
    }
}
