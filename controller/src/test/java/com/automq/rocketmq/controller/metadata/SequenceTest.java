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

import com.automq.rocketmq.controller.metadata.database.mapper.S3ObjectMapper;
import com.automq.rocketmq.controller.metadata.database.mapper.SequenceMapper;
import java.io.IOException;
import java.util.UUID;
import org.apache.ibatis.session.SqlSession;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class SequenceTest extends DatabaseTestBase {

    @Test
    public void testCRUD() throws IOException {
        try (SqlSession session = getSessionFactory().openSession()) {
            SequenceMapper sequenceMapper = session.getMapper(SequenceMapper.class);
            long next = sequenceMapper.next(S3ObjectMapper.SEQUENCE_NAME);
            Assertions.assertTrue(next >= 1);

            String name = UUID.randomUUID().toString();
            long initial = 100;
            sequenceMapper.create(name, initial);
            next = sequenceMapper.next(name);
            Assertions.assertEquals(initial, next);

            sequenceMapper.update(name, 110);
            next = sequenceMapper.next(name);
            Assertions.assertEquals(110, next);
        }
    }


}
