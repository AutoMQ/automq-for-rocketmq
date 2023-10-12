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

import apache.rocketmq.controller.v1.S3ObjectState;
import com.automq.rocketmq.controller.metadata.database.dao.S3Object;
import com.automq.rocketmq.controller.metadata.database.mapper.S3ObjectMapper;
import com.automq.rocketmq.controller.metadata.database.mapper.SequenceMapper;
import org.apache.ibatis.session.SqlSession;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import java.io.IOException;
import java.util.Calendar;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class S3ObjectTest extends DatabaseTestBase {
    @Test
    @Order(1)
    public void testS3ObjectCRUD() throws IOException {
        try (SqlSession session = this.getSessionFactory().openSession()) {
            S3ObjectMapper s3ObjectMapper = session.getMapper(S3ObjectMapper.class);

            SequenceMapper sequenceMapper = session.getMapper(SequenceMapper.class);
            long next = sequenceMapper.next(S3ObjectMapper.SEQUENCE_NAME);

            S3Object s3Object = new S3Object();
            s3Object.setId(next++);
            Calendar calendar = Calendar.getInstance();
            calendar.add(Calendar.SECOND, 30);
            s3Object.setExpiredTimestamp(calendar.getTime());

            int affectedRows = s3ObjectMapper.prepare(s3Object);
            Assertions.assertEquals(1, affectedRows);
            Assertions.assertTrue(s3Object.getId() > 0);

            // test getById
            S3Object s3Object1 = s3ObjectMapper.getById(s3Object.getId());
            Assertions.assertEquals(s3Object, s3Object1);

            // test delete
            Calendar calendar1 = Calendar.getInstance();
            calendar1.add(Calendar.SECOND, 30);
            s3Object1.setMarkedForDeletionTimestamp(calendar1.getTime());
            s3ObjectMapper.markToDelete(s3Object1.getId());

            S3Object s3Object2 = s3ObjectMapper.getById(s3Object.getId());
            Assertions.assertEquals(S3ObjectState.BOS_WILL_DELETE, s3Object2.getState());
        }
    }


    @Test
    @Order(2)
    public void testExpired() throws IOException {
        try (SqlSession session = this.getSessionFactory().openSession()) {

            SequenceMapper sequenceMapper = session.getMapper(SequenceMapper.class);
            long next = sequenceMapper.next(S3ObjectMapper.SEQUENCE_NAME);

            S3ObjectMapper s3ObjectMapper = session.getMapper(S3ObjectMapper.class);
            S3Object s3Object = new S3Object();
            s3Object.setId(next++);

            s3Object.setStreamId(1L);
            s3Object.setObjectSize(555L);
            s3Object.setState(S3ObjectState.BOS_PREPARED);

            Calendar calendar = Calendar.getInstance();
            calendar.add(Calendar.SECOND, 30);
            s3Object.setExpiredTimestamp(calendar.getTime());

            int affectedRows = s3ObjectMapper.prepare(s3Object);
            Assertions.assertEquals(1, affectedRows);
            Assertions.assertTrue(s3Object.getId() > 0);


            S3Object s3Object1 = s3ObjectMapper.getById(s3Object.getId());
            Calendar calendar1 = Calendar.getInstance();
            calendar1.add(Calendar.SECOND, 30);
            s3Object1.setExpiredTimestamp(calendar1.getTime());

            S3Object s3Object2 = s3ObjectMapper.getById(s3Object.getId());
            Assertions.assertEquals(s3Object1, s3Object2);
        }
    }

    @Test
    @Order(4)
    public void testCommit() throws IOException {
        try (SqlSession session = this.getSessionFactory().openSession()) {
            SequenceMapper sequenceMapper = session.getMapper(SequenceMapper.class);
            long next = sequenceMapper.next(S3ObjectMapper.SEQUENCE_NAME);

            S3ObjectMapper s3ObjectMapper = session.getMapper(S3ObjectMapper.class);
            S3Object s3Object = new S3Object();
            s3Object.setId(next++);
            s3Object.setStreamId(1L);
            s3Object.setObjectSize(555L);
            s3Object.setState(S3ObjectState.BOS_PREPARED);

            Calendar calendar = Calendar.getInstance();
            calendar.add(Calendar.SECOND, 30);
            s3Object.setExpiredTimestamp(calendar.getTime());

            int affectedRows = s3ObjectMapper.prepare(s3Object);
            Assertions.assertEquals(1, affectedRows);
            Assertions.assertTrue(s3Object.getId() > 0);


            S3Object s3Object1 = s3ObjectMapper.getById(s3Object.getId());
            Calendar calendar1 = Calendar.getInstance();
            calendar1.add(Calendar.SECOND, 30);
            s3Object1.setCommittedTimestamp(calendar1.getTime());
            s3Object1.setStreamId(1L);
            s3Object1.setObjectSize(100L);

            affectedRows = s3ObjectMapper.commit(s3Object1);
            Assertions.assertEquals(1, affectedRows);

            S3Object s3Object2 = s3ObjectMapper.getById(s3Object1.getId());
            Assertions.assertEquals(S3ObjectState.BOS_COMMITTED, s3Object2.getState());
            Assertions.assertEquals(1L, s3Object2.getStreamId());
            Assertions.assertEquals(100L, s3Object2.getObjectSize());
        }
    }

}
