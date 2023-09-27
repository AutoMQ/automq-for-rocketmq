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
            S3Object s3Object = new S3Object();
            s3Object.setObjectId(987L);
            s3Object.setStreamId(1L);
            s3Object.setObjectSize(555L);
            s3Object.setState(S3ObjectState.BOS_PREPARED);

            Calendar calendar = Calendar.getInstance();
            calendar.add(Calendar.SECOND, 30);
            long time = calendar.getTime().getTime();
            s3Object.setPreparedTimestamp(time);
            s3Object.setExpiredTimestamp(time + 5 * 1000);

            int affectedRows = s3ObjectMapper.create(s3Object);
            Assertions.assertEquals(1, affectedRows);
            Assertions.assertTrue(s3Object.getId() > 0);
            s3ObjectMapper.prepare(s3Object);

            // test getById
            S3Object s3Object1 = s3ObjectMapper.getById(s3Object.getId());
            Assertions.assertEquals(s3Object, s3Object1);

            // test getByObjectId
            S3Object s3Object2 = s3ObjectMapper.getByObjectId(s3Object.getObjectId());
            Assertions.assertEquals(1, affectedRows);
            Assertions.assertEquals(987, s3Object2.getObjectId());
            Assertions.assertEquals(555, s3Object2.getObjectSize());
            Assertions.assertEquals(S3ObjectState.BOS_PREPARED, s3Object2.getState());
            Assertions.assertEquals(time, s3Object2.getPreparedTimestamp());
            Assertions.assertEquals(time + 5 * 1000, s3Object2.getExpiredTimestamp());

            Calendar calendar1 = Calendar.getInstance();
            calendar1.add(Calendar.SECOND, 30);
            long time1 = calendar1.getTime().getTime();
            s3Object2.setMarkedForDeletionTimestamp(time1 + 10 * 1000);
            s3ObjectMapper.delete(s3Object2);

            S3Object s3Object3 = s3ObjectMapper.getByObjectId(s3Object2.getObjectId());
            Assertions.assertEquals(s3Object2, s3Object3);
        }
    }


    @Test
    @Order(2)
    public void testExpired() throws IOException {
        try (SqlSession session = this.getSessionFactory().openSession()) {
            S3ObjectMapper s3ObjectMapper = session.getMapper(S3ObjectMapper.class);
            S3Object s3Object = new S3Object();
            s3Object.setObjectId(987L);
            s3Object.setStreamId(1L);
            s3Object.setObjectSize(555L);
            s3Object.setState(S3ObjectState.BOS_PREPARED);

            Calendar calendar = Calendar.getInstance();
            calendar.add(Calendar.SECOND, 30);
            long time = calendar.getTime().getTime();
            s3Object.setPreparedTimestamp(time);
            s3Object.setExpiredTimestamp(time + 5 * 1000);

            int affectedRows = s3ObjectMapper.create(s3Object);
            Assertions.assertEquals(1, affectedRows);
            Assertions.assertTrue(s3Object.getId() > 0);


            S3Object s3Object1 = s3ObjectMapper.getByObjectId(s3Object.getObjectId());
            Calendar calendar1 = Calendar.getInstance();
            calendar1.add(Calendar.SECOND, 30);
            long time1 = calendar1.getTime().getTime();
            s3Object1.setExpiredTimestamp(time1 + 10 * 1000);
            affectedRows = s3ObjectMapper.expired(s3Object1);
            Assertions.assertEquals(1, affectedRows);

            S3Object s3Object2 = s3ObjectMapper.getByObjectId(s3Object.getObjectId());
            Assertions.assertEquals(s3Object1, s3Object2);
        }
    }

    @Test
    @Order(4)
    public void testCommit() throws IOException {
        try (SqlSession session = this.getSessionFactory().openSession()) {
            S3ObjectMapper s3ObjectMapper = session.getMapper(S3ObjectMapper.class);
            S3Object s3Object = new S3Object();
            s3Object.setObjectId(987L);
            s3Object.setStreamId(1L);
            s3Object.setObjectSize(555L);
            s3Object.setState(S3ObjectState.BOS_PREPARED);

            Calendar calendar = Calendar.getInstance();
            calendar.add(Calendar.SECOND, 30);
            long time = calendar.getTime().getTime();
            s3Object.setPreparedTimestamp(time);
            s3Object.setExpiredTimestamp(time + 5 * 1000);

            int affectedRows = s3ObjectMapper.create(s3Object);
            Assertions.assertEquals(1, affectedRows);
            Assertions.assertTrue(s3Object.getId() > 0);


            S3Object s3Object1 = s3ObjectMapper.getByObjectId(s3Object.getObjectId());
            Calendar calendar1 = Calendar.getInstance();
            calendar1.add(Calendar.SECOND, 30);
            long time1 = calendar1.getTime().getTime();
            s3Object1.setCommittedTimestamp(time1 + 10 * 1000);

            affectedRows = s3ObjectMapper.commit(s3Object1);
            Assertions.assertEquals(1, affectedRows);

            S3Object s3Object2 = s3ObjectMapper.getByObjectId(s3Object1.getObjectId());
            Assertions.assertEquals(s3Object1, s3Object2);
        }
    }

}
