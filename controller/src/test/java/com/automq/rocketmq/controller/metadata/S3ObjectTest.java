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

import apache.rocketmq.controller.v1.S3Object;
import apache.rocketmq.controller.v1.S3ObjectState;
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
    public void testCreateS3Object() throws IOException {
        try (SqlSession session = this.getSessionFactory().openSession()) {
            S3ObjectMapper s3ObjectMapper = session.getMapper(S3ObjectMapper.class);
            S3Object s3Object = S3Object.newBuilder().
                    setObjectId(987).setObjectSize(555).
                    setState(S3ObjectState.BOS_COMMITTED).build();

            int affectedRows = s3ObjectMapper.create(s3Object);
            Assertions.assertEquals(1, affectedRows);

            S3Object s3Object1 = s3ObjectMapper.getByObjectId(s3Object.getObjectId());
            Assertions.assertEquals(987, s3Object1.getObjectId());
            Assertions.assertEquals(555, s3Object1.getObjectSize());
            Assertions.assertEquals(S3ObjectState.BOS_COMMITTED, s3Object1.getState());

            s3ObjectMapper.delete(s3Object1.getObjectId());
        }
    }

    @Test
    @Order(2)
    public void testPrepare() throws IOException {
        try (SqlSession session = this.getSessionFactory().openSession()) {
            S3ObjectMapper s3ObjectMapper = session.getMapper(S3ObjectMapper.class);
            S3Object s3Object = S3Object.newBuilder().
                    setObjectId(987).setObjectSize(555).
                    setState(S3ObjectState.BOS_COMMITTED).build();
            int affectedRows = s3ObjectMapper.create(s3Object);
            Assertions.assertEquals(1, affectedRows);

            S3Object s3Object1 = s3ObjectMapper.getByObjectId(s3Object.getObjectId());
            Calendar calendar = Calendar.getInstance();
            calendar.add(Calendar.SECOND, 30);
            long time = calendar.getTime().getTime();
            S3Object s3Object2 = s3Object1.toBuilder().setPreparedTimestamp(time).build();
            affectedRows = s3ObjectMapper.prepare(s3Object2);
            Assertions.assertEquals(1, affectedRows);

            s3Object1 = s3ObjectMapper.getByObjectId(s3Object.getObjectId());
            Assertions.assertEquals(time, s3Object1.getPreparedTimestamp());
            s3ObjectMapper.delete(s3Object1.getObjectId());
        }
    }

    @Test
    @Order(3)
    public void testExpired() throws IOException {
        try (SqlSession session = this.getSessionFactory().openSession()) {
            S3ObjectMapper s3ObjectMapper = session.getMapper(S3ObjectMapper.class);
            S3Object s3Object = S3Object.newBuilder().
                    setObjectId(987).setObjectSize(555).
                    setState(S3ObjectState.BOS_COMMITTED).build();
            int affectedRows = s3ObjectMapper.create(s3Object);
            Assertions.assertEquals(1, affectedRows);

            S3Object s3Object1 = s3ObjectMapper.getByObjectId(s3Object.getObjectId());
            Calendar calendar = Calendar.getInstance();
            calendar.add(Calendar.SECOND, 30);
            long time = calendar.getTime().getTime();
            S3Object s3Object2 = s3Object1.toBuilder().setExpiredTimestamp(time).build();
            affectedRows = s3ObjectMapper.expired(s3Object2);
            Assertions.assertEquals(1, affectedRows);

            s3Object1 = s3ObjectMapper.getByObjectId(s3Object.getObjectId());
            Assertions.assertEquals(time, s3Object1.getExpiredTimestamp());
            s3ObjectMapper.delete(s3Object1.getObjectId());
        }
    }

    @Test
    @Order(4)
    public void testCommit() throws IOException {
        try (SqlSession session = this.getSessionFactory().openSession()) {
            S3ObjectMapper s3ObjectMapper = session.getMapper(S3ObjectMapper.class);
            S3Object s3Object = S3Object.newBuilder().
                    setObjectId(987).setObjectSize(555).
                    setState(S3ObjectState.BOS_COMMITTED).build();
            int affectedRows = s3ObjectMapper.create(s3Object);
            Assertions.assertEquals(1, affectedRows);

            S3Object s3Object1 = s3ObjectMapper.getByObjectId(s3Object.getObjectId());
            Calendar calendar = Calendar.getInstance();
            calendar.add(Calendar.SECOND, 30);
            long time = calendar.getTime().getTime();
            S3Object s3Object2 = s3Object1.toBuilder().setCommittedTimestamp(time).build();
            affectedRows = s3ObjectMapper.commit(s3Object2);
            Assertions.assertEquals(1, affectedRows);

            s3Object1 = s3ObjectMapper.getByObjectId(s3Object.getObjectId());
            Assertions.assertEquals(time, s3Object1.getCommittedTimestamp());
            s3ObjectMapper.delete(s3Object1.getObjectId());
        }
    }

}
