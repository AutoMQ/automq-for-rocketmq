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

import apache.rocketmq.controller.v1.S3StreamObject;
import com.automq.rocketmq.controller.metadata.database.mapper.S3StreamObjectMapper;
import org.apache.ibatis.session.SqlSession;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import java.io.IOException;
import java.util.Calendar;
import java.util.List;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class S3StreamObjectTest extends DatabaseTestBase {

    @Test
    @Order(1)
    public void testCreateS3StreamObject() throws IOException {
        try (SqlSession session = this.getSessionFactory().openSession()) {
            S3StreamObjectMapper s3StreamObjectMapper = session.getMapper(S3StreamObjectMapper.class);
            S3StreamObject s3StreamObject = S3StreamObject.newBuilder().
                setObjectId(11).setObjectSize(123).
                setStreamId(111).setStartOffset(1234).
                setEndOffset(2345).build();

            int affectedRows = s3StreamObjectMapper.create(s3StreamObject);
            Assertions.assertEquals(1, affectedRows);

            S3StreamObject s3StreamObject1 = s3StreamObjectMapper.getByStreamAndObject(s3StreamObject.getStreamId(), s3StreamObject.getObjectId());
            Assertions.assertEquals(123, s3StreamObject1.getObjectSize());
            Assertions.assertEquals(1234, s3StreamObject1.getStartOffset());
            Assertions.assertEquals(2345, s3StreamObject1.getEndOffset());

            s3StreamObjectMapper.delete(s3StreamObject1.getStreamId(), s3StreamObject1.getObjectId());
            List<S3StreamObject> s3StreamObjects = s3StreamObjectMapper.list();
            Assertions.assertTrue(s3StreamObjects.isEmpty());
        }
    }

    @Test
    @Order(2)
    public void testListS3StreamObjectsByStreamId() throws IOException {
        try (SqlSession session = this.getSessionFactory().openSession()) {
            S3StreamObjectMapper s3StreamObjectMapper = session.getMapper(S3StreamObjectMapper.class);
            S3StreamObject s3StreamObject = S3StreamObject.newBuilder().
                setObjectId(11).setObjectSize(123).
                setStreamId(111).setStartOffset(1234).
                setEndOffset(2345).build();

            int affectedRows = s3StreamObjectMapper.create(s3StreamObject);
            Assertions.assertEquals(1, affectedRows);

            List<S3StreamObject> s3StreamObjects = s3StreamObjectMapper.listByStreamId(s3StreamObject.getStreamId());
            Assertions.assertNotNull(s3StreamObjects);
            Assertions.assertFalse(s3StreamObjects.isEmpty());
            Assertions.assertEquals(123, s3StreamObjects.get(0).getObjectSize());
            Assertions.assertEquals(1234, s3StreamObjects.get(0).getStartOffset());
            Assertions.assertEquals(2345, s3StreamObjects.get(0).getEndOffset());

            s3StreamObjectMapper.delete(s3StreamObjects.get(0).getStreamId(), s3StreamObjects.get(0).getObjectId());
            s3StreamObjects = s3StreamObjectMapper.list();
            Assertions.assertTrue(s3StreamObjects.isEmpty());
        }
    }

    @Test
    @Order(3)
    public void testListS3StreamObjectsByObjectId() throws IOException {
        try (SqlSession session = this.getSessionFactory().openSession()) {
            S3StreamObjectMapper s3StreamObjectMapper = session.getMapper(S3StreamObjectMapper.class);
            S3StreamObject s3StreamObject = S3StreamObject.newBuilder().
                setObjectId(11).setObjectSize(123).
                setStreamId(111).setStartOffset(1234).
                setEndOffset(2345).build();

            int affectedRows = s3StreamObjectMapper.create(s3StreamObject);
            Assertions.assertEquals(1, affectedRows);

            List<S3StreamObject> s3StreamObjects = s3StreamObjectMapper.listByObjectId(s3StreamObject.getObjectId());
            Assertions.assertNotNull(s3StreamObjects);
            Assertions.assertFalse(s3StreamObjects.isEmpty());
            Assertions.assertEquals(123, s3StreamObjects.get(0).getObjectSize());
            Assertions.assertEquals(1234, s3StreamObjects.get(0).getStartOffset());
            Assertions.assertEquals(2345, s3StreamObjects.get(0).getEndOffset());

            s3StreamObjectMapper.delete(s3StreamObjects.get(0).getStreamId(), s3StreamObjects.get(0).getObjectId());
            s3StreamObjects = s3StreamObjectMapper.list();
            Assertions.assertTrue(s3StreamObjects.isEmpty());
        }
    }

    @Test
    @Order(4)
    public void testCommitS3StreamObject() throws IOException {
        try (SqlSession session = this.getSessionFactory().openSession()) {
            S3StreamObjectMapper s3StreamObjectMapper = session.getMapper(S3StreamObjectMapper.class);
            S3StreamObject s3StreamObject = S3StreamObject.newBuilder().
                setObjectId(11).setObjectSize(123).
                setStreamId(111).setStartOffset(1234).
                setEndOffset(2345).build();

            int affectedRows = s3StreamObjectMapper.create(s3StreamObject);
            Assertions.assertEquals(1, affectedRows);

            S3StreamObject s3StreamObject1 = s3StreamObjectMapper.getByStreamAndObject(s3StreamObject.getStreamId(), s3StreamObject.getObjectId());

            Calendar calendar = Calendar.getInstance();
            calendar.add(Calendar.SECOND, 30);
            long time = calendar.getTime().getTime();
            S3StreamObject s3StreamObject2 = s3StreamObject1.toBuilder().setCommittedTimestamp(time).build();

            affectedRows = s3StreamObjectMapper.commit(s3StreamObject2);
            Assertions.assertEquals(1, affectedRows);

            s3StreamObjectMapper.delete(s3StreamObject2.getStreamId(), s3StreamObject2.getObjectId());
            List<S3StreamObject> s3StreamObjects = s3StreamObjectMapper.list();
            Assertions.assertTrue(s3StreamObjects.isEmpty());
        }
    }
}
