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

import com.automq.rocketmq.controller.metadata.database.dao.S3StreamObject;
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
    public void testS3StreamObjectCRUD() throws IOException {
        try (SqlSession session = this.getSessionFactory().openSession()) {
            S3StreamObjectMapper s3StreamObjectMapper = session.getMapper(S3StreamObjectMapper.class);

            S3StreamObject s3StreamObject = new S3StreamObject();
            s3StreamObject.setObjectId(11);
            s3StreamObject.setObjectSize(123);
            s3StreamObject.setStreamId(111);
            s3StreamObject.setStartOffset(1234);
            s3StreamObject.setEndOffset(2345);

            Calendar calendar = Calendar.getInstance();
            calendar.add(Calendar.SECOND, 30);
            long time = calendar.getTime().getTime();

            s3StreamObject.setBaseDataTimestamp(time);

            int affectedRows = s3StreamObjectMapper.create(s3StreamObject);
            Assertions.assertEquals(1, affectedRows);
            Assertions.assertTrue(s3StreamObject.getId() > 0);

            // test getById
            S3StreamObject s3StreamObject1 = s3StreamObjectMapper.getById(s3StreamObject.getId());
            Assertions.assertEquals(s3StreamObject, s3StreamObject1);

            // test getByStreamAndObject
            S3StreamObject s3StreamObject2 = s3StreamObjectMapper.getByStreamAndObject(s3StreamObject.getStreamId(), s3StreamObject.getObjectId());
            Assertions.assertEquals(123, s3StreamObject2.getObjectSize());
            Assertions.assertEquals(1234, s3StreamObject2.getStartOffset());
            Assertions.assertEquals(2345, s3StreamObject2.getEndOffset());

            // test listByObjectId
            List<S3StreamObject> s3StreamObjects = s3StreamObjectMapper.listByObjectId(s3StreamObject.getObjectId());
            Assertions.assertNotNull(s3StreamObjects);
            Assertions.assertFalse(s3StreamObjects.isEmpty());
            Assertions.assertEquals(123, s3StreamObjects.get(0).getObjectSize());
            Assertions.assertEquals(1234, s3StreamObjects.get(0).getStartOffset());
            Assertions.assertEquals(2345, s3StreamObjects.get(0).getEndOffset());


            // test listByStreamId
            List<S3StreamObject> s3StreamObjects1 = s3StreamObjectMapper.listByStreamId(s3StreamObject.getStreamId());
            Assertions.assertNotNull(s3StreamObjects);
            Assertions.assertFalse(s3StreamObjects.isEmpty());
            Assertions.assertEquals(123, s3StreamObjects1.get(0).getObjectSize());
            Assertions.assertEquals(1234, s3StreamObjects1.get(0).getStartOffset());
            Assertions.assertEquals(2345, s3StreamObjects1.get(0).getEndOffset());

            // test list
            s3StreamObjects = s3StreamObjectMapper.list(null, s3StreamObject.getStreamId(), 2000L);
            Assertions.assertEquals(1, s3StreamObjects.size());
            Assertions.assertEquals(s3StreamObject, s3StreamObjects.get(0));

            // test delete
            s3StreamObjectMapper.delete(s3StreamObject1.getId(), null, null);
            List<S3StreamObject> s3StreamObjects3 = s3StreamObjectMapper.list(null, s3StreamObject1.getStreamId(), null);
            Assertions.assertTrue(s3StreamObjects3.isEmpty());
        }
    }

    @Test
    @Order(1)
    public void testCommitS3StreamObject() throws IOException {
        try (SqlSession session = this.getSessionFactory().openSession()) {
            S3StreamObjectMapper s3StreamObjectMapper = session.getMapper(S3StreamObjectMapper.class);
            S3StreamObject s3StreamObject = new S3StreamObject();
            s3StreamObject.setObjectId(11);
            s3StreamObject.setObjectSize(123);
            s3StreamObject.setStreamId(111);
            s3StreamObject.setStartOffset(1234);
            s3StreamObject.setEndOffset(2345);


            Calendar calendar = Calendar.getInstance();
            calendar.add(Calendar.SECOND, 30);
            long time = calendar.getTime().getTime();

            s3StreamObject.setCommittedTimestamp(time);

            int affectedRows = s3StreamObjectMapper.create(s3StreamObject);
            Assertions.assertEquals(1, affectedRows);

            S3StreamObject s3StreamObject1 = s3StreamObjectMapper.getByStreamAndObject(s3StreamObject.getStreamId(), s3StreamObject.getObjectId());

            s3StreamObject1.setCommittedTimestamp(time + 10 * 1000);

            affectedRows = s3StreamObjectMapper.commit(s3StreamObject1);

            S3StreamObject s3StreamObject2 = s3StreamObjectMapper.getById(s3StreamObject.getId());
            Assertions.assertEquals(1, affectedRows);
            Assertions.assertEquals(s3StreamObject1, s3StreamObject2);

            s3StreamObjectMapper.delete(null, s3StreamObject2.getStreamId(), s3StreamObject2.getObjectId());
            List<S3StreamObject> s3StreamObjects = s3StreamObjectMapper.list(s3StreamObject.getObjectId(), null, null);
            Assertions.assertTrue(s3StreamObjects.isEmpty());
        }
    }
}
