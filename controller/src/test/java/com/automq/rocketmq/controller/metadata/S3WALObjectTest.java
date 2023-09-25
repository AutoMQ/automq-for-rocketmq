/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package com.automq.rocketmq.controller.metadata;

import com.automq.rocketmq.controller.metadata.database.dao.S3WALObject;
import com.automq.rocketmq.controller.metadata.database.mapper.S3WALObjectMapper;
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
public class S3WALObjectTest extends DatabaseTestBase {

    @Test
    public void testS3WALObjectCRUD() throws IOException {
        try (SqlSession session = this.getSessionFactory().openSession()) {
            S3WALObjectMapper s3WALObjectMapper = session.getMapper(S3WALObjectMapper.class);
            S3WALObject s3WALObject = new S3WALObject();
            s3WALObject.setObjectId(123);
            s3WALObject.setBrokerId(1);
            s3WALObject.setSequenceId(999);
            s3WALObject.setSubStreams("<json><json>");

            Calendar calendar = Calendar.getInstance();
            calendar.add(Calendar.SECOND, 30);
            long time = calendar.getTime().getTime();

            s3WALObject.setBaseDataTimestamp(time);

            int affectedRows = s3WALObjectMapper.create(s3WALObject);
            Assertions.assertEquals(1, affectedRows);

            S3WALObject s3WALObject1 = s3WALObjectMapper.getByObjectId(s3WALObject.getObjectId());
            Assertions.assertEquals(s3WALObject, s3WALObject1);

            List<S3WALObject> s3WALObjects = s3WALObjectMapper.list(s3WALObject1.getBrokerId(), s3WALObject1.getSequenceId());
            Assertions.assertNotNull(s3WALObjects);
            Assertions.assertFalse(s3WALObjects.isEmpty());
            Assertions.assertEquals(1, s3WALObjects.size());

            Assertions.assertEquals(123, s3WALObjects.get(0).getObjectId());
            Assertions.assertEquals(1, s3WALObjects.get(0).getBrokerId());
            Assertions.assertEquals(999, s3WALObjects.get(0).getSequenceId());
            Assertions.assertEquals("<json><json>", s3WALObjects.get(0).getSubStreams());
            Assertions.assertEquals(time, s3WALObjects.get(0).getBaseDataTimestamp());

            // test delete
            s3WALObjectMapper.delete(s3WALObject.getObjectId(), null, null);
            List<S3WALObject> s3StreamObjects3 = s3WALObjectMapper.list(null, s3WALObject.getSequenceId());
            Assertions.assertTrue(s3StreamObjects3.isEmpty());
        }
    }

    @Test
    @Order(1)
    public void testCommitS3WALObject() throws IOException {
        try (SqlSession session = this.getSessionFactory().openSession()) {
            S3WALObjectMapper s3WALObjectMapper = session.getMapper(S3WALObjectMapper.class);
            S3WALObject s3WALObject = new S3WALObject();
            s3WALObject.setObjectId(123);
            s3WALObject.setBrokerId(1);
            s3WALObject.setSequenceId(999);
            s3WALObject.setSubStreams("<json><json>");

            Calendar calendar = Calendar.getInstance();
            calendar.add(Calendar.SECOND, 30);
            long time = calendar.getTime().getTime();

            s3WALObject.setBaseDataTimestamp(time);

            int affectedRows = s3WALObjectMapper.create(s3WALObject);
            Assertions.assertEquals(1, affectedRows);

            S3WALObject s3WALObject1 = s3WALObjectMapper.getByObjectId(s3WALObject.getObjectId());
            s3WALObject1.setCommittedTimestamp(time + 10 * 1000);

            affectedRows = s3WALObjectMapper.commit(s3WALObject1);
            S3WALObject s3WALObject2 = s3WALObjectMapper.getByObjectId(s3WALObject1.getObjectId());
            Assertions.assertEquals(1, affectedRows);
            Assertions.assertEquals(s3WALObject1, s3WALObject2);

            s3WALObjectMapper.delete(null, s3WALObject1.getBrokerId(), null);
            List<S3WALObject> s3WALObjects = s3WALObjectMapper.list(null, s3WALObject1.getSequenceId());
            Assertions.assertTrue(s3WALObjects.isEmpty());
        }
    }
}
