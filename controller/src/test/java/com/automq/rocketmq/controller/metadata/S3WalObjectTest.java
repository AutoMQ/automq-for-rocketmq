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

import com.automq.rocketmq.controller.metadata.database.dao.S3WalObject;
import com.automq.rocketmq.controller.metadata.database.mapper.S3WalObjectMapper;
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
public class S3WalObjectTest extends DatabaseTestBase {

    @Test
    public void testS3WalObjectCRUD() throws IOException {
        try (SqlSession session = this.getSessionFactory().openSession()) {
            S3WalObjectMapper s3WalObjectMapper = session.getMapper(S3WalObjectMapper.class);
            S3WalObject s3WALObject = new S3WalObject();
            s3WALObject.setObjectId(123);
            s3WALObject.setObjectSize(22);
            s3WALObject.setBrokerId(1);
            s3WALObject.setSequenceId(999);
            s3WALObject.setSubStreams("<json><json>");

            Calendar calendar = Calendar.getInstance();
            calendar.add(Calendar.SECOND, 30);
            long time = calendar.getTime().getTime();

            s3WALObject.setBaseDataTimestamp(time);

            int affectedRows = s3WalObjectMapper.create(s3WALObject);
            Assertions.assertEquals(1, affectedRows);

            S3WalObject s3WalObject1 = s3WalObjectMapper.getByObjectId(s3WALObject.getObjectId());
            Assertions.assertEquals(s3WALObject, s3WalObject1);

            List<S3WalObject> s3WalObjects = s3WalObjectMapper.list(s3WalObject1.getBrokerId(), s3WalObject1.getSequenceId());
            Assertions.assertNotNull(s3WalObjects);
            Assertions.assertFalse(s3WalObjects.isEmpty());
            Assertions.assertEquals(1, s3WalObjects.size());

            Assertions.assertEquals(123, s3WalObjects.get(0).getObjectId());
            Assertions.assertEquals(22, s3WalObjects.get(0).getObjectSize());
            Assertions.assertEquals(1, s3WalObjects.get(0).getBrokerId());
            Assertions.assertEquals(999, s3WalObjects.get(0).getSequenceId());
            Assertions.assertEquals("<json><json>", s3WalObjects.get(0).getSubStreams());
            Assertions.assertEquals(time, s3WalObjects.get(0).getBaseDataTimestamp());

            // test delete
            s3WalObjectMapper.delete(s3WALObject.getObjectId(), null, null);
            List<S3WalObject> s3StreamObjects3 = s3WalObjectMapper.list(null, s3WALObject.getSequenceId());
            Assertions.assertTrue(s3StreamObjects3.isEmpty());
        }
    }

    @Test
    @Order(1)
    public void testCommitS3WalObject() throws IOException {
        try (SqlSession session = this.getSessionFactory().openSession()) {
            S3WalObjectMapper s3WalObjectMapper = session.getMapper(S3WalObjectMapper.class);
            S3WalObject s3WALObject = new S3WalObject();
            s3WALObject.setObjectId(123);
            s3WALObject.setObjectSize(22);
            s3WALObject.setBrokerId(1);
            s3WALObject.setSequenceId(999);
            s3WALObject.setSubStreams("<json><json>");

            Calendar calendar = Calendar.getInstance();
            calendar.add(Calendar.SECOND, 30);
            long time = calendar.getTime().getTime();

            s3WALObject.setBaseDataTimestamp(time);

            int affectedRows = s3WalObjectMapper.create(s3WALObject);
            Assertions.assertEquals(1, affectedRows);

            S3WalObject s3WalObject1 = s3WalObjectMapper.getByObjectId(s3WALObject.getObjectId());
            s3WalObject1.setCommittedTimestamp(time + 10 * 1000);

            affectedRows = s3WalObjectMapper.commit(s3WalObject1);
            S3WalObject s3WalObject2 = s3WalObjectMapper.getByObjectId(s3WalObject1.getObjectId());
            Assertions.assertEquals(1, affectedRows);
            Assertions.assertEquals(s3WalObject1, s3WalObject2);

            s3WalObjectMapper.delete(null, s3WalObject1.getBrokerId(), null);
            List<S3WalObject> s3WalObjects = s3WalObjectMapper.list(null, s3WalObject1.getSequenceId());
            Assertions.assertTrue(s3WalObjects.isEmpty());
        }
    }
}
