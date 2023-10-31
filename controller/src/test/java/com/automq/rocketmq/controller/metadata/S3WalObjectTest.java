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

import apache.rocketmq.controller.v1.SubStream;
import apache.rocketmq.controller.v1.SubStreams;
import com.automq.rocketmq.controller.metadata.database.dao.S3WalObject;
import com.automq.rocketmq.controller.metadata.database.mapper.S3WalObjectMapper;
import com.google.protobuf.util.JsonFormat;
import org.apache.ibatis.session.SqlSession;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import java.io.IOException;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class S3WalObjectTest extends DatabaseTestBase {

    @Test
    public void testS3WalObjectCRUD() throws IOException {
        try (SqlSession session = this.getSessionFactory().openSession()) {
            S3WalObjectMapper s3WalObjectMapper = session.getMapper(S3WalObjectMapper.class);
            S3WalObject s3WALObject = new S3WalObject();
            s3WALObject.setObjectId(123L);
            s3WALObject.setObjectSize(22L);
            s3WALObject.setNodeId(1);
            s3WALObject.setSequenceId(999L);
            s3WALObject.setSubStreams("{}");

            Calendar calendar = Calendar.getInstance();
            calendar.add(Calendar.SECOND, 30);
            Date time = calendar.getTime();

            s3WALObject.setBaseDataTimestamp(time);

            int affectedRows = s3WalObjectMapper.create(s3WALObject);
            Assertions.assertEquals(1, affectedRows);

            S3WalObject s3WalObject1 = s3WalObjectMapper.getByObjectId(s3WALObject.getObjectId());
            Assertions.assertEquals(s3WALObject.getObjectId(), s3WalObject1.getObjectId());
            Assertions.assertEquals(s3WALObject.getObjectSize(), s3WalObject1.getObjectSize());
            Assertions.assertEquals(s3WALObject.getNodeId(), s3WalObject1.getNodeId());
            Assertions.assertEquals(s3WALObject.getSequenceId(), s3WalObject1.getSequenceId());
            Assertions.assertEquals(s3WALObject.getSubStreams(), s3WalObject1.getSubStreams());
            Assertions.assertEquals(s3WALObject.getBaseDataTimestamp(), s3WalObject1.getBaseDataTimestamp());

            List<S3WalObject> s3WalObjects = s3WalObjectMapper.list(s3WalObject1.getNodeId(), s3WalObject1.getSequenceId());
            Assertions.assertNotNull(s3WalObjects);
            Assertions.assertFalse(s3WalObjects.isEmpty());
            Assertions.assertEquals(1, s3WalObjects.size());

            Assertions.assertEquals(123, s3WalObjects.get(0).getObjectId());
            Assertions.assertEquals(22, s3WalObjects.get(0).getObjectSize());
            Assertions.assertEquals(1, s3WalObjects.get(0).getNodeId());
            Assertions.assertEquals(999, s3WalObjects.get(0).getSequenceId());
            Assertions.assertEquals("{}", s3WalObjects.get(0).getSubStreams());
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
            s3WALObject.setObjectId(123L);
            s3WALObject.setObjectSize(22L);
            s3WALObject.setNodeId(1);
            s3WALObject.setSequenceId(999L);
            s3WALObject.setSubStreams("{}");

            Calendar calendar = Calendar.getInstance();
            calendar.add(Calendar.SECOND, 30);
            Date time = calendar.getTime();

            s3WALObject.setBaseDataTimestamp(time);

            int affectedRows = s3WalObjectMapper.create(s3WALObject);
            Assertions.assertEquals(1, affectedRows);

            S3WalObject s3WalObject1 = s3WalObjectMapper.getByObjectId(s3WALObject.getObjectId());
            s3WalObject1.setCommittedTimestamp(new Date(time.getTime() + 10 * 1000));

            affectedRows = s3WalObjectMapper.commit(s3WalObject1);
            S3WalObject s3WalObject2 = s3WalObjectMapper.getByObjectId(s3WalObject1.getObjectId());
            Assertions.assertEquals(1, affectedRows);
            Assertions.assertEquals(s3WalObject1.getObjectId(), s3WalObject2.getObjectId());
            Assertions.assertEquals(s3WalObject1.getObjectSize(), s3WalObject2.getObjectSize());
            Assertions.assertEquals(s3WalObject1.getNodeId(), s3WalObject2.getNodeId());
            Assertions.assertEquals(s3WalObject1.getSequenceId(), s3WalObject2.getSequenceId());
            Assertions.assertEquals(s3WalObject1.getSubStreams(), s3WalObject2.getSubStreams());
            Assertions.assertEquals(s3WalObject1.getBaseDataTimestamp(), s3WalObject2.getBaseDataTimestamp());
            Assertions.assertEquals(s3WalObject1.getCommittedTimestamp(), s3WalObject2.getCommittedTimestamp());

            s3WalObjectMapper.delete(null, s3WalObject1.getNodeId(), null);
            List<S3WalObject> s3WalObjects = s3WalObjectMapper.list(null, s3WalObject1.getSequenceId());
            Assertions.assertTrue(s3WalObjects.isEmpty());
        }
    }

    @Test
    public void testStreamExclusive() throws IOException {
        try (SqlSession session = getSessionFactory().openSession()) {
            S3WalObjectMapper mapper = session.getMapper(S3WalObjectMapper.class);

            S3WalObject walObject = new S3WalObject();
            walObject.setNodeId(1);
            walObject.setObjectSize(128L);
            walObject.setObjectId(2L);
            walObject.setSequenceId(3L);
            walObject.setBaseDataTimestamp(new Date());
            walObject.setCommittedTimestamp(new Date());
            walObject.setCreatedTimestamp(new Date());
            SubStreams subStreams = SubStreams.newBuilder()
                .putSubStreams(1, SubStream.newBuilder().setStreamId(1).setStartOffset(0).setEndOffset(10).build())
                .putSubStreams(2, SubStream.newBuilder().setStreamId(2).setStartOffset(0).setEndOffset(10).build())
                .putSubStreams(3, SubStream.newBuilder().setStreamId(3).setStartOffset(0).setEndOffset(10).build())
                .build();
            walObject.setSubStreams(JsonFormat.printer().print(subStreams));
            int rowsAffected = mapper.create(walObject);
            Assertions.assertEquals(1, rowsAffected);

            Assertions.assertTrue(mapper.streamExclusive(1, 1));
            Assertions.assertTrue(mapper.streamExclusive(1, 2));
            Assertions.assertTrue(mapper.streamExclusive(1, 3));

            Assertions.assertFalse(mapper.streamExclusive(2, 1));
            Assertions.assertFalse(mapper.streamExclusive(2, 2));
            Assertions.assertFalse(mapper.streamExclusive(2, 3));
        }
    }
}
