/*
 * Copyright 2024, AutoMQ CO.,LTD.
 *
 * Use of this software is governed by the Business Source License
 * included in the file BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

package com.automq.rocketmq.metadata;

import apache.rocketmq.controller.v1.SubStream;
import apache.rocketmq.controller.v1.SubStreams;
import com.automq.rocketmq.metadata.dao.S3StreamSetObject;
import com.automq.rocketmq.metadata.mapper.S3StreamSetObjectMapper;
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
public class S3StreamSetObjectTest extends DatabaseTestBase {

    @Test
    public void testS3WalObjectCRUD() throws IOException {
        try (SqlSession session = this.getSessionFactory().openSession()) {
            S3StreamSetObjectMapper s3StreamSetObjectMapper = session.getMapper(S3StreamSetObjectMapper.class);
            S3StreamSetObject s3StreamSetObject = new S3StreamSetObject();
            s3StreamSetObject.setObjectId(123L);
            s3StreamSetObject.setObjectSize(22L);
            s3StreamSetObject.setNodeId(1);
            s3StreamSetObject.setSequenceId(999L);
            s3StreamSetObject.setSubStreams("{}");

            Calendar calendar = Calendar.getInstance();
            calendar.add(Calendar.SECOND, 30);
            Date time = calendar.getTime();

            s3StreamSetObject.setBaseDataTimestamp(time);

            int affectedRows = s3StreamSetObjectMapper.create(s3StreamSetObject);
            Assertions.assertEquals(1, affectedRows);

            S3StreamSetObject s3StreamSetObject1 = s3StreamSetObjectMapper.getByObjectId(s3StreamSetObject.getObjectId());
            Assertions.assertEquals(s3StreamSetObject.getObjectId(), s3StreamSetObject1.getObjectId());
            Assertions.assertEquals(s3StreamSetObject.getObjectSize(), s3StreamSetObject1.getObjectSize());
            Assertions.assertEquals(s3StreamSetObject.getNodeId(), s3StreamSetObject1.getNodeId());
            Assertions.assertEquals(s3StreamSetObject.getSequenceId(), s3StreamSetObject1.getSequenceId());
            Assertions.assertEquals(s3StreamSetObject.getSubStreams(), s3StreamSetObject1.getSubStreams());
            Assertions.assertEquals(s3StreamSetObject.getBaseDataTimestamp(), s3StreamSetObject1.getBaseDataTimestamp());

            List<S3StreamSetObject> s3StreamSetObjects = s3StreamSetObjectMapper.list(s3StreamSetObject1.getNodeId(), s3StreamSetObject1.getSequenceId());
            Assertions.assertNotNull(s3StreamSetObjects);
            Assertions.assertFalse(s3StreamSetObjects.isEmpty());
            Assertions.assertEquals(1, s3StreamSetObjects.size());

            Assertions.assertEquals(123, s3StreamSetObjects.get(0).getObjectId());
            Assertions.assertEquals(22, s3StreamSetObjects.get(0).getObjectSize());
            Assertions.assertEquals(1, s3StreamSetObjects.get(0).getNodeId());
            Assertions.assertEquals(999, s3StreamSetObjects.get(0).getSequenceId());
            Assertions.assertEquals("{}", s3StreamSetObjects.get(0).getSubStreams());
            Assertions.assertEquals(time, s3StreamSetObjects.get(0).getBaseDataTimestamp());

            // test delete
            s3StreamSetObjectMapper.delete(s3StreamSetObject.getObjectId(), null, null);
            List<S3StreamSetObject> s3StreamObjects3 = s3StreamSetObjectMapper.list(null, s3StreamSetObject.getSequenceId());
            Assertions.assertTrue(s3StreamObjects3.isEmpty());
        }
    }

    @Test
    @Order(1)
    public void testCommitS3WalObject() throws IOException {
        try (SqlSession session = this.getSessionFactory().openSession()) {
            S3StreamSetObjectMapper s3StreamSetObjectMapper = session.getMapper(S3StreamSetObjectMapper.class);
            S3StreamSetObject s3StreamSetObject = new S3StreamSetObject();
            s3StreamSetObject.setObjectId(123L);
            s3StreamSetObject.setObjectSize(22L);
            s3StreamSetObject.setNodeId(1);
            s3StreamSetObject.setSequenceId(999L);
            s3StreamSetObject.setSubStreams("{}");

            Calendar calendar = Calendar.getInstance();
            calendar.add(Calendar.SECOND, 30);
            Date time = calendar.getTime();

            s3StreamSetObject.setBaseDataTimestamp(time);

            int affectedRows = s3StreamSetObjectMapper.create(s3StreamSetObject);
            Assertions.assertEquals(1, affectedRows);

            S3StreamSetObject s3StreamSetObject1 = s3StreamSetObjectMapper.getByObjectId(s3StreamSetObject.getObjectId());
            s3StreamSetObject1.setCommittedTimestamp(new Date(time.getTime() + 10 * 1000));

            affectedRows = s3StreamSetObjectMapper.commit(s3StreamSetObject1);
            S3StreamSetObject s3StreamSetObject2 = s3StreamSetObjectMapper.getByObjectId(s3StreamSetObject1.getObjectId());
            Assertions.assertEquals(1, affectedRows);
            Assertions.assertEquals(s3StreamSetObject1.getObjectId(), s3StreamSetObject2.getObjectId());
            Assertions.assertEquals(s3StreamSetObject1.getObjectSize(), s3StreamSetObject2.getObjectSize());
            Assertions.assertEquals(s3StreamSetObject1.getNodeId(), s3StreamSetObject2.getNodeId());
            Assertions.assertEquals(s3StreamSetObject1.getSequenceId(), s3StreamSetObject2.getSequenceId());
            Assertions.assertEquals(s3StreamSetObject1.getSubStreams(), s3StreamSetObject2.getSubStreams());
            Assertions.assertEquals(s3StreamSetObject1.getBaseDataTimestamp(), s3StreamSetObject2.getBaseDataTimestamp());
            Assertions.assertEquals(s3StreamSetObject1.getCommittedTimestamp(), s3StreamSetObject2.getCommittedTimestamp());

            s3StreamSetObjectMapper.delete(null, s3StreamSetObject1.getNodeId(), null);
            List<S3StreamSetObject> s3StreamSetObjects = s3StreamSetObjectMapper.list(null, s3StreamSetObject1.getSequenceId());
            Assertions.assertTrue(s3StreamSetObjects.isEmpty());
        }
    }

    @Test
    public void testStreamExclusive() throws IOException {
        try (SqlSession session = getSessionFactory().openSession()) {
            S3StreamSetObjectMapper mapper = session.getMapper(S3StreamSetObjectMapper.class);

            S3StreamSetObject walObject = new S3StreamSetObject();
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
