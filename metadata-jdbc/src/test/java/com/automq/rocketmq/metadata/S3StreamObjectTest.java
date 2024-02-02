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

import com.automq.rocketmq.metadata.dao.S3StreamObject;
import com.automq.rocketmq.metadata.mapper.S3StreamObjectMapper;
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
public class S3StreamObjectTest extends DatabaseTestBase {

    @Test
    public void testS3StreamObjectCRUD() throws IOException {
        try (SqlSession session = this.getSessionFactory().openSession()) {
            S3StreamObjectMapper s3StreamObjectMapper = session.getMapper(S3StreamObjectMapper.class);

            S3StreamObject s3StreamObject = new S3StreamObject();
            s3StreamObject.setObjectId(11L);
            s3StreamObject.setObjectSize(123L);
            s3StreamObject.setStreamId(111L);
            s3StreamObject.setStartOffset(1234L);
            s3StreamObject.setEndOffset(2345L);

            Calendar calendar = Calendar.getInstance();
            calendar.add(Calendar.SECOND, 30);
            Date time = calendar.getTime();

            s3StreamObject.setBaseDataTimestamp(time);

            int affectedRows = s3StreamObjectMapper.create(s3StreamObject);
            Assertions.assertEquals(1, affectedRows);
            Assertions.assertTrue(s3StreamObject.getId() > 0);

            // test getById
            S3StreamObject s3StreamObject1 = s3StreamObjectMapper.getById(s3StreamObject.getId());
            Assertions.assertEquals(s3StreamObject.getId(), s3StreamObject1.getId());
            Assertions.assertEquals(s3StreamObject.getObjectId(), s3StreamObject1.getObjectId());
            Assertions.assertEquals(s3StreamObject.getObjectSize(), s3StreamObject1.getObjectSize());
            Assertions.assertEquals(s3StreamObject.getStreamId(), s3StreamObject1.getStreamId());
            Assertions.assertEquals(s3StreamObject.getStartOffset(), s3StreamObject1.getStartOffset());
            Assertions.assertEquals(s3StreamObject.getEndOffset(), s3StreamObject1.getEndOffset());
            Assertions.assertEquals(s3StreamObject.getCommittedTimestamp(), s3StreamObject1.getCommittedTimestamp());

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
            s3StreamObjects = s3StreamObjectMapper.list(null, s3StreamObject.getStreamId(), 2000L, 2111L, 1);
            Assertions.assertEquals(1, s3StreamObjects.size());
            Assertions.assertEquals(s3StreamObject.getId(), s3StreamObjects.get(0).getId());
            Assertions.assertEquals(s3StreamObject.getObjectId(), s3StreamObjects.get(0).getObjectId());
            Assertions.assertEquals(s3StreamObject.getObjectSize(), s3StreamObjects.get(0).getObjectSize());
            Assertions.assertEquals(s3StreamObject.getStreamId(), s3StreamObjects.get(0).getStreamId());
            Assertions.assertEquals(s3StreamObject.getStartOffset(), s3StreamObjects.get(0).getStartOffset());
            Assertions.assertEquals(s3StreamObject.getEndOffset(), s3StreamObjects.get(0).getEndOffset());
            Assertions.assertEquals(s3StreamObject.getCommittedTimestamp(), s3StreamObjects.get(0).getCommittedTimestamp());


            // test delete
            s3StreamObjectMapper.delete(s3StreamObject1.getId(), null, null);
            List<S3StreamObject> s3StreamObjects3 = s3StreamObjectMapper.list(null, s3StreamObject1.getStreamId(), null, null, null);
            Assertions.assertTrue(s3StreamObjects3.isEmpty());
        }
    }

    @Test
    @Order(1)
    public void testCommitS3StreamObject() throws IOException {
        try (SqlSession session = this.getSessionFactory().openSession()) {
            S3StreamObjectMapper s3StreamObjectMapper = session.getMapper(S3StreamObjectMapper.class);
            S3StreamObject s3StreamObject = new S3StreamObject();
            s3StreamObject.setObjectId(11L);
            s3StreamObject.setObjectSize(123L);
            s3StreamObject.setStreamId(111L);
            s3StreamObject.setStartOffset(1234L);
            s3StreamObject.setEndOffset(2345L);

            Calendar calendar = Calendar.getInstance();
            calendar.add(Calendar.SECOND, 30);
            Date time = calendar.getTime();

            s3StreamObject.setCommittedTimestamp(time);
            int affectedRows = s3StreamObjectMapper.commit(s3StreamObject);
            Assertions.assertEquals(1, affectedRows);

            S3StreamObject s3StreamObject2 = s3StreamObjectMapper.getById(s3StreamObject.getId());
            Assertions.assertEquals(s3StreamObject.getId(), s3StreamObject2.getId());
            Assertions.assertEquals(s3StreamObject.getObjectId(), s3StreamObject2.getObjectId());
            Assertions.assertEquals(s3StreamObject.getObjectSize(), s3StreamObject2.getObjectSize());
            Assertions.assertEquals(s3StreamObject.getStreamId(), s3StreamObject2.getStreamId());
            Assertions.assertEquals(s3StreamObject.getStartOffset(), s3StreamObject2.getStartOffset());
            Assertions.assertEquals(s3StreamObject.getEndOffset(), s3StreamObject2.getEndOffset());
            Assertions.assertEquals(s3StreamObject.getCommittedTimestamp(), s3StreamObject2.getCommittedTimestamp());

            s3StreamObjectMapper.delete(null, s3StreamObject2.getStreamId(), s3StreamObject2.getObjectId());
            List<S3StreamObject> s3StreamObjects = s3StreamObjectMapper.list(s3StreamObject.getObjectId(), null, null, null, null);
            Assertions.assertTrue(s3StreamObjects.isEmpty());
        }
    }

    @Test
    public void testBatchDelete() throws IOException {
        try (SqlSession session = this.getSessionFactory().openSession()) {
            S3StreamObjectMapper s3StreamObjectMapper = session.getMapper(S3StreamObjectMapper.class);
            S3StreamObject s3StreamObject = new S3StreamObject();
            s3StreamObject.setObjectId(11L);
            s3StreamObject.setObjectSize(123L);
            s3StreamObject.setStreamId(111L);
            s3StreamObject.setStartOffset(1234L);
            s3StreamObject.setEndOffset(2345L);

            Calendar calendar = Calendar.getInstance();
            calendar.add(Calendar.SECOND, 30);
            Date time = calendar.getTime();

            s3StreamObject.setCommittedTimestamp(time);
            s3StreamObjectMapper.commit(s3StreamObject);
            s3StreamObjectMapper.batchDelete(List.of(11L));
            Assertions.assertTrue(s3StreamObjectMapper.list(null, null, null, null, null).isEmpty());
        }
    }

    @Test
    public void testListRecyclable() throws IOException {
        try (SqlSession session = this.getSessionFactory().openSession()) {
            S3StreamObjectMapper s3StreamObjectMapper = session.getMapper(S3StreamObjectMapper.class);
            S3StreamObject s3StreamObject = new S3StreamObject();
            s3StreamObject.setObjectId(11L);
            s3StreamObject.setObjectSize(123L);
            s3StreamObject.setStreamId(111L);
            s3StreamObject.setStartOffset(1234L);
            s3StreamObject.setEndOffset(2345L);

            Calendar calendar = Calendar.getInstance();
            calendar.add(Calendar.SECOND, 30);
            Date time = calendar.getTime();

            s3StreamObject.setCommittedTimestamp(time);
            s3StreamObjectMapper.commit(s3StreamObject);
            session.commit();

            Calendar threshold = Calendar.getInstance();
            threshold.add(Calendar.HOUR, 1);
            Assertions.assertTrue(threshold.getTimeInMillis() > time.getTime());

            List<S3StreamObject> objects = s3StreamObjectMapper.list(null, null, null, null, null);
            Assertions.assertTrue(objects.get(0).getCommittedTimestamp().getTime() < threshold.getTimeInMillis());
            Assertions.assertEquals(111, objects.get(0).getStreamId());

            List<S3StreamObject> ids = s3StreamObjectMapper.recyclable(List.of(111L), threshold.getTime());
            Assertions.assertEquals(1, ids.size());

            threshold = Calendar.getInstance();
            threshold.add(Calendar.HOUR, -80);
            ids = s3StreamObjectMapper.recyclable(List.of(111L), threshold.getTime());
            Assertions.assertTrue(ids.isEmpty());
        }
    }
}
