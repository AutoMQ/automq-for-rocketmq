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

package com.automq.rocketmq.metadata.service;

import apache.rocketmq.controller.v1.S3ObjectState;
import apache.rocketmq.controller.v1.S3StreamObject;
import apache.rocketmq.controller.v1.S3StreamSetObject;
import apache.rocketmq.controller.v1.StreamRole;
import apache.rocketmq.controller.v1.StreamState;
import apache.rocketmq.controller.v1.SubStream;
import com.automq.rocketmq.common.config.ControllerConfig;
import com.automq.rocketmq.common.system.StreamConstants;
import com.automq.rocketmq.metadata.DatabaseTestBase;
import com.automq.rocketmq.metadata.dao.Range;
import com.automq.rocketmq.metadata.dao.S3Object;
import com.automq.rocketmq.metadata.dao.Stream;
import com.automq.rocketmq.metadata.mapper.RangeMapper;
import com.automq.rocketmq.metadata.mapper.S3ObjectMapper;
import com.automq.rocketmq.metadata.mapper.S3StreamObjectMapper;
import com.automq.rocketmq.metadata.mapper.S3StreamSetObjectMapper;
import com.automq.rocketmq.metadata.mapper.StreamMapper;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.LongStream;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.ibatis.session.SqlSession;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class DefaultS3MetadataServiceTest extends DatabaseTestBase {

    ControllerConfig config;

    ExecutorService executorService;

    public DefaultS3MetadataServiceTest() {
        this.config = Mockito.mock(ControllerConfig.class);
        Mockito.when(config.nodeId()).thenReturn(1);
        this.executorService = Executors.newSingleThreadExecutor();
    }

    @Test
    public void testListStreamObjects() throws IOException, ExecutionException, InterruptedException {
        long startOffset = 0L, interval = 1000L, endOffset;
        int limit = 1;
        long streamId;
        try (SqlSession session = getSessionFactory().openSession()) {
            S3StreamObjectMapper s3StreamObjectMapper = session.getMapper(S3StreamObjectMapper.class);
            List<com.automq.rocketmq.metadata.dao.S3StreamObject> s3StreamObjects = buildS3StreamObjs(1, 1, startOffset, interval);
            s3StreamObjects.forEach(s3StreamObjectMapper::create);
            streamId = s3StreamObjects.get(0).getStreamId();
            endOffset = s3StreamObjects.get(0).getEndOffset();
            session.commit();
        }

        try (S3MetadataService service = new DefaultS3MetadataService(config, getSessionFactory(), executorService)) {
            List<S3StreamObject> s3StreamObjects = service.listStreamObjects(streamId, startOffset, endOffset, limit).get();
            S3StreamObject s3StreamObject = s3StreamObjects.get(0);
            Assertions.assertEquals(1, s3StreamObject.getObjectId());
            Assertions.assertEquals(100, s3StreamObject.getObjectSize());
            Assertions.assertEquals(1, s3StreamObject.getStreamId());
            Assertions.assertEquals(startOffset, s3StreamObject.getStartOffset());
            Assertions.assertEquals(endOffset, s3StreamObject.getEndOffset());
        }
    }

    @Test
    public void testListWALObjects_WithPrams() throws IOException, ExecutionException, InterruptedException {
        long streamId, startOffset, endOffset;
        streamId = 1;
        startOffset = 0L;
        endOffset = 9L;
        int limit = 1;

        try (SqlSession session = getSessionFactory().openSession()) {
            RangeMapper rangeMapper = session.getMapper(RangeMapper.class);
            Range range = new Range();
            range.setStreamId(streamId);
            range.setRangeId(0);
            range.setStartOffset(startOffset);
            range.setEndOffset(endOffset);
            range.setEpoch(1L);
            range.setNodeId(1);
            rangeMapper.create(range);

            S3StreamSetObjectMapper s3StreamSetObjectMapper = session.getMapper(S3StreamSetObjectMapper.class);
            buildS3WalObjs(1, 1).stream().peek(s3WalObject -> {
                Map<Long, SubStream> subStreams = buildWalSubStreams(4, 0, 10);
                s3WalObject.setSubStreams(toJson(subStreams));
            }).forEach(s3StreamSetObjectMapper::create);

            session.commit();
        }

        Map<Long, SubStream> expectedSubStream = buildWalSubStreams(1, 0, 10);

        try (S3MetadataService service = new DefaultS3MetadataService(config, getSessionFactory(), executorService)) {
            List<S3StreamSetObject> s3StreamSetObjects = service.listStreamSetObjects(streamId, startOffset, endOffset, limit).get();

            Assertions.assertFalse(s3StreamSetObjects.isEmpty());
            S3StreamSetObject s3StreamSetObject = s3StreamSetObjects.get(0);
            Assertions.assertEquals(1, s3StreamSetObject.getObjectId());
            Assertions.assertEquals(100, s3StreamSetObject.getObjectSize());
            Assertions.assertEquals(1, s3StreamSetObject.getBrokerId());
            Assertions.assertEquals(1, s3StreamSetObject.getSequenceId());
            Assertions.assertEquals(expectedSubStream, s3StreamSetObject.getSubStreams().getSubStreamsMap());
        }

        try (SqlSession session = getSessionFactory().openSession()) {
            S3StreamSetObjectMapper s3StreamSetObjectMapper = session.getMapper(S3StreamSetObjectMapper.class);
            s3StreamSetObjectMapper.delete(123L, 1, null);
            session.commit();
        }
    }

    @Test
    public void testListWALObjects_NotParams() throws IOException, ExecutionException, InterruptedException {
        try (SqlSession session = getSessionFactory().openSession()) {
            S3StreamSetObjectMapper s3StreamSetObjectMapper = session.getMapper(S3StreamSetObjectMapper.class);

            buildS3WalObjs(1, 1).stream().peek(s3WalObject1 -> {
                Map<Long, SubStream> subStreams = buildWalSubStreams(4, 0, 10);
                s3WalObject1.setSubStreams(toJson(subStreams));
            }).forEach(s3StreamSetObjectMapper::create);

            session.commit();
        }

        Map<Long, SubStream> subStreams = buildWalSubStreams(4, 0, 10);

        try (S3MetadataService service = new DefaultS3MetadataService(config, getSessionFactory(), executorService)) {
            List<S3StreamSetObject> s3StreamSetObjects = service.listStreamSetObjects().get();

            Assertions.assertFalse(s3StreamSetObjects.isEmpty());
            S3StreamSetObject s3StreamSetObject = s3StreamSetObjects.get(0);
            Assertions.assertEquals(1, s3StreamSetObject.getObjectId());
            Assertions.assertEquals(100, s3StreamSetObject.getObjectSize());
            Assertions.assertEquals(1, s3StreamSetObject.getBrokerId());
            Assertions.assertEquals(1, s3StreamSetObject.getSequenceId());
            Assertions.assertEquals(subStreams, s3StreamSetObject.getSubStreams().getSubStreamsMap());
        }

        try (SqlSession session = getSessionFactory().openSession()) {
            S3StreamSetObjectMapper s3StreamSetObjectMapper = session.getMapper(S3StreamSetObjectMapper.class);
            s3StreamSetObjectMapper.delete(123L, 1, null);
            session.commit();
        }
    }

    @Test
    public void testListObjects_OnlyStream() throws IOException, ExecutionException, InterruptedException {
        long startOffset, endOffset;
        startOffset = 0L;
        endOffset = 9L;
        int limit = 3;

        try (SqlSession session = getSessionFactory().openSession()) {
            S3StreamSetObjectMapper s3StreamSetObjectMapper = session.getMapper(S3StreamSetObjectMapper.class);

            buildS3WalObjs(1, 1).stream().peek(s3WalObject1 -> {
                Map<Long, SubStream> subStreams = buildWalSubStreams(4, 10, 10);
                s3WalObject1.setSubStreams(toJson(subStreams));
            }).forEach(s3StreamSetObjectMapper::create);

            S3StreamObjectMapper s3StreamObjectMapper = session.getMapper(S3StreamObjectMapper.class);
            buildS3StreamObjs(5, 1, 0, 10).forEach(s3StreamObjectMapper::create);
            session.commit();
        }

        try (DefaultS3MetadataService service = new DefaultS3MetadataService(config, getSessionFactory(), executorService)) {
            Pair<List<S3StreamObject>, List<S3StreamSetObject>> listPair = service.listObjects(1, startOffset, endOffset, limit).get();

            Assertions.assertFalse(listPair.getLeft().isEmpty());
            Assertions.assertTrue(listPair.getRight().isEmpty());
            S3StreamObject s3StreamObject = listPair.getLeft().get(0);
            Assertions.assertEquals(5, s3StreamObject.getObjectId());
            Assertions.assertEquals(100, s3StreamObject.getObjectSize());
            Assertions.assertEquals(1, s3StreamObject.getStreamId());
            Assertions.assertEquals(0, s3StreamObject.getStartOffset());
            Assertions.assertEquals(10, s3StreamObject.getEndOffset());
        }

        try (SqlSession session = getSessionFactory().openSession()) {
            S3StreamObjectMapper s3StreamObjectMapper = session.getMapper(S3StreamObjectMapper.class);
            s3StreamObjectMapper.delete(null, 1L, 122L);
            session.commit();
        }
    }

    @Test
    public void testListObjects_OnlyWAL() throws IOException, ExecutionException, InterruptedException {
        long streamId, startOffset, endOffset;
        streamId = 1;
        startOffset = 11L;
        endOffset = 19L;
        int limit = 3;

        try (SqlSession session = getSessionFactory().openSession()) {
            S3StreamSetObjectMapper s3StreamSetObjectMapper = session.getMapper(S3StreamSetObjectMapper.class);
            buildS3WalObjs(1, 1).stream().peek(s3WalObject1 -> {
                Map<Long, SubStream> subStreams = buildWalSubStreams(4, 10, 10);
                s3WalObject1.setSubStreams(toJson(subStreams));
            }).forEach(s3StreamSetObjectMapper::create);

            S3StreamObjectMapper s3StreamObjectMapper = session.getMapper(S3StreamObjectMapper.class);
            buildS3StreamObjs(5, 1, 0, 10).forEach(s3StreamObjectMapper::create);
            session.commit();
        }

        Map<Long, SubStream> subStreams = buildWalSubStreams(1, 10, 10);

        try (S3MetadataService service = new DefaultS3MetadataService(config, getSessionFactory(), executorService)) {
            Pair<List<S3StreamObject>, List<S3StreamSetObject>> listPair = service.listObjects(streamId, startOffset, endOffset, limit).get();

            Assertions.assertTrue(listPair.getLeft().isEmpty());
            Assertions.assertFalse(listPair.getRight().isEmpty());

            S3StreamSetObject s3StreamSetObject = listPair.getRight().get(0);
            Assertions.assertEquals(1, s3StreamSetObject.getObjectId());
            Assertions.assertEquals(100, s3StreamSetObject.getObjectSize());
            Assertions.assertEquals(1, s3StreamSetObject.getBrokerId());
            Assertions.assertEquals(1, s3StreamSetObject.getSequenceId());
            Assertions.assertEquals(subStreams, s3StreamSetObject.getSubStreams().getSubStreamsMap());
        }

        try (SqlSession session = getSessionFactory().openSession()) {
            S3StreamSetObjectMapper s3StreamSetObjectMapper = session.getMapper(S3StreamSetObjectMapper.class);
            s3StreamSetObjectMapper.delete(123L, 1, null);
            session.commit();
        }
    }

    @Test
    public void testListObjects_Both() throws IOException, ExecutionException, InterruptedException {
        long streamId, startOffset, endOffset;
        streamId = 1;
        startOffset = 0L;
        endOffset = 21L;
        int limit = 3;

        try (SqlSession session = getSessionFactory().openSession()) {
            S3StreamSetObjectMapper s3StreamSetObjectMapper = session.getMapper(S3StreamSetObjectMapper.class);
            buildS3WalObjs(1, 1).stream().peek(s3WalObject1 -> {
                Map<Long, SubStream> subStreams = buildWalSubStreams(4, 10, 10);
                s3WalObject1.setSubStreams(toJson(subStreams));
            }).forEach(s3StreamSetObjectMapper::create);

            S3StreamObjectMapper s3StreamObjectMapper = session.getMapper(S3StreamObjectMapper.class);
            buildS3StreamObjs(5, 1, 0, 10).forEach(s3StreamObjectMapper::create);

            session.commit();
        }

        Map<Long, SubStream> subStreams = buildWalSubStreams(1, 10, 10);

        try (S3MetadataService service = new DefaultS3MetadataService(config, getSessionFactory(), executorService)) {
            Pair<List<S3StreamObject>, List<S3StreamSetObject>> listPair = service.listObjects(streamId, startOffset, endOffset, limit).get();

            Assertions.assertFalse(listPair.getLeft().isEmpty());
            Assertions.assertFalse(listPair.getRight().isEmpty());
            S3StreamObject s3StreamObject = listPair.getLeft().get(0);
            Assertions.assertEquals(5, s3StreamObject.getObjectId());
            Assertions.assertEquals(100, s3StreamObject.getObjectSize());
            Assertions.assertEquals(streamId, s3StreamObject.getStreamId());
            Assertions.assertEquals(0, s3StreamObject.getStartOffset());
            Assertions.assertEquals(10, s3StreamObject.getEndOffset());

            S3StreamSetObject s3StreamSetObject = listPair.getRight().get(0);
            Assertions.assertEquals(1, s3StreamSetObject.getObjectId());
            Assertions.assertEquals(100, s3StreamSetObject.getObjectSize());
            Assertions.assertEquals(1, s3StreamSetObject.getBrokerId());
            Assertions.assertEquals(1, s3StreamSetObject.getSequenceId());
            Assertions.assertEquals(subStreams, s3StreamSetObject.getSubStreams().getSubStreamsMap());
        }

        try (SqlSession session = getSessionFactory().openSession()) {
            S3StreamSetObjectMapper s3StreamSetObjectMapper = session.getMapper(S3StreamSetObjectMapper.class);
            s3StreamSetObjectMapper.delete(123L, 1, null);

            S3StreamObjectMapper s3StreamObjectMapper = session.getMapper(S3StreamObjectMapper.class);
            s3StreamObjectMapper.delete(null, streamId, 122L);
            session.commit();
        }
    }

    @Test
    public void testListObjects_Both_Interleaved() throws IOException, ExecutionException, InterruptedException {
        long streamId, startOffset, endOffset;
        streamId = 1;
        startOffset = 0L;
        endOffset = 40L;
        int limit = 3;

        try (SqlSession session = getSessionFactory().openSession()) {
            S3StreamSetObjectMapper s3StreamSetObjectMapper = session.getMapper(S3StreamSetObjectMapper.class);
            buildS3WalObjs(1, 1).stream().peek(s3WalObject -> {
                Map<Long, SubStream> subStreams = buildWalSubStreams(1, 0, 10);
                s3WalObject.setSubStreams(toJson(subStreams));
            }).forEach(s3StreamSetObjectMapper::create);

            buildS3WalObjs(2, 1).stream().peek(s3WalObject -> {
                Map<Long, SubStream> subStreams = buildWalSubStreams(1, 20, 20);
                s3WalObject.setSubStreams(toJson(subStreams));
            }).forEach(s3StreamSetObjectMapper::create);

            S3StreamObjectMapper s3StreamObjectMapper = session.getMapper(S3StreamObjectMapper.class);
            buildS3StreamObjs(5, 1, 10, 10).forEach(s3StreamObjectMapper::create);
            buildS3StreamObjs(6, 1, 40, 10).forEach(s3StreamObjectMapper::create);

            session.commit();
        }

        Map<Long, SubStream> subStreams1 = buildWalSubStreams(1, 0, 10);
        Map<Long, SubStream> subStreams2 = buildWalSubStreams(1, 20, 20);

        try (S3MetadataService service = new DefaultS3MetadataService(config, getSessionFactory(), executorService)) {
            Pair<List<S3StreamObject>, List<S3StreamSetObject>> listPair = service.listObjects(streamId, startOffset, endOffset, limit).get();

            Assertions.assertFalse(listPair.getLeft().isEmpty());
            Assertions.assertFalse(listPair.getRight().isEmpty());
            List<S3StreamObject> s3StreamObjects = listPair.getLeft();
            Assertions.assertEquals(1, s3StreamObjects.size());
            S3StreamObject s3StreamObject = s3StreamObjects.get(0);

            Assertions.assertEquals(5, s3StreamObject.getObjectId());
            Assertions.assertEquals(100, s3StreamObject.getObjectSize());
            Assertions.assertEquals(streamId, s3StreamObject.getStreamId());
            Assertions.assertEquals(10, s3StreamObject.getStartOffset());
            Assertions.assertEquals(20, s3StreamObject.getEndOffset());

            List<S3StreamSetObject> s3StreamSetObjects = listPair.getRight();
            Assertions.assertEquals(2, s3StreamSetObjects.size());
            S3StreamSetObject s3StreamSetObject = s3StreamSetObjects.get(0);

            Assertions.assertEquals(1, s3StreamSetObject.getObjectId());
            Assertions.assertEquals(100, s3StreamSetObject.getObjectSize());
            Assertions.assertEquals(1, s3StreamSetObject.getBrokerId());
            Assertions.assertEquals(1, s3StreamSetObject.getSequenceId());
            Assertions.assertEquals(subStreams1, s3StreamSetObject.getSubStreams().getSubStreamsMap());

            S3StreamSetObject s3StreamSetObject1 = s3StreamSetObjects.get(1);
            Assertions.assertEquals(2, s3StreamSetObject1.getObjectId());
            Assertions.assertEquals(100, s3StreamSetObject1.getObjectSize());
            Assertions.assertEquals(1, s3StreamSetObject1.getBrokerId());
            Assertions.assertEquals(2, s3StreamSetObject1.getSequenceId());
            Assertions.assertEquals(subStreams2, s3StreamSetObject1.getSubStreams().getSubStreamsMap());
        }

        try (SqlSession session = getSessionFactory().openSession()) {
            S3StreamSetObjectMapper s3StreamSetObjectMapper = session.getMapper(S3StreamSetObjectMapper.class);
            s3StreamSetObjectMapper.delete(1L, 1, null);
            s3StreamSetObjectMapper.delete(2L, 2, null);

            S3StreamObjectMapper s3StreamObjectMapper = session.getMapper(S3StreamObjectMapper.class);
            s3StreamObjectMapper.delete(null, streamId, 122L);
            s3StreamObjectMapper.delete(null, streamId, 121L);
            session.commit();
        }
    }

    @Test
    public void testTrimStream() throws IOException {
        long streamId, streamEpoch = 1, newStartOffset = 2000;
        int nodeId = 1, rangId = 0;
        try (SqlSession session = this.getSessionFactory().openSession()) {
            StreamMapper streamMapper = session.getMapper(StreamMapper.class);
            RangeMapper rangeMapper = session.getMapper(RangeMapper.class);

            Stream stream = new Stream();
            stream.setSrcNodeId(nodeId);
            stream.setDstNodeId(nodeId);
            stream.setStartOffset(1234L);
            stream.setEpoch(0L);
            stream.setRangeId(rangId);
            stream.setTopicId(1L);
            stream.setQueueId(2);
            stream.setState(StreamState.OPEN);
            stream.setStreamRole(StreamRole.STREAM_ROLE_DATA);
            streamMapper.create(stream);
            streamId = stream.getId();

            Range range = new Range();
            range.setRangeId(rangId);
            range.setStreamId(streamId);
            range.setEpoch(0L);
            range.setStartOffset(1234L);
            range.setEndOffset(2345L);
            range.setNodeId(nodeId);
            rangeMapper.create(range);

            session.commit();
        }

        try (S3MetadataService service = new DefaultS3MetadataService(config, getSessionFactory(), executorService)) {
            service.trimStream(streamId, streamEpoch, newStartOffset);
        }

        try (SqlSession session = this.getSessionFactory().openSession()) {
            StreamMapper streamMapper = session.getMapper(StreamMapper.class);
            Stream stream = streamMapper.getByStreamId(streamId);
            Assertions.assertEquals(newStartOffset, stream.getStartOffset());
            Assertions.assertEquals(nodeId, stream.getSrcNodeId());
            Assertions.assertEquals(nodeId, stream.getDstNodeId());
            Assertions.assertEquals(0, stream.getRangeId());

            RangeMapper rangeMapper = session.getMapper(RangeMapper.class);
            Range range = rangeMapper.get(rangId, streamId, null);
            Assertions.assertEquals(newStartOffset, range.getStartOffset());
            Assertions.assertEquals(2345, range.getEndOffset());
            Assertions.assertEquals(nodeId, range.getNodeId());
            Assertions.assertEquals(streamId, range.getStreamId());
        }

    }
    @Test
    public void testPrepareS3Objects() throws IOException {
        long objectId;

        try (S3MetadataService service = new DefaultS3MetadataService(config, getSessionFactory(), executorService)) {
            objectId = service.prepareS3Objects(3, 5).get();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        try (SqlSession session = getSessionFactory().openSession()) {
            S3ObjectMapper s3ObjectMapper = session.getMapper(S3ObjectMapper.class);
            for (long index = objectId; index < objectId + 3; index++) {
                S3Object object = s3ObjectMapper.getById(index);
                Assertions.assertEquals(S3ObjectState.BOS_PREPARED, object.getState());
            }
        }
    }

    @Test
    public void testCommitStreamObject() throws IOException {
        long objectId, streamId = 1;

        try (S3MetadataService metadataService = new DefaultS3MetadataService(config, getSessionFactory(), executorService)) {
            objectId = metadataService.prepareS3Objects(3, 5).get();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        S3StreamObject news3StreamObject = S3StreamObject.newBuilder()
            .setObjectId(objectId + 2)
            .setStreamId(streamId)
            .setObjectSize(111L)
            .build();

        try (SqlSession session = this.getSessionFactory().openSession()) {
            S3StreamObjectMapper s3StreamObjectMapper = session.getMapper(S3StreamObjectMapper.class);
            buildS3StreamObjs(objectId, 2, 3, 100L).forEach(s3StreamObjectMapper::create);
            session.commit();
        }

        try (S3MetadataService service = new DefaultS3MetadataService(config, getSessionFactory(), executorService)) {
            List<Long> compactedObjects = new ArrayList<>();
            compactedObjects.add(objectId);
            compactedObjects.add(objectId + 1);
            service.commitStreamObject(news3StreamObject, compactedObjects);
        }

        try (SqlSession session = getSessionFactory().openSession()) {
            S3ObjectMapper s3ObjectMapper = session.getMapper(S3ObjectMapper.class);
            S3Object s3Object = s3ObjectMapper.getById(objectId);
            Assertions.assertEquals(S3ObjectState.BOS_WILL_DELETE, s3Object.getState());

            S3Object s3Object1 = s3ObjectMapper.getById(objectId);
            Assertions.assertEquals(S3ObjectState.BOS_WILL_DELETE, s3Object1.getState());

            S3StreamObjectMapper s3StreamObjectMapper = session.getMapper(S3StreamObjectMapper.class);
            for (long index = objectId; index < objectId + 2; index++) {
                com.automq.rocketmq.metadata.dao.S3StreamObject object = s3StreamObjectMapper.getByObjectId(index);
                Assertions.assertNull(object);
            }

            com.automq.rocketmq.metadata.dao.S3StreamObject object = s3StreamObjectMapper.getByObjectId(objectId + 2);
            Assertions.assertEquals(111L, object.getObjectSize());
            Assertions.assertEquals(streamId, object.getStreamId());
            Assertions.assertTrue(object.getBaseDataTimestamp().getTime() > 0);
            Assertions.assertTrue(object.getCommittedTimestamp().getTime() > 0);
        }
    }

    @Test
    public void testCommitStreamObject_NoCompacted() throws IOException {
        long objectId, streamId = 1;

        try (S3MetadataService metadataService = new DefaultS3MetadataService(config, getSessionFactory(), executorService)) {
            objectId = metadataService.prepareS3Objects(3, 5).get();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        S3StreamObject news3StreamObject = S3StreamObject.newBuilder()
            .setObjectId(objectId + 2)
            .setStreamId(streamId)
            .setObjectSize(111L)
            .build();

        try (DefaultS3MetadataService service = new DefaultS3MetadataService(config, getSessionFactory(), executorService)) {
            service.commitStreamObject(news3StreamObject, Collections.emptyList());
        }

        try (SqlSession session = getSessionFactory().openSession()) {
            S3ObjectMapper s3ObjectMapper = session.getMapper(S3ObjectMapper.class);
            S3StreamObjectMapper s3StreamObjectMapper = session.getMapper(S3StreamObjectMapper.class);
            S3Object s3Object = s3ObjectMapper.getById(objectId + 2);
            Assertions.assertEquals(S3ObjectState.BOS_COMMITTED, s3Object.getState());

            com.automq.rocketmq.metadata.dao.S3StreamObject object = s3StreamObjectMapper.getByObjectId(objectId + 2);
            Assertions.assertTrue(object.getBaseDataTimestamp().getTime() > 0);
            Assertions.assertTrue(object.getCommittedTimestamp().getTime() > 0);
            Assertions.assertEquals(111L, object.getObjectSize());
            Assertions.assertEquals(streamId, object.getStreamId());
        }
    }

    @Test
    public void testCommitStreamObject_ObjectNotExist() throws IOException {
        long streamId = 1;

        S3StreamObject s3StreamObject = S3StreamObject.newBuilder()
            .setObjectId(1)
            .setStreamId(streamId)
            .setObjectSize(111L)
            .build();

        try (SqlSession session = this.getSessionFactory().openSession()) {
            S3StreamObjectMapper s3StreamObjectMapper = session.getMapper(S3StreamObjectMapper.class);
            buildS3StreamObjs(1, 1, 100L, 100L).forEach(s3StreamObjectMapper::create);
        }

        List<Long> compactedObjects = new ArrayList<>();
        try (S3MetadataService service = new DefaultS3MetadataService(config, getSessionFactory(), executorService)) {
            Assertions.assertThrows(ExecutionException.class, () -> service.commitStreamObject(s3StreamObject, compactedObjects).get());
        }

    }

    @Test
    public void testCommitStreamObject_StreamNotExist() throws IOException {
        long streamId = 1;

        S3StreamObject s3StreamObject = S3StreamObject.newBuilder()
            .setObjectId(-1)
            .setStreamId(streamId)
            .setObjectSize(111L)
            .build();

        try (SqlSession session = this.getSessionFactory().openSession()) {
            S3StreamObjectMapper s3StreamObjectMapper = session.getMapper(S3StreamObjectMapper.class);
            buildS3StreamObjs(1, 1, 100L, 100L).forEach(s3StreamObjectMapper::create);
        }

        List<Long> compactedObjects = new ArrayList<>();
        try (S3MetadataService service = new DefaultS3MetadataService(config, getSessionFactory(), executorService)) {
            Assertions.assertThrows(ExecutionException.class, () -> service.commitStreamObject(s3StreamObject, compactedObjects).get());
        }

    }

    private void insertStream(SqlSession session, long streamId, StreamState state) {
        StreamMapper mapper = session.getMapper(StreamMapper.class);
        Stream stream = new Stream();
        stream.setId(streamId);
        stream.setState(state);
        stream.setQueueId(1);
        stream.setTopicId(1L);
        stream.setSrcNodeId(1);
        stream.setDstNodeId(1);
        stream.setStartOffset(0L);
        stream.setRangeId(0);
        stream.setStreamRole(StreamRole.STREAM_ROLE_DATA);
        mapper.insert(stream);
    }

    private void createRange(SqlSession session, long streamId, int rangeId, long startOffset, long endOffset) {
        RangeMapper mapper = session.getMapper(RangeMapper.class);
        Range range = new Range();
        range.setRangeId(rangeId);
        range.setStartOffset(startOffset);
        range.setEndOffset(endOffset);
        range.setStreamId(streamId);
        range.setNodeId(1);
        range.setEpoch(1L);
        mapper.create(range);
    }

    @Test
    public void testCommitWALObject() throws IOException, ExecutionException, InterruptedException {
        long objectId;
        int nodeId = 1;

        try (S3MetadataService s3MetadataService = new DefaultS3MetadataService(config, getSessionFactory(), executorService)) {
            objectId = s3MetadataService.prepareS3Objects(5, 5).get();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        S3StreamSetObject walObject = S3StreamSetObject.newBuilder()
            .setObjectId(objectId + 4)
            .setObjectSize(222L)
            .setBrokerId(nodeId)
            .build();

        try (SqlSession session = this.getSessionFactory().openSession()) {
            S3StreamSetObjectMapper s3StreamSetObjectMapper = session.getMapper(S3StreamSetObjectMapper.class);

            LongStream.range(1, 3).forEach(streamId -> {
                insertStream(session, streamId, StreamState.OPEN);
                createRange(session, streamId, 0, 0, 20);
            });

            buildS3WalObjs(objectId + 2, 1).stream().peek(s3WalObject -> {
                Map<Long, SubStream> subStreams = buildWalSubStreams(1, 20L, 10L);
                s3WalObject.setSubStreams(toJson(subStreams));
            }).forEach(s3StreamSetObjectMapper::create);

            buildS3WalObjs(objectId + 3, 1).stream().peek(s3WalObject -> {
                Map<Long, SubStream> subStreams = buildWalSubStreams(1, 30L, 10L);
                s3WalObject.setSubStreams(toJson(subStreams));
            }).forEach(s3StreamSetObjectMapper::create);

            session.commit();
        }

        List<Long> compactedObjects = new ArrayList<>();
        compactedObjects.add(objectId + 2);
        compactedObjects.add(objectId + 3);

        List<S3StreamObject> s3StreamObjects = buildS3StreamObjs(objectId, 2, 0, 10)
            .stream().map(s3StreamObject2 -> S3StreamObject.newBuilder()
                .setObjectId(s3StreamObject2.getObjectId())
                .setStreamId(s3StreamObject2.getStreamId())
                .setObjectSize(s3StreamObject2.getObjectSize())
                .setBaseDataTimestamp(s3StreamObject2.getBaseDataTimestamp().getTime())
                .setStartOffset(s3StreamObject2.getStartOffset())
                .setEndOffset(s3StreamObject2.getEndOffset())
                .build()).toList();

        try (S3MetadataService manager = new DefaultS3MetadataService(config, getSessionFactory(), executorService)) {
            manager.commitStreamSetObject(walObject, s3StreamObjects, compactedObjects).get();
        }

        try (SqlSession session = getSessionFactory().openSession()) {
            S3ObjectMapper s3ObjectMapper = session.getMapper(S3ObjectMapper.class);
            for (long index = objectId; index < objectId + 2; index++) {
                S3Object object = s3ObjectMapper.getById(index);
                Assertions.assertEquals(S3ObjectState.BOS_COMMITTED, object.getState());
            }

            for (long index = objectId + 2; index < objectId + 4; index++) {
                S3Object object = s3ObjectMapper.getById(index);
                Assertions.assertEquals(S3ObjectState.BOS_WILL_DELETE, object.getState());
            }

            S3StreamObjectMapper s3StreamObjectMapper = session.getMapper(S3StreamObjectMapper.class);
            for (long index = objectId; index < objectId + 2; index++) {
                com.automq.rocketmq.metadata.dao.S3StreamObject object = s3StreamObjectMapper.getByObjectId(index);
                Assertions.assertTrue(object.getCommittedTimestamp().getTime() > 0);
            }

            S3Object s3Object = s3ObjectMapper.getById(objectId + 4);
            Assertions.assertEquals(222L, s3Object.getObjectSize());
            Assertions.assertEquals(StreamConstants.NOOP_STREAM_ID, s3Object.getStreamId());

            S3StreamSetObjectMapper s3StreamSetObjectMapper = session.getMapper(S3StreamSetObjectMapper.class);
            com.automq.rocketmq.metadata.dao.S3StreamSetObject object = s3StreamSetObjectMapper.getByObjectId(objectId + 4);
            Assertions.assertEquals(objectId + 2, object.getSequenceId());
            Assertions.assertTrue(object.getBaseDataTimestamp().getTime() > 0);
            Assertions.assertTrue(object.getCommittedTimestamp().getTime() > 0);
        }
    }

    @Test
    public void testCommitWalObject_ObjectNotPrepare() throws IOException, ExecutionException, InterruptedException {
        long streamId, startOffset = 0, endOffset = 10;
        Integer nodeId = 1;

        S3StreamSetObject walObject = S3StreamSetObject.newBuilder()
            .setObjectId(3)
            .setBrokerId(-1)
            .build();

        try (SqlSession session = this.getSessionFactory().openSession()) {
            S3StreamObjectMapper s3StreamObjectMapper = session.getMapper(S3StreamObjectMapper.class);
            RangeMapper rangeMapper = session.getMapper(RangeMapper.class);
            StreamMapper streamMapper = session.getMapper(StreamMapper.class);
            Stream stream = new Stream();
            stream.setState(StreamState.OPEN);
            stream.setStreamRole(StreamRole.STREAM_ROLE_DATA);
            stream.setTopicId(2L);
            stream.setRangeId(1);
            stream.setEpoch(3L);
            stream.setTopicId(1L);
            stream.setQueueId(2);
            streamMapper.create(stream);
            streamId = stream.getId();
            Range range = new Range();

            range.setStreamId(streamId);
            range.setRangeId(1);
            range.setEpoch(3L);
            range.setStartOffset(0L);
            range.setEndOffset(0L);
            range.setNodeId(nodeId);
            rangeMapper.create(range);

            buildS3StreamObjs(1, 1, 0L, 100L).forEach(s3StreamObjectMapper::create);
            session.commit();
        }

        List<Long> compactedObjects = new ArrayList<>();
        try (S3MetadataService manager = new DefaultS3MetadataService(config, getSessionFactory(), executorService)) {
            List<S3StreamObject> s3StreamObjects = manager.listStreamObjects(streamId, startOffset, endOffset, 2).get();
            Assertions.assertThrows(ExecutionException.class, () -> manager.commitStreamSetObject(walObject, s3StreamObjects, compactedObjects).get());
        }

    }

    @Test
    public void testCommitWalObject_WalNotExist() throws IOException, ExecutionException, InterruptedException {
        long streamId;
        int nodeId = 1;
        long objectId;
        Calendar calendar = Calendar.getInstance();

        S3StreamSetObject walObject = S3StreamSetObject.newBuilder()
            .setObjectId(-1)
            .setSequenceId(-1)
            .setBrokerId(-1)
            .build();

        try (SqlSession session = this.getSessionFactory().openSession()) {
            RangeMapper rangeMapper = session.getMapper(RangeMapper.class);
            StreamMapper streamMapper = session.getMapper(StreamMapper.class);
            Stream stream = new Stream();
            stream.setState(StreamState.OPEN);
            stream.setStreamRole(StreamRole.STREAM_ROLE_DATA);
            stream.setTopicId(2L);
            stream.setRangeId(1);
            stream.setEpoch(3L);
            stream.setTopicId(1L);
            stream.setQueueId(2);
            streamMapper.create(stream);
            streamId = stream.getId();
            Range range = new Range();

            range.setStreamId(streamId);
            range.setRangeId(1);
            range.setEpoch(3L);
            range.setStartOffset(0L);
            range.setEndOffset(0L);
            range.setNodeId(nodeId);
            rangeMapper.create(range);

            S3ObjectMapper objectMapper = session.getMapper(S3ObjectMapper.class);
            S3Object s3Object = new S3Object();
            s3Object.setId(nextS3ObjectId());
            s3Object.setState(S3ObjectState.BOS_PREPARED);
            s3Object.setStreamId(streamId);
            s3Object.setObjectSize(2139L);

            calendar.add(Calendar.HOUR, 1);
            s3Object.setExpiredTimestamp(calendar.getTime());
            objectMapper.prepare(s3Object);
            objectId = s3Object.getId();

            session.commit();
        }

        List<Long> compactedObjects = new ArrayList<>();
        try (S3MetadataService manager = new DefaultS3MetadataService(config, getSessionFactory(), executorService)) {
            calendar.add(Calendar.HOUR, 2);
            S3StreamObject streamObject = S3StreamObject.newBuilder()
                .setObjectId(objectId)
                .setStreamId(streamId)
                .setBaseDataTimestamp(calendar.getTimeInMillis())
                .setStartOffset(0)
                .setEndOffset(2)
                .setObjectSize(2139)
                .build();

            List<S3StreamObject> s3StreamObjects = new ArrayList<>();
            s3StreamObjects.add(streamObject);
            manager.commitStreamSetObject(walObject, s3StreamObjects, compactedObjects).get();
        }

        try (SqlSession session = getSessionFactory().openSession()) {
            S3StreamObjectMapper mapper = session.getMapper(S3StreamObjectMapper.class);
            com.automq.rocketmq.metadata.dao.S3StreamObject s3StreamObject = mapper.getByObjectId(objectId);
            Assertions.assertTrue(s3StreamObject.getCommittedTimestamp().getTime() > 0);

            S3ObjectMapper objectMapper = session.getMapper(S3ObjectMapper.class);
            S3Object s3Object = objectMapper.getById(objectId);
            Assertions.assertNotNull(s3Object.getCommittedTimestamp());
            Assertions.assertEquals(S3ObjectState.BOS_COMMITTED, s3Object.getState());

            RangeMapper rangeMapper = session.getMapper(RangeMapper.class);
            Range range = rangeMapper.get(1, streamId, null);
            Assertions.assertEquals(2, range.getEndOffset());
        }

    }
}
