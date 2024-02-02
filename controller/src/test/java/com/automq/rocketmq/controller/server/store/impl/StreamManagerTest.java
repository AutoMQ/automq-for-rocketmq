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

package com.automq.rocketmq.controller.server.store.impl;

import apache.rocketmq.controller.v1.AssignmentStatus;
import apache.rocketmq.controller.v1.DescribeStreamRequest;
import apache.rocketmq.controller.v1.S3ObjectState;
import apache.rocketmq.controller.v1.StreamMetadata;
import apache.rocketmq.controller.v1.StreamRole;
import apache.rocketmq.controller.v1.StreamState;
import com.automq.rocketmq.common.api.DataStore;
import com.automq.rocketmq.controller.ControllerClient;
import com.automq.rocketmq.controller.MetadataStore;
import com.automq.rocketmq.controller.server.store.DefaultMetadataStore;
import com.automq.rocketmq.controller.store.DatabaseTestBase;
import com.automq.rocketmq.metadata.dao.Group;
import com.automq.rocketmq.metadata.dao.S3Object;
import com.automq.rocketmq.metadata.dao.S3StreamObject;
import com.automq.rocketmq.metadata.dao.Stream;
import com.automq.rocketmq.metadata.mapper.S3ObjectMapper;
import com.automq.rocketmq.metadata.mapper.S3StreamObjectMapper;
import com.automq.rocketmq.metadata.mapper.StreamMapper;
import java.io.IOException;
import java.util.Date;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import org.apache.ibatis.session.SqlSession;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

class StreamManagerTest extends DatabaseTestBase {

    @Test
    public void testGetStream() throws IOException {
        createAssignment(1, 2, 3, 4, AssignmentStatus.ASSIGNMENT_STATUS_ASSIGNED);
        Group group = createGroup("G1");
        try (MetadataStore store = new DefaultMetadataStore(getControllerClient(), getSessionFactory(), config)) {
            store.start();
            awaitElectedAsLeader(store);

            StreamManager streamManager = new StreamManager(store);
            StreamMetadata stream = streamManager.getStream(1, 2, group.getId(), StreamRole.STREAM_ROLE_RETRY).join();
            Assertions.assertNotNull(stream);

            streamManager.describeStream(DescribeStreamRequest.newBuilder().setStreamId(stream.getStreamId()).build());
        }
    }

    @Test
    public void testGetStream_NotFound() throws IOException {
        Group group = createGroup("G1");
        try (MetadataStore store = new DefaultMetadataStore(getControllerClient(), getSessionFactory(), config)) {
            store.start();
            awaitElectedAsLeader(store);

            StreamManager streamManager = new StreamManager(store);
            Assertions.assertThrows(CompletionException.class, () -> {
                streamManager.getStream(1, 2, group.getId(), StreamRole.STREAM_ROLE_RETRY).join();
            });
        }
    }

    @Test
    public void testGetStream_IllegalState() throws IOException {
        createAssignment(1, 2, 3, 4, AssignmentStatus.ASSIGNMENT_STATUS_ASSIGNED);
        createAssignment(1, 2, 3, 4, AssignmentStatus.ASSIGNMENT_STATUS_ASSIGNED);
        Group group = createGroup("G1");
        try (MetadataStore store = new DefaultMetadataStore(getControllerClient(), getSessionFactory(), config)) {
            store.start();
            awaitElectedAsLeader(store);

            StreamManager streamManager = new StreamManager(store);
            Assertions.assertThrows(CompletionException.class, () -> {
                streamManager.getStream(1, 2, group.getId(), StreamRole.STREAM_ROLE_RETRY).join();
            });
        }
    }

    @Test
    public void testGetStream_AssignmentStatus() throws IOException {
        createAssignment(1, 2, 3, 4, AssignmentStatus.ASSIGNMENT_STATUS_YIELDING);
        Group group = createGroup("G1");
        try (MetadataStore store = new DefaultMetadataStore(getControllerClient(), getSessionFactory(), config)) {
            store.start();
            awaitElectedAsLeader(store);

            StreamManager streamManager = new StreamManager(store);
            Assertions.assertThrows(CompletionException.class, () -> {
                streamManager.getStream(1, 2, group.getId(), StreamRole.STREAM_ROLE_RETRY).join();
            });
        }
    }



    @Test
    public void testDeleteStream() throws IOException {

        long streamId;
        try (SqlSession session = getSessionFactory().openSession()) {
            Stream stream = new Stream();
            stream.setState(StreamState.UNINITIALIZED);
            stream.setStreamRole(StreamRole.STREAM_ROLE_DATA);
            stream.setEpoch(-1L);
            stream.setTopicId(1L);
            stream.setQueueId(2);
            stream.setRangeId(3);
            stream.setStartOffset(100L);
            stream.setSrcNodeId(5);
            stream.setDstNodeId(6);
            StreamMapper mapper = session.getMapper(StreamMapper.class);
            mapper.create(stream);
            streamId = stream.getId();

            S3StreamObjectMapper streamObjectMapper = session.getMapper(S3StreamObjectMapper.class);
            S3StreamObject streamObject = new S3StreamObject();
            streamObject.setObjectId(1L);
            streamObject.setStreamId(streamId);
            streamObject.setStartOffset(2L);
            streamObject.setEndOffset(4L);
            streamObject.setCreatedTimestamp(new Date());
            streamObject.setBaseDataTimestamp(new Date());
            streamObject.setCommittedTimestamp(new Date());
            streamObject.setObjectSize(5L);
            streamObjectMapper.create(streamObject);

            S3ObjectMapper s3ObjectMapper = session.getMapper(S3ObjectMapper.class);
            S3Object s3Object = new S3Object();
            s3Object.setId(1L);
            s3Object.setStreamId(streamId);
            s3Object.setObjectSize(5L);
            s3Object.setState(S3ObjectState.BOS_COMMITTED);
            s3Object.setPreparedTimestamp(new Date());
            s3Object.setCommittedTimestamp(new Date());
            s3Object.setExpiredTimestamp(new Date());
            s3ObjectMapper.prepare(s3Object);
            s3ObjectMapper.commit(s3Object);
            session.commit();
        }

        ControllerClient controllerClient = Mockito.mock(ControllerClient.class);
        DataStore dataStore = Mockito.mock(DataStore.class);
        Mockito.when(dataStore.batchDeleteS3Objects(ArgumentMatchers.anyList())).thenAnswer(r -> CompletableFuture.completedFuture(r.getArgument(0)));
        try (MetadataStore store = new DefaultMetadataStore(controllerClient, getSessionFactory(), config)) {
            store.setDataStore(dataStore);
            StreamManager streamManager = new StreamManager(store);
            streamManager.deleteStream(streamId);
            streamManager.deleteStream(streamId);

            try (SqlSession session = getSessionFactory().openSession()) {
                S3StreamObjectMapper streamObjectMapper = session.getMapper(S3StreamObjectMapper.class);
                Assertions.assertTrue(streamObjectMapper.listByStreamId(streamId).isEmpty());
            }
        }
    }
}