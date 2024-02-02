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

import apache.rocketmq.controller.v1.StreamRole;
import apache.rocketmq.controller.v1.StreamState;
import com.automq.rocketmq.metadata.dao.Stream;
import com.automq.rocketmq.metadata.dao.StreamCriteria;
import com.automq.rocketmq.metadata.mapper.StreamMapper;
import org.apache.ibatis.session.SqlSession;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import java.io.IOException;
import java.util.List;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class StreamTest extends DatabaseTestBase {

    @Test
    @Order(1)
    public void testCreateStream() throws IOException {
        try (SqlSession session = this.getSessionFactory().openSession()) {
            StreamMapper streamMapper = session.getMapper(StreamMapper.class);
            Stream stream = new Stream();
            stream.setTopicId(1L);
            stream.setQueueId(2);
            stream.setStreamRole(StreamRole.STREAM_ROLE_OPS);
            stream.setStartOffset(1234L);
            stream.setEpoch(1L);
            stream.setRangeId(2);
            stream.setState(StreamState.OPEN);

            int affectedRows = streamMapper.create(stream);
            Assertions.assertEquals(1, affectedRows);

            Stream createdStream = streamMapper.getByStreamId(stream.getId());
            Assertions.assertNotNull(createdStream);
            Assertions.assertEquals(1, createdStream.getEpoch());
            Assertions.assertEquals(2, createdStream.getRangeId());
            Assertions.assertEquals(StreamState.OPEN, createdStream.getState());
            Assertions.assertEquals(1, createdStream.getTopicId());
            Assertions.assertEquals(2, createdStream.getQueueId());
            Assertions.assertEquals(StreamRole.STREAM_ROLE_OPS, createdStream.getStreamRole());


            streamMapper.delete(createdStream.getId());

            List<Stream> streams = streamMapper.byCriteria(StreamCriteria.newBuilder().build());
            Assertions.assertTrue(streams.isEmpty());
        }
    }

    @Test
    @Order(2)
    public void testIncreaseEpoch() throws IOException {
        try (SqlSession session = this.getSessionFactory().openSession()) {
            StreamMapper streamMapper = session.getMapper(StreamMapper.class);
            Stream stream = new Stream();
            stream.setId(123L);
            stream.setStartOffset(1234L);
            stream.setEpoch(1L);
            stream.setRangeId(2);
            stream.setTopicId(1L);
            stream.setQueueId(2);
            stream.setState(StreamState.OPEN);
            stream.setStreamRole(StreamRole.STREAM_ROLE_DATA);

            int affectedRows = streamMapper.create(stream);
            Assertions.assertEquals(1, affectedRows);

            Stream stream1 = streamMapper.getByStreamId(stream.getId());
            Assertions.assertEquals(1, stream1.getEpoch());
            affectedRows = streamMapper.increaseEpoch(stream.getId());
            Assertions.assertEquals(1, affectedRows);

            stream1 = streamMapper.getByStreamId(stream.getId());
            Assertions.assertEquals(2, stream1.getEpoch());
            streamMapper.delete(stream1.getId());
        }
    }

    @Test
    @Order(3)
    public void testUpdateLastRange() throws IOException {
        try (SqlSession session = this.getSessionFactory().openSession()) {
            StreamMapper streamMapper = session.getMapper(StreamMapper.class);
            Stream stream = new Stream();
            stream.setId(123L);
            stream.setStartOffset(1234L);
            stream.setEpoch(1L);
            stream.setRangeId(2);
            stream.setTopicId(1L);
            stream.setQueueId(2);
            stream.setState(StreamState.OPEN);
            stream.setStreamRole(StreamRole.STREAM_ROLE_DATA);
            int affectedRows = streamMapper.create(stream);
            Assertions.assertEquals(1, affectedRows);

            Stream stream1 = streamMapper.getByStreamId(stream.getId());
            Assertions.assertEquals(2, stream1.getRangeId());
            affectedRows = streamMapper.updateLastRange(stream1.getId(), 4);
            Assertions.assertEquals(1, affectedRows);

            stream1 = streamMapper.getByStreamId(stream.getId());
            Assertions.assertEquals(4, stream1.getRangeId());
            streamMapper.delete(stream1.getId());
        }
    }

    @Test
    @Order(4)
    public void testUpdateStreamState() throws IOException {
        try (SqlSession session = this.getSessionFactory().openSession()) {
            StreamMapper streamMapper = session.getMapper(StreamMapper.class);
            Stream stream = new Stream();
            stream.setId(123L);
            stream.setStartOffset(1234L);
            stream.setEpoch(1L);
            stream.setRangeId(2);
            stream.setTopicId(1L);
            stream.setQueueId(2);
            stream.setState(StreamState.OPEN);
            stream.setStreamRole(StreamRole.STREAM_ROLE_DATA);
            int affectedRows = streamMapper.create(stream);
            Assertions.assertEquals(1, affectedRows);

            Stream stream1 = streamMapper.getByStreamId(stream.getId());
            Assertions.assertEquals(StreamState.OPEN, stream1.getState());

            StreamCriteria criteria = StreamCriteria.newBuilder()
                .addStreamId(stream1.getId())
                .withState(StreamState.OPEN)
                .build();
            affectedRows = streamMapper.updateStreamState(criteria, StreamState.CLOSED);
            Assertions.assertEquals(1, affectedRows);

            stream1 = streamMapper.getByStreamId(stream.getId());
            Assertions.assertEquals(StreamState.forNumber(StreamState.CLOSED_VALUE), stream1.getState());
            streamMapper.delete(stream1.getId());
        }
    }

}
