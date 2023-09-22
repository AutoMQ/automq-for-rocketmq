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

import apache.rocketmq.controller.v1.StreamMetadata;
import apache.rocketmq.controller.v1.StreamState;
import com.automq.rocketmq.controller.metadata.database.mapper.StreamMapper;
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
            StreamMetadata stream = StreamMetadata.newBuilder().
                setStreamId(123).
                setEpoch(1).
                setRangeId(2).
                setState(StreamState.OPEN).build();

            int affectedRows = streamMapper.create(stream);
            Assertions.assertEquals(1, affectedRows);

            StreamMetadata createdStream = streamMapper.getByStreamId(stream.getStreamId());
            Assertions.assertNotNull(createdStream);
            Assertions.assertEquals(123, createdStream.getStreamId());
            Assertions.assertEquals(1, createdStream.getEpoch());
            Assertions.assertEquals(2, createdStream.getRangeId());
            Assertions.assertEquals(StreamState.OPEN, createdStream.getState());

            streamMapper.delete(createdStream.getStreamId());

            List<StreamMetadata> streams = streamMapper.list();
            Assertions.assertTrue(streams.isEmpty());
        }
    }

    @Test
    @Order(2)
    public void testIncreaseEpoch() throws IOException {
        try (SqlSession session = this.getSessionFactory().openSession()) {
            StreamMapper streamMapper = session.getMapper(StreamMapper.class);
            StreamMetadata stream = StreamMetadata.newBuilder().
                setStreamId(123).
                setEpoch(1).
                setRangeId(2).
                setState(StreamState.OPEN).build();
            int affectedRows = streamMapper.create(stream);
            Assertions.assertEquals(1, affectedRows);

            StreamMetadata stream1 = streamMapper.getByStreamId(stream.getStreamId());
            Assertions.assertEquals(1, stream1.getEpoch());
            affectedRows = streamMapper.increaseEpoch(stream.getStreamId());
            Assertions.assertEquals(1, affectedRows);

            stream1 = streamMapper.getByStreamId(stream.getStreamId());
            Assertions.assertEquals(2, stream1.getEpoch());
            streamMapper.delete(stream1.getStreamId());
        }
    }

    @Test
    @Order(3)
    public void testUpdateLastRange() throws IOException {
        try (SqlSession session = this.getSessionFactory().openSession()) {
            StreamMapper streamMapper = session.getMapper(StreamMapper.class);
            StreamMetadata stream = StreamMetadata.newBuilder().
                setStreamId(123).
                setEpoch(1).
                setRangeId(2).
                setState(StreamState.OPEN).build();
            int affectedRows = streamMapper.create(stream);
            Assertions.assertEquals(1, affectedRows);


            StreamMetadata stream1 = streamMapper.getByStreamId(stream.getStreamId());
            Assertions.assertEquals(2, stream1.getRangeId());
            affectedRows = streamMapper.updateLastRange(stream1.getStreamId(), 4);
            Assertions.assertEquals(1, affectedRows);

            stream1 = streamMapper.getByStreamId(stream.getStreamId());
            Assertions.assertEquals(4, stream1.getRangeId());
            streamMapper.delete(stream1.getStreamId());
        }
    }

    @Test
    @Order(4)
    public void testUpdateStreamState() throws IOException {
        try (SqlSession session = this.getSessionFactory().openSession()) {
            StreamMapper streamMapper = session.getMapper(StreamMapper.class);
            StreamMetadata stream = StreamMetadata.newBuilder().
                setStreamId(123).
                setEpoch(1).
                setRangeId(2).
                setState(StreamState.OPEN).build();
            int affectedRows = streamMapper.create(stream);
            Assertions.assertEquals(1, affectedRows);


            StreamMetadata stream1 = streamMapper.getByStreamId(stream.getStreamId());
            Assertions.assertEquals(StreamState.OPEN, stream1.getState());
            affectedRows = streamMapper.updateStreamState(stream1.getStreamId(), StreamState.CLOSED_VALUE);
            Assertions.assertEquals(1, affectedRows);

            stream1 = streamMapper.getByStreamId(stream.getStreamId());
            Assertions.assertEquals(StreamState.CLOSED_VALUE, stream1.getStateValue());
            streamMapper.delete(stream1.getStreamId());
        }
    }

}
