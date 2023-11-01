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

package com.automq.rocketmq.controller.metadata.database;

import apache.rocketmq.controller.v1.AcceptTypes;
import apache.rocketmq.controller.v1.AssignmentStatus;
import apache.rocketmq.controller.v1.ConsumerGroup;
import apache.rocketmq.controller.v1.CreateTopicRequest;
import apache.rocketmq.controller.v1.GroupStatus;
import apache.rocketmq.controller.v1.GroupType;
import apache.rocketmq.controller.v1.S3ObjectState;
import apache.rocketmq.controller.v1.S3StreamObject;
import apache.rocketmq.controller.v1.S3WALObject;
import apache.rocketmq.controller.v1.StreamMetadata;
import apache.rocketmq.controller.v1.StreamRole;
import apache.rocketmq.controller.v1.StreamState;
import apache.rocketmq.controller.v1.SubStream;
import apache.rocketmq.controller.v1.TopicStatus;
import apache.rocketmq.controller.v1.MessageType;
import apache.rocketmq.controller.v1.UpdateTopicRequest;
import com.automq.rocketmq.common.system.StreamConstants;
import com.automq.rocketmq.controller.exception.ControllerException;
import com.automq.rocketmq.controller.metadata.ControllerClient;
import com.automq.rocketmq.controller.metadata.DatabaseTestBase;
import com.automq.rocketmq.controller.metadata.MetadataStore;
import com.automq.rocketmq.controller.metadata.Role;
import com.automq.rocketmq.controller.metadata.database.dao.Group;
import com.automq.rocketmq.controller.metadata.database.dao.Lease;
import com.automq.rocketmq.controller.metadata.database.dao.Node;
import com.automq.rocketmq.controller.metadata.database.dao.QueueAssignment;
import com.automq.rocketmq.controller.metadata.database.dao.S3Object;
import com.automq.rocketmq.controller.metadata.database.dao.S3WalObject;
import com.automq.rocketmq.controller.metadata.database.dao.Stream;
import com.automq.rocketmq.controller.metadata.database.dao.Range;
import com.automq.rocketmq.controller.metadata.database.dao.StreamCriteria;
import com.automq.rocketmq.controller.metadata.database.dao.Topic;
import com.automq.rocketmq.controller.metadata.database.mapper.GroupMapper;
import com.automq.rocketmq.controller.metadata.database.mapper.NodeMapper;
import com.automq.rocketmq.controller.metadata.database.mapper.QueueAssignmentMapper;
import com.automq.rocketmq.controller.metadata.database.mapper.RangeMapper;
import com.automq.rocketmq.controller.metadata.database.mapper.S3ObjectMapper;
import com.automq.rocketmq.controller.metadata.database.mapper.S3StreamObjectMapper;
import com.automq.rocketmq.controller.metadata.database.mapper.S3WalObjectMapper;
import com.automq.rocketmq.controller.metadata.database.mapper.StreamMapper;
import com.automq.rocketmq.controller.metadata.database.mapper.TopicMapper;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.ibatis.session.SqlSession;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class DefaultMetadataStoreTest extends DatabaseTestBase {
    ControllerClient client;

    public DefaultMetadataStoreTest() {
        this.client = Mockito.mock(ControllerClient.class);
    }

    @Test
    void testRegisterNode() throws IOException, ExecutionException, InterruptedException {
        int nodeId;
        try (DefaultMetadataStore metadataStore = new DefaultMetadataStore(client, getSessionFactory(), config)) {
            metadataStore.start();
            Awaitility.await().with().atMost(10, TimeUnit.SECONDS)
                .pollInterval(100, TimeUnit.MILLISECONDS)
                .until(metadataStore::isLeader);

            String name = "broker-0";
            String address = "localhost:1234";
            String instanceId = "i-register";
            Node node = metadataStore.registerBrokerNode(name, address, instanceId).get();
            Assertions.assertTrue(node.getId() > 0);
            nodeId = node.getId();
        }

        try (SqlSession session = this.getSessionFactory().openSession()) {
            NodeMapper nodeMapper = session.getMapper(NodeMapper.class);
            nodeMapper.delete(nodeId);
            session.commit();
        }
    }

    @Test
    void testRegisterBroker_badArguments() throws IOException {
        try (DefaultMetadataStore metadataStore = new DefaultMetadataStore(client, getSessionFactory(), config)) {
            metadataStore.start();
            Awaitility.await().with().atMost(10, TimeUnit.SECONDS)
                .pollInterval(100, TimeUnit.MILLISECONDS)
                .until(metadataStore::isLeader);
            String name = "test-broker -0";
            String address = "localhost:1234";
            String instanceId = "i-register";
            Assertions.assertThrows(CompletionException.class, () -> metadataStore.registerBrokerNode("", address, instanceId).join());
            Assertions.assertThrows(CompletionException.class, () -> metadataStore.registerBrokerNode(name, null, instanceId).join());
            Assertions.assertThrows(CompletionException.class, () -> metadataStore.registerBrokerNode(name, address, "").join());
        }
    }

    /**
     * Dummy test, should be removed later
     */
    @Test
    void testGetLease() throws IOException {
        try (DefaultMetadataStore metadataStore = new DefaultMetadataStore(client, getSessionFactory(), config)) {
            Assertions.assertNull(metadataStore.getLease());
        }
    }

    @Test
    void testIsLeader() throws IOException {
        try (MetadataStore metadataStore = new DefaultMetadataStore(client, getSessionFactory(), config)) {
            metadataStore.start();
            Awaitility.await()
                .with()
                .atMost(10, TimeUnit.SECONDS)
                .pollInterval(100, TimeUnit.MILLISECONDS).until(metadataStore::isLeader);
        }
    }

    @Test
    void testLeaderAddress() throws IOException, ControllerException {
        String address = "localhost:1234";
        int nodeId;
        try (SqlSession session = getSessionFactory().openSession()) {
            NodeMapper nodeMapper = session.getMapper(NodeMapper.class);
            Node node = new Node();
            node.setAddress(address);
            node.setName("broker-test-name");
            node.setInstanceId("i-leader-address");
            nodeMapper.create(node);
            nodeId = node.getId();
            session.commit();
        }
        Mockito.when(config.nodeId()).thenReturn(nodeId);

        try (DefaultMetadataStore metadataStore = new DefaultMetadataStore(client, getSessionFactory(), config)) {
            metadataStore.start();
            Awaitility.await()
                .with()
                .atMost(10, TimeUnit.SECONDS)
                .pollInterval(100, TimeUnit.MILLISECONDS).until(metadataStore::isLeader);

            Awaitility.await()
                .with()
                .atMost(10, TimeUnit.SECONDS)
                .pollInterval(100, TimeUnit.MILLISECONDS).until(() -> !metadataStore.getNodes().isEmpty());

            Assertions.assertEquals(metadataStore.getLease().getNodeId(), nodeId);

            String addr = metadataStore.leaderAddress();
            Assertions.assertEquals(address, addr);
        }
    }

    @Test
    void testLeaderAddress_NoLeader() throws IOException {
        try (MetadataStore metadataStore = new DefaultMetadataStore(client, getSessionFactory(), config)) {
            Assertions.assertThrows(ControllerException.class, metadataStore::leaderAddress);
        }
    }

    @Test
    void testLeaderAddress_NoLeaderNode() throws IOException {
        try (MetadataStore metadataStore = new DefaultMetadataStore(client, getSessionFactory(), config)) {
            metadataStore.start();
            Awaitility.await()
                .with()
                .atMost(10, TimeUnit.SECONDS)
                .pollInterval(100, TimeUnit.MILLISECONDS).until(metadataStore::isLeader);

            Assertions.assertThrows(ControllerException.class, metadataStore::leaderAddress);
        }
    }

    @Test
    void testCreateTopic() throws IOException, ExecutionException, InterruptedException {
        String address = "localhost:1234";
        int nodeId;
        try (SqlSession session = getSessionFactory().openSession()) {
            NodeMapper nodeMapper = session.getMapper(NodeMapper.class);
            Node node = new Node();
            node.setAddress(address);
            node.setName("broker-test-name");
            node.setInstanceId("i-leader-address");
            nodeMapper.create(node);
            nodeId = node.getId();
            session.commit();
        }
        Mockito.when(config.nodeId()).thenReturn(nodeId);

        long topicId;
        int queueNum = 4;
        String topicName = "t1";
        try (MetadataStore metadataStore = new DefaultMetadataStore(client, getSessionFactory(), config)) {
            metadataStore.start();
            Awaitility.await().with().atMost(10, TimeUnit.SECONDS)
                .pollInterval(100, TimeUnit.MILLISECONDS)
                .until(metadataStore::isLeader);

            Awaitility.await().with().atMost(10, TimeUnit.SECONDS)
                .pollInterval(100, TimeUnit.MILLISECONDS)
                .until(metadataStore::hasAliveBrokerNodes);

            CreateTopicRequest request = CreateTopicRequest.newBuilder()
                .setTopic(topicName)
                .setCount(queueNum)
                .setAcceptTypes(AcceptTypes.newBuilder()
                    .addTypes(MessageType.NORMAL)
                    .addTypes(MessageType.FIFO)
                    .addTypes(MessageType.TRANSACTION)
                    .build())
                .build();
            topicId = metadataStore.createTopic(request).get();
        }

        try (SqlSession session = getSessionFactory().openSession()) {
            TopicMapper topicMapper = session.getMapper(TopicMapper.class);
            List<Topic> topics = topicMapper.list(null, null);
            topics.stream().filter(topic -> topic.getName().equals("t1")).forEach(topic -> Assertions.assertEquals(4, topic.getQueueNum()));

            QueueAssignmentMapper assignmentMapper = session.getMapper(QueueAssignmentMapper.class);
            List<QueueAssignment> assignments = assignmentMapper.list(topicId, null, null, null, null);
            Assertions.assertEquals(4, assignments.size());

            StreamMapper streamMapper = session.getMapper(StreamMapper.class);
            List<Stream> streams = streamMapper.byCriteria(StreamCriteria.newBuilder().withTopicId(topicId).build());
            // By default, we create 3 streams for each message queue: data, ops, snapshot
            Assertions.assertEquals(queueNum * 3, streams.size());
        }
    }

    private static AcceptTypes decodeAcceptTypes(String json) throws InvalidProtocolBufferException {
        AcceptTypes.Builder builder = AcceptTypes.newBuilder();
        JsonFormat.parser().ignoringUnknownFields().merge(json, builder);
        return builder.build();
    }

    @Test
    void testUpdateTopic() throws IOException, ExecutionException, InterruptedException {
        String address = "localhost:1234";
        int nodeId;
        try (SqlSession session = getSessionFactory().openSession()) {
            NodeMapper nodeMapper = session.getMapper(NodeMapper.class);
            Node node = new Node();
            node.setAddress(address);
            node.setName("broker-test-name");
            node.setInstanceId("i-leader-address");
            nodeMapper.create(node);
            nodeId = node.getId();
            session.commit();
        }
        Mockito.when(config.nodeId()).thenReturn(nodeId);

        long topicId;
        int queueNum = 4;
        String topicName = "t1";

        AcceptTypes acceptTypes = AcceptTypes.newBuilder()
            .addTypes(MessageType.NORMAL)
            .addTypes(MessageType.TRANSACTION)
            .build();

        try (MetadataStore metadataStore = new DefaultMetadataStore(client, getSessionFactory(), config)) {
            metadataStore.start();
            Awaitility.await().with().atMost(10, TimeUnit.SECONDS)
                .pollInterval(100, TimeUnit.MILLISECONDS)
                .until(metadataStore::isLeader);

            Awaitility.await().with().atMost(10, TimeUnit.SECONDS)
                .pollInterval(100, TimeUnit.MILLISECONDS)
                .until(metadataStore::hasAliveBrokerNodes);

            CreateTopicRequest request = CreateTopicRequest.newBuilder()
                .setTopic(topicName)
                .setCount(queueNum)
                .setAcceptTypes(AcceptTypes.newBuilder()
                    .addTypes(MessageType.NORMAL)
                    .addTypes(MessageType.FIFO)
                    .addTypes(MessageType.TRANSACTION)
                    .build())
                .build();
            topicId = metadataStore.createTopic(request).get();

            UpdateTopicRequest updateTopicRequest = UpdateTopicRequest.newBuilder()
                .setTopicId(topicId)
                .setName(topicName)
                .setAcceptTypes(acceptTypes)
                .build();
            metadataStore.updateTopic(updateTopicRequest).get();
        }

        try (SqlSession session = getSessionFactory().openSession()) {
            TopicMapper topicMapper = session.getMapper(TopicMapper.class);
            List<Topic> topics = topicMapper.list(null, null);
            topics.stream().filter(topic -> topic.getName().equals("t1")).forEach(topic -> Assertions.assertEquals(4, topic.getQueueNum()));

            topics.stream().filter(topic -> topic.getName().equals("t1"))
                .forEach(topic -> {
                    try {
                        Assertions.assertEquals(acceptTypes, decodeAcceptTypes(topic.getAcceptMessageTypes()));
                    } catch (InvalidProtocolBufferException e) {
                        Assertions.fail(e);
                    }
                });

            QueueAssignmentMapper assignmentMapper = session.getMapper(QueueAssignmentMapper.class);
            List<QueueAssignment> assignments = assignmentMapper.list(topicId, null, null, null, null);
            Assertions.assertEquals(4, assignments.size());

            StreamMapper streamMapper = session.getMapper(StreamMapper.class);
            List<Stream> streams = streamMapper.byCriteria(StreamCriteria.newBuilder().withTopicId(topicId).build());
            // By default, we create 3 streams for each message queue: data, ops, snapshot
            Assertions.assertEquals(queueNum * 3, streams.size());
        }
    }

    @Test
    public void testListAssignments() throws IOException, ExecutionException, InterruptedException {
        try (SqlSession session = getSessionFactory().openSession()) {
            QueueAssignmentMapper mapper = session.getMapper(QueueAssignmentMapper.class);
            QueueAssignment assignment = new QueueAssignment();
            assignment.setTopicId(1);
            assignment.setStatus(AssignmentStatus.ASSIGNMENT_STATUS_ASSIGNED);
            assignment.setDstNodeId(2);
            assignment.setSrcNodeId(3);
            assignment.setQueueId(4);
            int affectedRows = mapper.create(assignment);
            Assertions.assertEquals(1, affectedRows);
            session.commit();
        }

        try (MetadataStore metadataStore = new DefaultMetadataStore(client, getSessionFactory(), config)) {
            List<QueueAssignment> assignmentList = metadataStore.listAssignments(null, null, null, null).get();
            Assertions.assertEquals(1, assignmentList.size());
            QueueAssignment assignment = assignmentList.get(0);
            Assertions.assertEquals(1, assignment.getTopicId());
            Assertions.assertEquals(AssignmentStatus.ASSIGNMENT_STATUS_ASSIGNED, assignment.getStatus());
            Assertions.assertEquals(2, assignment.getDstNodeId());
            Assertions.assertEquals(3, assignment.getSrcNodeId());
            Assertions.assertEquals(4, assignment.getQueueId());
        }
    }

    @Test
    public void testDeleteTopic() throws IOException, ExecutionException, InterruptedException {
        String address = "localhost:1234";
        int nodeId;
        long topicId;
        try (SqlSession session = getSessionFactory().openSession()) {
            NodeMapper nodeMapper = session.getMapper(NodeMapper.class);
            Node node = new Node();
            node.setAddress(address);
            node.setName("broker-test-name");
            node.setInstanceId("i-leader-address");
            nodeMapper.create(node);
            nodeId = node.getId();

            TopicMapper topicMapper = session.getMapper(TopicMapper.class);
            Topic topic = new Topic();
            topic.setStatus(TopicStatus.TOPIC_STATUS_ACTIVE);
            topic.setName("T");
            topic.setQueueNum(1);
            topic.setAcceptMessageTypes("[\"NORMAL\",\"DELAY\"]");
            topicMapper.create(topic);
            topicId = topic.getId();
            session.commit();
        }
        Mockito.when(config.nodeId()).thenReturn(nodeId);

        try (MetadataStore metadataStore = new DefaultMetadataStore(client, getSessionFactory(), config)) {
            metadataStore.start();
            Awaitility.await().with().atMost(10, TimeUnit.SECONDS).pollInterval(100, TimeUnit.MILLISECONDS)
                .until(metadataStore::isLeader);

            metadataStore.deleteTopic(topicId).get();
        }

        try (SqlSession session = this.getSessionFactory().openSession()) {
            NodeMapper nodeMapper = session.getMapper(NodeMapper.class);
            nodeMapper.delete(nodeId);
            session.commit();
        }

    }

    @Test
    public void testDeleteTopic_NotFound() throws IOException {
        try (MetadataStore metadataStore = new DefaultMetadataStore(client, getSessionFactory(), config)) {
            metadataStore.start();
            Awaitility.await().with().atMost(10, TimeUnit.SECONDS).pollInterval(100, TimeUnit.MILLISECONDS)
                .until(metadataStore::isLeader);
            Assertions.assertThrows(ExecutionException.class, () -> metadataStore.deleteTopic(1).get());
        }
    }

    @Test
    public void testDescribeTopic() throws IOException, ExecutionException, InterruptedException {
        long topicId;
        AcceptTypes acceptTypes = AcceptTypes.newBuilder()
            .addTypes(MessageType.NORMAL)
            .addTypes(MessageType.DELAY)
            .build();
        String messageType = JsonFormat.printer().print(acceptTypes);
        try (SqlSession session = getSessionFactory().openSession()) {

            TopicMapper topicMapper = session.getMapper(TopicMapper.class);
            Topic topic = new Topic();
            topic.setName("T1");
            topic.setStatus(TopicStatus.TOPIC_STATUS_ACTIVE);
            topic.setQueueNum(0);
            topic.setAcceptMessageTypes(messageType);
            topicMapper.create(topic);
            topicId = topic.getId();

            QueueAssignmentMapper assignmentMapper = session.getMapper(QueueAssignmentMapper.class);
            QueueAssignment assignment = new QueueAssignment();
            assignment.setTopicId(topicId);
            assignment.setStatus(AssignmentStatus.ASSIGNMENT_STATUS_ASSIGNED);
            assignment.setQueueId(1);
            assignment.setDstNodeId(2);
            assignment.setSrcNodeId(3);
            assignmentMapper.create(assignment);

            assignment = new QueueAssignment();
            assignment.setTopicId(topicId);
            assignment.setStatus(AssignmentStatus.ASSIGNMENT_STATUS_YIELDING);
            assignment.setSrcNodeId(3);
            assignment.setDstNodeId(2);
            assignment.setQueueId(2);
            assignmentMapper.create(assignment);
            session.commit();
        }

        try (MetadataStore metadataStore = new DefaultMetadataStore(client, getSessionFactory(), config)) {
            metadataStore.start();
            Awaitility.await().with().atMost(10, TimeUnit.SECONDS).pollInterval(100, TimeUnit.MILLISECONDS)
                .until(metadataStore::isLeader);
            apache.rocketmq.controller.v1.Topic topic = metadataStore.describeTopic(topicId, null).get();
            Assertions.assertEquals("T1", topic.getName());
            Assertions.assertEquals(1, topic.getAssignmentsCount());
            Assertions.assertEquals(1, topic.getReassignmentsCount());
            Assertions.assertEquals(messageType, JsonFormat.printer().print(topic.getAcceptTypes()));
        }
    }

    @Test
    public void testMarkMessageQueueAssignable() throws IOException, ExecutionException, InterruptedException {
        try (SqlSession session = getSessionFactory().openSession()) {
            QueueAssignmentMapper assignmentMapper = session.getMapper(QueueAssignmentMapper.class);
            QueueAssignment assignment = new QueueAssignment();
            assignment.setQueueId(1);
            assignment.setTopicId(2);
            assignment.setStatus(AssignmentStatus.ASSIGNMENT_STATUS_YIELDING);
            assignmentMapper.create(assignment);
            session.commit();
        }

        try (MetadataStore metadataStore = new DefaultMetadataStore(client, getSessionFactory(), config)) {
            metadataStore.start();
            Awaitility.await().with().atMost(10, TimeUnit.SECONDS).pollInterval(100, TimeUnit.MILLISECONDS)
                .until(metadataStore::isLeader);
            metadataStore.markMessageQueueAssignable(2, 1);

            List<QueueAssignment> assignments = metadataStore.listAssignments(2L, null, null, null).get();
            for (QueueAssignment assignment : assignments) {
                if (assignment.getQueueId() == 1) {
                    Assertions.assertEquals(AssignmentStatus.ASSIGNMENT_STATUS_ASSIGNED, assignment.getStatus());
                }
            }
        }
    }

    @Test
    public void testOpenStream_WithCloseStream_AtStart() throws IOException, ExecutionException,
        InterruptedException {
        long streamEpoch, streamId;
        try (SqlSession session = this.getSessionFactory().openSession()) {
            StreamMapper streamMapper = session.getMapper(StreamMapper.class);
            Stream stream = new Stream();
            stream.setRangeId(-1);
            stream.setSrcNodeId(1);
            stream.setDstNodeId(1);
            stream.setTopicId(1L);
            stream.setQueueId(2);
            stream.setState(StreamState.UNINITIALIZED);
            stream.setStreamRole(StreamRole.STREAM_ROLE_DATA);
            streamMapper.create(stream);
            streamId = stream.getId();
            streamEpoch = stream.getEpoch();

            session.commit();
        }
        long targetStreamEpoch = streamEpoch + 1;

        try (DefaultMetadataStore metadataStore = new DefaultMetadataStore(client, getSessionFactory(), config)) {
            Assertions.assertNull(metadataStore.getLease());
            Lease lease = new Lease();
            lease.setNodeId(config.nodeId());
            metadataStore.setLease(lease);
            metadataStore.setRole(Role.Leader);

            StreamMetadata metadata = metadataStore.openStream(streamId, streamEpoch, config.nodeId()).get();
            Assertions.assertNotNull(metadata);
            Assertions.assertEquals(streamId, metadata.getStreamId());
            Assertions.assertEquals(0, metadata.getStartOffset());
            Assertions.assertEquals(0, metadata.getEndOffset());
            Assertions.assertEquals(targetStreamEpoch, metadata.getEpoch());
            Assertions.assertEquals(0, metadata.getRangeId());
            Assertions.assertEquals(StreamState.OPEN, metadata.getState());

            metadataStore.closeStream(metadata.getStreamId(), metadata.getEpoch(), config.nodeId());
        }

        try (SqlSession session = getSessionFactory().openSession()) {
            StreamMapper streamMapper = session.getMapper(StreamMapper.class);
            RangeMapper rangeMapper = session.getMapper(RangeMapper.class);

            Stream stream = streamMapper.getByStreamId(streamId);
            Assertions.assertEquals(streamId, stream.getId());
            Assertions.assertEquals(0, stream.getStartOffset());
            Assertions.assertEquals(targetStreamEpoch, stream.getEpoch());
            Assertions.assertEquals(0, stream.getRangeId());
            Assertions.assertEquals(StreamState.CLOSED, stream.getState());

            Range range = rangeMapper.get(stream.getRangeId(), streamId, null);
            Assertions.assertEquals(0, range.getRangeId());
            Assertions.assertEquals(streamId, range.getStreamId());
            Assertions.assertEquals(targetStreamEpoch, range.getEpoch());
            Assertions.assertEquals(0, range.getStartOffset());
            Assertions.assertEquals(0, range.getEndOffset());

            streamMapper.delete(streamId);
            session.commit();
        }

    }

    @Test
    public void testOpenStream_WithClosedStream() throws IOException, ExecutionException,
        InterruptedException {
        long streamId, streamEpoch = 1;
        try (SqlSession session = this.getSessionFactory().openSession()) {
            StreamMapper streamMapper = session.getMapper(StreamMapper.class);
            RangeMapper rangeMapper = session.getMapper(RangeMapper.class);

            Stream stream = new Stream();
            stream.setRangeId(0);
            stream.setState(StreamState.CLOSED);
            stream.setStreamRole(StreamRole.STREAM_ROLE_DATA);
            stream.setEpoch(streamEpoch);
            stream.setSrcNodeId(1);
            stream.setDstNodeId(1);
            stream.setTopicId(1L);
            stream.setQueueId(2);
            stream.setStartOffset(1234L);
            streamMapper.create(stream);
            streamId = stream.getId();

            Range range = new Range();
            range.setRangeId(0);
            range.setStreamId(streamId);
            range.setEpoch(streamEpoch);
            range.setStartOffset(1234L);
            range.setEndOffset(2345L);
            range.setNodeId(1);
            rangeMapper.create(range);
            session.commit();
        }
        long targetStreamEpoch = streamEpoch + 1;

        try (DefaultMetadataStore metadataStore = new DefaultMetadataStore(client, getSessionFactory(), config)) {
            Assertions.assertNull(metadataStore.getLease());
            Lease lease = new Lease();
            lease.setNodeId(config.nodeId());
            metadataStore.setLease(lease);
            metadataStore.setRole(Role.Leader);

            StreamMetadata metadata = metadataStore.openStream(streamId, streamEpoch, config.nodeId()).get();
            Assertions.assertNotNull(metadata);
            Assertions.assertEquals(streamId, metadata.getStreamId());
            Assertions.assertEquals(1234, metadata.getStartOffset());
            Assertions.assertEquals(targetStreamEpoch, metadata.getEpoch());
            Assertions.assertEquals(1, metadata.getRangeId());
            Assertions.assertEquals(2345, metadata.getEndOffset());
            Assertions.assertEquals(StreamState.OPEN, metadata.getState());

            metadataStore.closeStream(metadata.getStreamId(), metadata.getEpoch(), config.nodeId());
        }

        try (SqlSession session = getSessionFactory().openSession()) {
            StreamMapper streamMapper = session.getMapper(StreamMapper.class);
            RangeMapper rangeMapper = session.getMapper(RangeMapper.class);

            Stream stream = streamMapper.getByStreamId(streamId);
            Assertions.assertEquals(streamId, stream.getId());
            Assertions.assertEquals(1234, stream.getStartOffset());
            Assertions.assertEquals(targetStreamEpoch, stream.getEpoch());
            Assertions.assertEquals(1, stream.getRangeId());
            Assertions.assertEquals(StreamState.CLOSED, stream.getState());

            Range range = rangeMapper.get(stream.getRangeId(), streamId, null);
            Assertions.assertEquals(1, range.getRangeId());
            Assertions.assertEquals(streamId, range.getStreamId());
            Assertions.assertEquals(targetStreamEpoch, range.getEpoch());
            Assertions.assertEquals(2345, range.getStartOffset());
            Assertions.assertEquals(2345, range.getEndOffset());

            streamMapper.delete(streamId);
            session.commit();
        }
    }

    @Test
    public void testOpenCloseStream_duplicateSome() throws IOException {
        long streamId, streamEpoch = 1;
        try (SqlSession session = this.getSessionFactory().openSession()) {
            StreamMapper streamMapper = session.getMapper(StreamMapper.class);
            RangeMapper rangeMapper = session.getMapper(RangeMapper.class);

            Stream stream = new Stream();
            stream.setRangeId(0);
            stream.setState(StreamState.CLOSED);
            stream.setStreamRole(StreamRole.STREAM_ROLE_DATA);
            stream.setEpoch(streamEpoch);
            stream.setSrcNodeId(1);
            stream.setDstNodeId(1);
            stream.setStartOffset(1234L);
            stream.setTopicId(1L);
            stream.setQueueId(2);
            streamMapper.create(stream);
            streamId = stream.getId();

            Range range = new Range();
            range.setRangeId(0);
            range.setStreamId(streamId);
            range.setEpoch(streamEpoch);
            range.setStartOffset(1234L);
            range.setEndOffset(2345L);
            range.setNodeId(1);
            rangeMapper.create(range);
            session.commit();
        }

        long targetStreamEpoch = streamEpoch + 1;

        try (DefaultMetadataStore metadataStore = new DefaultMetadataStore(client, getSessionFactory(), config)) {
            Assertions.assertNull(metadataStore.getLease());
            Lease lease = new Lease();
            lease.setNodeId(config.nodeId());
            metadataStore.setLease(lease);
            metadataStore.setRole(Role.Leader);

            metadataStore.start();
            Awaitility.await().with().atMost(10, TimeUnit.SECONDS)
                .pollInterval(100, TimeUnit.MILLISECONDS)
                .until(metadataStore::isLeader);

            String name = "broker-0";
            String address = "localhost:1234";
            String instanceId = "i-register";
            Node node = new Node();
            node.setName(name);
            node.setAddress(address);
            node.setInstanceId(instanceId);
            node.setId(1);
            metadataStore.addBrokerNode(node);

            StreamMetadata metadata = metadataStore.openStream(streamId, streamEpoch, config.nodeId()).join();
            Assertions.assertNotNull(metadata);
            Assertions.assertEquals(streamId, metadata.getStreamId());
            Assertions.assertEquals(1234, metadata.getStartOffset());
            Assertions.assertEquals(targetStreamEpoch, metadata.getEpoch());
            Assertions.assertEquals(1, metadata.getRangeId());
            Assertions.assertEquals(2345, metadata.getEndOffset());
            Assertions.assertEquals(StreamState.OPEN, metadata.getState());

            Assertions.assertThrows(CompletionException.class, () -> metadataStore.openStream(streamId, streamEpoch, config.nodeId()).join());
            Assertions.assertDoesNotThrow(() -> metadataStore.openStream(streamId, streamEpoch + 1, config.nodeId()).join());

            metadataStore.closeStream(metadata.getStreamId(), metadata.getEpoch(), config.nodeId()).join();
        }

        try (SqlSession session = getSessionFactory().openSession()) {
            StreamMapper streamMapper = session.getMapper(StreamMapper.class);
            RangeMapper rangeMapper = session.getMapper(RangeMapper.class);

            Stream stream = streamMapper.getByStreamId(streamId);
            Assertions.assertEquals(streamId, stream.getId());
            Assertions.assertEquals(1234, stream.getStartOffset());
            Assertions.assertEquals(targetStreamEpoch, stream.getEpoch());
            Assertions.assertEquals(1, stream.getRangeId());
            Assertions.assertEquals(StreamState.CLOSED, stream.getState());

            Range range = rangeMapper.get(stream.getRangeId(), streamId, null);
            Assertions.assertEquals(1, range.getRangeId());
            Assertions.assertEquals(streamId, range.getStreamId());
            Assertions.assertEquals(targetStreamEpoch, range.getEpoch());
            Assertions.assertEquals(2345, range.getStartOffset());
            Assertions.assertEquals(2345, range.getEndOffset());

            streamMapper.delete(streamId);
            session.commit();
        }
    }

    @Test
    public void testOpenCloseStream_duplicateDifferent() throws IOException {
        long streamId, streamEpoch = 1;
        int nodeId = 1;
        try (SqlSession session = this.getSessionFactory().openSession()) {
            StreamMapper streamMapper = session.getMapper(StreamMapper.class);
            RangeMapper rangeMapper = session.getMapper(RangeMapper.class);

            Stream stream = new Stream();
            stream.setRangeId(0);
            stream.setState(StreamState.CLOSED);
            stream.setStreamRole(StreamRole.STREAM_ROLE_DATA);
            stream.setEpoch(streamEpoch);
            stream.setSrcNodeId(nodeId);
            stream.setDstNodeId(nodeId);
            stream.setStartOffset(1234L);
            stream.setTopicId(1L);
            stream.setQueueId(2);
            streamMapper.create(stream);
            streamId = stream.getId();

            Range range = new Range();
            range.setRangeId(0);
            range.setStreamId(streamId);
            range.setEpoch(streamEpoch);
            range.setStartOffset(1234L);
            range.setEndOffset(2345L);
            range.setNodeId(nodeId);
            rangeMapper.create(range);
            session.commit();
        }

        long targetStreamEpoch = streamEpoch + 1;

        try (DefaultMetadataStore metadataStore = new DefaultMetadataStore(client, getSessionFactory(), config)) {
            Assertions.assertNull(metadataStore.getLease());
            Lease lease = new Lease();
            lease.setNodeId(config.nodeId());
            metadataStore.setLease(lease);
            metadataStore.setRole(Role.Leader);

            metadataStore.start();
            Awaitility.await().with().atMost(10, TimeUnit.SECONDS)
                .pollInterval(100, TimeUnit.MILLISECONDS)
                .until(metadataStore::isLeader);

            String name = "broker-0";
            String address = "localhost:1234";
            String instanceId = "i-register";
            Node node = new Node();
            node.setName(name);
            node.setAddress(address);
            node.setInstanceId(instanceId);
            node.setId(1);
            metadataStore.addBrokerNode(node);

            // Should throw if node is wrong
            Assertions.assertThrows(CompletionException.class, () -> metadataStore.openStream(streamId, streamEpoch, nodeId + 1).join());

            StreamMetadata metadata = metadataStore.openStream(streamId, streamEpoch, config.nodeId()).join();
            Assertions.assertNotNull(metadata);
            Assertions.assertEquals(streamId, metadata.getStreamId());
            Assertions.assertEquals(1234, metadata.getStartOffset());
            Assertions.assertEquals(targetStreamEpoch, metadata.getEpoch());
            Assertions.assertEquals(1, metadata.getRangeId());
            Assertions.assertEquals(2345, metadata.getEndOffset());
            Assertions.assertEquals(StreamState.OPEN, metadata.getState());

            metadataStore.closeStream(metadata.getStreamId(), metadata.getEpoch(), config.nodeId()).join();
        }

        try (SqlSession session = getSessionFactory().openSession()) {
            StreamMapper streamMapper = session.getMapper(StreamMapper.class);
            RangeMapper rangeMapper = session.getMapper(RangeMapper.class);

            Stream stream = streamMapper.getByStreamId(streamId);
            Assertions.assertEquals(streamId, stream.getId());
            Assertions.assertEquals(1234, stream.getStartOffset());
            Assertions.assertEquals(targetStreamEpoch, stream.getEpoch());
            Assertions.assertEquals(1, stream.getRangeId());
            Assertions.assertEquals(StreamState.CLOSED, stream.getState());

            Range range = rangeMapper.get(stream.getRangeId(), streamId, null);
            Assertions.assertEquals(1, range.getRangeId());
            Assertions.assertEquals(streamId, range.getStreamId());
            Assertions.assertEquals(targetStreamEpoch, range.getEpoch());
            Assertions.assertEquals(2345, range.getStartOffset());
            Assertions.assertEquals(2345, range.getEndOffset());

            streamMapper.delete(streamId);
            session.commit();
        }
    }

    @Test
    public void testOpenCloseStream_switch() throws IOException {
        long streamId, streamEpoch = 1;
        try (SqlSession session = this.getSessionFactory().openSession()) {
            StreamMapper streamMapper = session.getMapper(StreamMapper.class);
            RangeMapper rangeMapper = session.getMapper(RangeMapper.class);

            Stream stream = new Stream();
            stream.setRangeId(0);
            stream.setState(StreamState.OPEN);
            stream.setStreamRole(StreamRole.STREAM_ROLE_DATA);
            stream.setEpoch(streamEpoch);
            stream.setSrcNodeId(1);
            stream.setDstNodeId(1);
            stream.setStartOffset(1234L);
            stream.setTopicId(1L);
            stream.setQueueId(2);
            streamMapper.create(stream);
            streamId = stream.getId();

            Range range = new Range();
            range.setRangeId(0);
            range.setStreamId(streamId);
            range.setEpoch(streamEpoch);
            range.setStartOffset(1234L);
            range.setEndOffset(2345L);
            range.setNodeId(1);
            rangeMapper.create(range);
            session.commit();
        }

        long targetStreamEpoch = streamEpoch + 1;

        try (DefaultMetadataStore metadataStore = new DefaultMetadataStore(client, getSessionFactory(), config)) {
            Assertions.assertNull(metadataStore.getLease());
            Lease lease = new Lease();
            lease.setNodeId(config.nodeId());
            metadataStore.setLease(lease);
            metadataStore.setRole(Role.Leader);

            metadataStore.start();
            Awaitility.await().with().atMost(10, TimeUnit.SECONDS)
                .pollInterval(100, TimeUnit.MILLISECONDS)
                .until(metadataStore::isLeader);

            String name = "broker-0";
            String address = "localhost:1234";
            String instanceId = "i-register";
            Node node = new Node();
            node.setName(name);
            node.setAddress(address);
            node.setInstanceId(instanceId);
            node.setId(1);
            metadataStore.addBrokerNode(node);

            metadataStore.closeStream(streamId, streamEpoch, config.nodeId()).join();
            metadataStore.closeStream(streamId, streamEpoch, config.nodeId()).join();

            try (SqlSession session = getSessionFactory().openSession()) {
                StreamMapper streamMapper = session.getMapper(StreamMapper.class);
                RangeMapper rangeMapper = session.getMapper(RangeMapper.class);

                Stream stream = streamMapper.getByStreamId(streamId);
                Assertions.assertEquals(streamId, stream.getId());
                Assertions.assertEquals(1234, stream.getStartOffset());
                Assertions.assertEquals(streamEpoch, stream.getEpoch());
                Assertions.assertEquals(0, stream.getRangeId());
                Assertions.assertEquals(StreamState.CLOSED, stream.getState());

                Range range = rangeMapper.get(stream.getRangeId(), streamId, null);
                Assertions.assertEquals(0, range.getRangeId());
                Assertions.assertEquals(streamId, range.getStreamId());
                Assertions.assertEquals(streamEpoch, range.getEpoch());
                Assertions.assertEquals(1234, range.getStartOffset());
                Assertions.assertEquals(2345, range.getEndOffset());

                session.commit();
            }

            StreamMetadata metadata = metadataStore.openStream(streamId, streamEpoch, config.nodeId()).join();
            Assertions.assertNotNull(metadata);
            Assertions.assertEquals(streamId, metadata.getStreamId());
            Assertions.assertEquals(1234, metadata.getStartOffset());
            Assertions.assertEquals(targetStreamEpoch, metadata.getEpoch());
            Assertions.assertEquals(1, metadata.getRangeId());
            Assertions.assertEquals(2345, metadata.getEndOffset());
            Assertions.assertEquals(StreamState.OPEN, metadata.getState());
        }

        try (SqlSession session = getSessionFactory().openSession()) {
            StreamMapper streamMapper = session.getMapper(StreamMapper.class);
            streamMapper.delete(streamId);
            session.commit();
        }
    }

    @Test
    public void testGetStream() throws IOException, ExecutionException, InterruptedException {
        long dataStreamId;
        long opsStreamId;
        long retryStreamId;
        try (SqlSession session = this.getSessionFactory().openSession()) {
            StreamMapper streamMapper = session.getMapper(StreamMapper.class);
            Stream stream = new Stream();
            stream.setTopicId(1L);
            stream.setQueueId(2);
            stream.setRangeId(0);
            stream.setState(StreamState.UNINITIALIZED);
            stream.setStreamRole(StreamRole.STREAM_ROLE_DATA);
            stream.setStartOffset(1234L);
            streamMapper.create(stream);
            dataStreamId = stream.getId();

            stream.setStreamRole(StreamRole.STREAM_ROLE_OPS);
            stream.setStartOffset(1234L);
            streamMapper.create(stream);
            opsStreamId = stream.getId();

            stream.setStreamRole(StreamRole.STREAM_ROLE_RETRY);
            stream.setGroupId(3L);
            stream.setStartOffset(1234L);
            streamMapper.create(stream);
            retryStreamId = stream.getId();
            session.commit();
        }

        try (DefaultMetadataStore metadataStore = new DefaultMetadataStore(client, getSessionFactory(), config)) {
            long streamId = metadataStore.getStream(1, 2, null, StreamRole.STREAM_ROLE_DATA)
                .get().getStreamId();
            Assertions.assertEquals(streamId, dataStreamId);
            streamId = metadataStore.getStream(1, 2, null, StreamRole.STREAM_ROLE_OPS).get().getStreamId();
            Assertions.assertEquals(streamId, opsStreamId);

            streamId = metadataStore.getStream(1, 2, 3L, StreamRole.STREAM_ROLE_RETRY).get().getStreamId();
            Assertions.assertEquals(streamId, retryStreamId);
        }
    }

    @Test
    public void testGetGroup() throws IOException, ExecutionException, InterruptedException {
        long groupId;
        try (SqlSession session = this.getSessionFactory().openSession()) {
            GroupMapper groupMapper = session.getMapper(GroupMapper.class);
            Group group = new Group();
            group.setGroupType(GroupType.GROUP_TYPE_STANDARD);
            group.setMaxDeliveryAttempt(5);
            group.setDeadLetterTopicId(1L);
            group.setStatus(GroupStatus.GROUP_STATUS_ACTIVE);
            group.setName("G1");
            groupMapper.create(group);
            groupId = group.getId();
            session.commit();
        }

        try (MetadataStore metadataStore = new DefaultMetadataStore(client, getSessionFactory(), config)) {
            ConsumerGroup got = metadataStore.describeGroup(groupId, null).get();
            Assertions.assertEquals(5, got.getMaxDeliveryAttempt());
            Assertions.assertEquals(GroupType.GROUP_TYPE_STANDARD, got.getGroupType());
            Assertions.assertEquals(1L, got.getDeadLetterTopicId());
            Assertions.assertEquals("G1", got.getName());
        }
    }

    @Test
    public void testListOpenStreams() throws IOException, ExecutionException, InterruptedException {
        try (SqlSession session = getSessionFactory().openSession()) {
            StreamMapper streamMapper = session.getMapper(StreamMapper.class);

            Stream stream = new Stream();
            stream.setEpoch(1L);
            stream.setTopicId(2L);
            stream.setQueueId(3);
            stream.setDstNodeId(4);
            stream.setSrcNodeId(5);
            stream.setState(StreamState.OPEN);
            stream.setStartOffset(6L);
            stream.setRangeId(7);
            stream.setStreamRole(StreamRole.STREAM_ROLE_DATA);
            streamMapper.create(stream);
            session.commit();
        }

        try (DefaultMetadataStore metadataStore = new DefaultMetadataStore(null, getSessionFactory(), config)) {
            metadataStore.start();
            Awaitility.await().with().atMost(3, TimeUnit.SECONDS).pollInterval(100, TimeUnit.MILLISECONDS)
                .until(metadataStore::isLeader);
            List<StreamMetadata> metadataList = metadataStore.listOpenStreams(4).get();
            Assertions.assertEquals(1, metadataList.size());
        }
    }

    @Test
    public void testListOpenStream() throws IOException, ExecutionException, InterruptedException {
        long streamId;
        int nodeId = 1, rangId = 0;
        try (SqlSession session = this.getSessionFactory().openSession()) {
            StreamMapper streamMapper = session.getMapper(StreamMapper.class);
            RangeMapper rangeMapper = session.getMapper(RangeMapper.class);

            com.automq.rocketmq.controller.metadata.database.dao.Stream stream = new com.automq.rocketmq.controller.metadata.database.dao.Stream();
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

        try (DefaultMetadataStore metadataStore = new DefaultMetadataStore(client, getSessionFactory(), config)) {
            Assertions.assertNull(metadataStore.getLease());
            Lease lease = new Lease();
            lease.setNodeId(config.nodeId());
            metadataStore.setLease(lease);
            metadataStore.setRole(Role.Leader);
            List<StreamMetadata> streams = metadataStore.listOpenStreams(nodeId).get();

            Assertions.assertFalse(streams.isEmpty());
            StreamMetadata streamMetadata = streams.get(0);
            Assertions.assertEquals(streamId, streamMetadata.getStreamId());
            Assertions.assertEquals(StreamState.OPEN, streamMetadata.getState());
            Assertions.assertEquals(1234, streamMetadata.getStartOffset());
            Assertions.assertEquals(2345, streamMetadata.getEndOffset());
        }

        try (SqlSession session = getSessionFactory().openSession()) {
            StreamMapper streamMapper = session.getMapper(StreamMapper.class);
            streamMapper.delete(streamId);
            session.commit();
        }

    }

    @Test
    public void testListOpenStream_NotFound() throws IOException, ExecutionException, InterruptedException {
        long streamId;
        int nodeId = 1, rangId = 0;
        try (SqlSession session = this.getSessionFactory().openSession()) {
            StreamMapper streamMapper = session.getMapper(StreamMapper.class);
            RangeMapper rangeMapper = session.getMapper(RangeMapper.class);

            com.automq.rocketmq.controller.metadata.database.dao.Stream stream = new com.automq.rocketmq.controller.metadata.database.dao.Stream();
            stream.setSrcNodeId(nodeId);
            stream.setDstNodeId(nodeId);
            stream.setStartOffset(1234L);
            stream.setEpoch(0L);
            stream.setRangeId(rangId);
            stream.setTopicId(1L);
            stream.setQueueId(2);
            stream.setState(StreamState.CLOSED);
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

        try (DefaultMetadataStore metadataStore = new DefaultMetadataStore(client, getSessionFactory(), config)) {
            Assertions.assertNull(metadataStore.getLease());
            Lease lease = new Lease();
            lease.setNodeId(config.nodeId());
            metadataStore.setLease(lease);
            metadataStore.setRole(Role.Leader);
            List<StreamMetadata> streams = metadataStore.listOpenStreams(nodeId).get();

            Assertions.assertTrue(streams.isEmpty());
        }

        try (SqlSession session = getSessionFactory().openSession()) {
            StreamMapper streamMapper = session.getMapper(StreamMapper.class);
            streamMapper.delete(streamId);
            session.commit();
        }
    }

    @Test
    public void testConsumerOffset() throws IOException, ExecutionException, InterruptedException {
        long groupId = 2, topicId = 1;
        int queueId = 4;
        try (MetadataStore metadataStore = new DefaultMetadataStore(client, getSessionFactory(), config)) {
            metadataStore.start();
            Awaitility.await().with().atMost(10, TimeUnit.SECONDS).pollInterval(100, TimeUnit.MILLISECONDS)
                .until(metadataStore::isLeader);
            metadataStore.commitOffset(groupId, topicId, queueId, 1000);

            Long offset = metadataStore.getConsumerOffset(groupId, topicId, queueId).get();
            Assertions.assertEquals(1000, offset);

            metadataStore.commitOffset(groupId, topicId, queueId, 2000);

            offset = metadataStore.getConsumerOffset(groupId, topicId, queueId).get();
            Assertions.assertEquals(2000, offset);
        }
    }

    @Test
    public void testGetStreams() throws IOException, ExecutionException, InterruptedException {
        int nodeId = 1;
        int count = 5;
        List<Long> streamIds = new ArrayList<>();
        try (SqlSession session = this.getSessionFactory().openSession()) {
            StreamMapper streamMapper = session.getMapper(StreamMapper.class);
            RangeMapper rangeMapper = session.getMapper(RangeMapper.class);
            long startOffset = 1234;
            for (int i = 0; i < count; i++) {
                com.automq.rocketmq.controller.metadata.database.dao.Stream stream = new com.automq.rocketmq.controller.metadata.database.dao.Stream();
                stream.setSrcNodeId(nodeId);
                stream.setDstNodeId(nodeId);
                stream.setStartOffset(startOffset);
                stream.setEpoch((long) i);
                stream.setRangeId(i + 1);
                stream.setTopicId(1L);
                stream.setQueueId(2);
                stream.setState(StreamState.OPEN);
                stream.setStreamRole(StreamRole.STREAM_ROLE_DATA);
                streamMapper.create(stream);
                long streamId = stream.getId();
                streamIds.add(streamId);

                Range range = new Range();
                range.setRangeId(i + 1);
                range.setStreamId(streamId);
                range.setEpoch(0L);
                range.setStartOffset(startOffset);
                range.setEndOffset(startOffset + 100);
                range.setNodeId(nodeId);
                rangeMapper.create(range);

                startOffset += 100;
            }
            session.commit();
        }

        try (DefaultMetadataStore metadataStore = new DefaultMetadataStore(client, getSessionFactory(), config)) {
            Assertions.assertNull(metadataStore.getLease());
            Lease lease = new Lease();
            lease.setNodeId(config.nodeId());
            metadataStore.setLease(lease);
            metadataStore.setRole(Role.Leader);
            List<StreamMetadata> streams = metadataStore.getStreams(streamIds).get();

            Assertions.assertFalse(streams.isEmpty());
            Assertions.assertEquals(count, streams.size());
            long startOffset = 1234;
            for (int i = 0; i < count; i++) {
                StreamMetadata streamMetadata = streams.get(i);
                Assertions.assertEquals(startOffset, streamMetadata.getStartOffset());
                Assertions.assertEquals(startOffset + 100, streamMetadata.getEndOffset());
                startOffset += 100;
            }
        }

    }

    @Test
    public void testGetStreams_IsEmpty() throws IOException, ExecutionException, InterruptedException {
        List<Long> streamIds = new ArrayList<>();

        try (DefaultMetadataStore metadataStore = new DefaultMetadataStore(client, getSessionFactory(), config)) {
            Assertions.assertNull(metadataStore.getLease());
            Lease lease = new Lease();
            lease.setNodeId(config.nodeId());
            metadataStore.setLease(lease);
            metadataStore.setRole(Role.Leader);
            Assertions.assertTrue(metadataStore.getStreams(streamIds).get().isEmpty());
        }

    }

    @Test
    public void addressOfNode_None() throws IOException, ExecutionException, InterruptedException {
        try (MetadataStore metadataStore = new DefaultMetadataStore(client, getSessionFactory(), config)) {
            metadataStore.start();
            Awaitility.await().with().atMost(10, TimeUnit.SECONDS).pollInterval(100, TimeUnit.MILLISECONDS)
                .until(metadataStore::isLeader);
            String address = metadataStore.addressOfNode(1).get();
            Assertions.assertNull(address);
        }
    }

    @Test
    public void addressOfNode() throws IOException, ExecutionException, InterruptedException {
        String name = "broker-0";
        String address = "localhost:1234";
        String instanceId = "i-register";

        try (MetadataStore metadataStore = new DefaultMetadataStore(client, getSessionFactory(), config)) {
            metadataStore.start();
            Awaitility.await().with().atMost(10, TimeUnit.SECONDS).pollInterval(100, TimeUnit.MILLISECONDS)
                .until(metadataStore::isLeader);
            Node node = new Node();
            node.setName(name);
            node.setAddress(address);
            node.setInstanceId(instanceId);
            node.setId(1);

            metadataStore.addBrokerNode(node);
            String adr = metadataStore.addressOfNode(1).get();
            Assertions.assertEquals(address, adr);
        }
    }
}