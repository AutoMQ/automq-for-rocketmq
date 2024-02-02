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

package com.automq.rocketmq.controller.server.store;

import apache.rocketmq.common.v1.Code;
import apache.rocketmq.controller.v1.AcceptTypes;
import apache.rocketmq.controller.v1.AssignmentStatus;
import apache.rocketmq.controller.v1.ConsumerGroup;
import apache.rocketmq.controller.v1.CreateTopicRequest;
import apache.rocketmq.controller.v1.DescribeClusterRequest;
import apache.rocketmq.controller.v1.GroupStatus;
import apache.rocketmq.controller.v1.GroupType;
import apache.rocketmq.controller.v1.MessageType;
import apache.rocketmq.controller.v1.StreamMetadata;
import apache.rocketmq.controller.v1.StreamRole;
import apache.rocketmq.controller.v1.StreamState;
import apache.rocketmq.controller.v1.TopicStatus;
import apache.rocketmq.controller.v1.UpdateTopicRequest;
import com.automq.rocketmq.common.exception.ControllerException;
import com.automq.rocketmq.controller.ControllerClient;
import com.automq.rocketmq.controller.MetadataStore;
import com.automq.rocketmq.controller.store.DatabaseTestBase;
import com.automq.rocketmq.metadata.dao.Group;
import com.automq.rocketmq.metadata.dao.Node;
import com.automq.rocketmq.metadata.dao.QueueAssignment;
import com.automq.rocketmq.metadata.dao.Range;
import com.automq.rocketmq.metadata.dao.Stream;
import com.automq.rocketmq.metadata.dao.StreamCriteria;
import com.automq.rocketmq.metadata.dao.Topic;
import com.automq.rocketmq.metadata.mapper.GroupMapper;
import com.automq.rocketmq.metadata.mapper.NodeMapper;
import com.automq.rocketmq.metadata.mapper.QueueAssignmentMapper;
import com.automq.rocketmq.metadata.mapper.RangeMapper;
import com.automq.rocketmq.metadata.mapper.StreamMapper;
import com.automq.rocketmq.metadata.mapper.TopicMapper;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.ibatis.session.SqlSession;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

class DefaultMetadataStoreTest extends DatabaseTestBase {
    ControllerClient client;

    public DefaultMetadataStoreTest() {
        this.client = Mockito.mock(ControllerClient.class);
    }

    @Test
    public void testDescribeCluster() throws IOException {
        try (DefaultMetadataStore metadataStore = new DefaultMetadataStore(client, getSessionFactory(), config)) {
            metadataStore.start();
            awaitElectedAsLeader(metadataStore);
            assertDoesNotThrow(() -> {
                metadataStore.describeCluster(DescribeClusterRequest.newBuilder().build()).join();
            });
        }
    }

    @Test
    public void testRegisterCurrentNode() throws IOException {
        try (DefaultMetadataStore metadataStore = new DefaultMetadataStore(client, getSessionFactory(), config)) {
            metadataStore.start();
            awaitElectedAsLeader(metadataStore);
            assertDoesNotThrow(() -> metadataStore.registerCurrentNode(config.name(), config.advertiseAddress(), config.instanceId()));
        }
    }

    @Test
    public void testApplyStreamChange() throws IOException {
        try (DefaultMetadataStore metadataStore = new DefaultMetadataStore(client, getSessionFactory(), config)) {
            metadataStore.start();
            awaitElectedAsLeader(metadataStore);
            Stream stream = new Stream();
            stream.setStreamRole(StreamRole.STREAM_ROLE_RETRY);
            stream.setId(2L);
            stream.setState(StreamState.DELETED);
            stream.setRangeId(1);
            stream.setQueueId(3);
            stream.setTopicId(4L);
            stream.setEpoch(5L);
            stream.setSrcNodeId(6);
            stream.setDstNodeId(7);
            stream.setGroupId(8L);
            metadataStore.applyStreamChange(List.of(stream));
        }
    }

    @Test
    public void testHeartbeat() throws IOException {

        Mockito.when(client.heartbeat(ArgumentMatchers.anyString(), ArgumentMatchers.anyInt(), ArgumentMatchers.anyLong(), ArgumentMatchers.anyBoolean()))
            .thenReturn(CompletableFuture.completedFuture(null));

        ElectionService electionService = Mockito.mock(ElectionService.class);
        Mockito.when(electionService.leaderAddress()).thenReturn(Optional.of("localhost:1234"));
        Mockito.when(electionService.leaderNodeId()).thenReturn(Optional.of(2));
        try (DefaultMetadataStore metadataStore = new DefaultMetadataStore(client, getSessionFactory(), config)) {
            metadataStore.setElectionService(electionService);
            metadataStore.heartbeat();
        }
    }

    @Test
    void testRegisterNode() throws IOException, ExecutionException, InterruptedException {
        int nodeId;
        try (DefaultMetadataStore metadataStore = new DefaultMetadataStore(client, getSessionFactory(), config)) {
            metadataStore.start();
            Awaitility.await().with().atMost(10, TimeUnit.SECONDS)
                .pollInterval(100, TimeUnit.MILLISECONDS)
                .until(metadataStore::isLeader);

            Node node = metadataStore.registerBrokerNode(config.name(), config.advertiseAddress(), config.instanceId()).get();
            assertTrue(node.getId() > 0);
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
            assertThrows(CompletionException.class, () -> metadataStore.registerBrokerNode("", address, instanceId).join());
            assertThrows(CompletionException.class, () -> metadataStore.registerBrokerNode(name, null, instanceId).join());
            assertThrows(CompletionException.class, () -> metadataStore.registerBrokerNode(name, address, "").join());
        }
    }

    /**
     * Dummy test, should be removed later
     */
    @Test
    void testGetLease() throws IOException {
        try (MetadataStore metadataStore = new DefaultMetadataStore(client, getSessionFactory(), config)) {
            assertTrue(metadataStore.electionService().leaderNodeId().isEmpty());
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
    void testLeaderAddress() throws IOException {
        try (MetadataStore metadataStore = new DefaultMetadataStore(client, getSessionFactory(), config)) {
            metadataStore.start();
            Awaitility.await()
                .with()
                .atMost(10, TimeUnit.SECONDS)
                .pollInterval(100, TimeUnit.MILLISECONDS).until(metadataStore::isLeader);

            Optional<Integer> leaderNodeId = metadataStore.electionService().leaderNodeId();
            Optional<String> leaderAddress = metadataStore.electionService().leaderAddress();
            assertTrue(leaderNodeId.isPresent());
            assertTrue(leaderAddress.isPresent());
        }
    }

    @Test
    void testLeaderAddress_NoLeader() throws IOException {
        try (MetadataStore metadataStore = new DefaultMetadataStore(client, getSessionFactory(), config)) {
            assertTrue(metadataStore.leaderAddress().isEmpty());
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
            assertTrue(metadataStore.leaderAddress().isPresent());
        }
    }

    @Test
    void testCreateTopic() throws IOException, ExecutionException, InterruptedException {
        String address = "localhost:2345";
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
            topics.stream().filter(topic -> topic.getName().equals("t1")).forEach(topic -> assertEquals(4, topic.getQueueNum()));

            QueueAssignmentMapper assignmentMapper = session.getMapper(QueueAssignmentMapper.class);
            List<QueueAssignment> assignments = assignmentMapper.list(topicId, null, null, null, null);
            assertEquals(4, assignments.size());

            StreamMapper streamMapper = session.getMapper(StreamMapper.class);
            List<Stream> streams = streamMapper.byCriteria(StreamCriteria.newBuilder().withTopicId(topicId).build());
            // By default, we create 3 streams for each message queue: data, ops, snapshot
            assertEquals(queueNum * 3, streams.size());
        }
    }

    private static AcceptTypes decodeAcceptTypes(String json) throws InvalidProtocolBufferException {
        AcceptTypes.Builder builder = AcceptTypes.newBuilder();
        JsonFormat.parser().ignoringUnknownFields().merge(json, builder);
        return builder.build();
    }

    @Test
    void testUpdateTopic() throws IOException, ExecutionException, InterruptedException {
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
            topics.stream().filter(topic -> topic.getName().equals("t1")).forEach(topic -> assertEquals(4, topic.getQueueNum()));

            topics.stream().filter(topic -> topic.getName().equals("t1"))
                .forEach(topic -> {
                    try {
                        assertEquals(acceptTypes, decodeAcceptTypes(topic.getAcceptMessageTypes()));
                    } catch (InvalidProtocolBufferException e) {
                        fail(e);
                    }
                });

            QueueAssignmentMapper assignmentMapper = session.getMapper(QueueAssignmentMapper.class);
            List<QueueAssignment> assignments = assignmentMapper.list(topicId, null, null, null, null);
            assertEquals(4, assignments.size());

            StreamMapper streamMapper = session.getMapper(StreamMapper.class);
            List<Stream> streams = streamMapper.byCriteria(StreamCriteria.newBuilder().withTopicId(topicId).build());
            // By default, we create 3 streams for each message queue: data, ops, snapshot
            assertEquals(queueNum * 3, streams.size());
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
            assertEquals(1, affectedRows);
            session.commit();
        }

        try (MetadataStore metadataStore = new DefaultMetadataStore(client, getSessionFactory(), config)) {
            List<QueueAssignment> assignmentList = metadataStore.listAssignments(null, null, null, null).get();
            assertEquals(1, assignmentList.size());
            QueueAssignment assignment = assignmentList.get(0);
            assertEquals(1, assignment.getTopicId());
            assertEquals(AssignmentStatus.ASSIGNMENT_STATUS_ASSIGNED, assignment.getStatus());
            assertEquals(2, assignment.getDstNodeId());
            assertEquals(3, assignment.getSrcNodeId());
            assertEquals(4, assignment.getQueueId());
        }
    }

    @Test
    public void testDeleteTopic() throws IOException, ExecutionException, InterruptedException {
        long topicId;
        try (SqlSession session = getSessionFactory().openSession()) {
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

        try (MetadataStore metadataStore = new DefaultMetadataStore(client, getSessionFactory(), config)) {
            metadataStore.start();
            Awaitility.await().with().atMost(10, TimeUnit.SECONDS).pollInterval(100, TimeUnit.MILLISECONDS)
                .until(metadataStore::isLeader);

            metadataStore.deleteTopic(topicId).get();
        }

    }

    @Test
    public void testDeleteTopic_NotFound() throws IOException {
        try (MetadataStore metadataStore = new DefaultMetadataStore(client, getSessionFactory(), config)) {
            metadataStore.start();
            Awaitility.await().with().atMost(10, TimeUnit.SECONDS).pollInterval(100, TimeUnit.MILLISECONDS)
                .until(metadataStore::isLeader);
            assertThrows(ExecutionException.class, () -> metadataStore.deleteTopic(1).get());
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
            assertEquals("T1", topic.getName());
            assertEquals(1, topic.getAssignmentsCount());
            assertEquals(1, topic.getReassignmentsCount());
            assertEquals(messageType, JsonFormat.printer().print(topic.getAcceptTypes()));
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
                    assertEquals(AssignmentStatus.ASSIGNMENT_STATUS_ASSIGNED, assignment.getStatus());
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
            metadataStore.start();
            Awaitility.await().with().atMost(10, TimeUnit.SECONDS)
                .pollInterval(100, TimeUnit.MILLISECONDS)
                .until(metadataStore::isLeader);

            StreamMetadata metadata = metadataStore.openStream(streamId, streamEpoch, 1).get();
            assertNotNull(metadata);
            assertEquals(streamId, metadata.getStreamId());
            assertEquals(0, metadata.getStartOffset());
            assertEquals(0, metadata.getEndOffset());
            assertEquals(targetStreamEpoch, metadata.getEpoch());
            assertEquals(0, metadata.getRangeId());
            assertEquals(StreamState.OPEN, metadata.getState());

            metadataStore.closeStream(metadata.getStreamId(), metadata.getEpoch(), config.nodeId());
        }

        try (SqlSession session = getSessionFactory().openSession()) {
            StreamMapper streamMapper = session.getMapper(StreamMapper.class);
            RangeMapper rangeMapper = session.getMapper(RangeMapper.class);

            Stream stream = streamMapper.getByStreamId(streamId);
            assertEquals(streamId, stream.getId());
            assertEquals(0, stream.getStartOffset());
            assertEquals(targetStreamEpoch, stream.getEpoch());
            assertEquals(0, stream.getRangeId());
            assertEquals(StreamState.CLOSED, stream.getState());

            Range range = rangeMapper.get(stream.getRangeId(), streamId, null);
            assertEquals(0, range.getRangeId());
            assertEquals(streamId, range.getStreamId());
            assertEquals(targetStreamEpoch, range.getEpoch());
            assertEquals(0, range.getStartOffset());
            assertEquals(0, range.getEndOffset());

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
            metadataStore.start();
            Awaitility.await().with().atMost(10, TimeUnit.SECONDS)
                .pollInterval(100, TimeUnit.MILLISECONDS)
                .until(metadataStore::isLeader);

            StreamMetadata metadata = metadataStore.openStream(streamId, streamEpoch, 1).get();
            assertNotNull(metadata);
            assertEquals(streamId, metadata.getStreamId());
            assertEquals(1234, metadata.getStartOffset());
            assertEquals(targetStreamEpoch, metadata.getEpoch());
            assertEquals(1, metadata.getRangeId());
            assertEquals(2345, metadata.getEndOffset());
            assertEquals(StreamState.OPEN, metadata.getState());

            metadataStore.closeStream(metadata.getStreamId(), metadata.getEpoch(), 1);
        }

        try (SqlSession session = getSessionFactory().openSession()) {
            StreamMapper streamMapper = session.getMapper(StreamMapper.class);
            RangeMapper rangeMapper = session.getMapper(RangeMapper.class);

            Stream stream = streamMapper.getByStreamId(streamId);
            assertEquals(streamId, stream.getId());
            assertEquals(1234, stream.getStartOffset());
            assertEquals(targetStreamEpoch, stream.getEpoch());
            assertEquals(1, stream.getRangeId());
            assertEquals(StreamState.CLOSED, stream.getState());

            Range range = rangeMapper.get(stream.getRangeId(), streamId, null);
            assertEquals(1, range.getRangeId());
            assertEquals(streamId, range.getStreamId());
            assertEquals(targetStreamEpoch, range.getEpoch());
            assertEquals(2345, range.getStartOffset());
            assertEquals(2345, range.getEndOffset());

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
            metadataStore.start();
            Awaitility.await().with().atMost(10, TimeUnit.SECONDS)
                .pollInterval(100, TimeUnit.MILLISECONDS)
                .until(metadataStore::isLeader);

            metadataStore.start();
            Awaitility.await().with().atMost(10, TimeUnit.SECONDS)
                .pollInterval(100, TimeUnit.MILLISECONDS)
                .until(metadataStore::isLeader);

            StreamMetadata metadata = metadataStore.openStream(streamId, streamEpoch, 1).join();
            assertNotNull(metadata);
            assertEquals(streamId, metadata.getStreamId());
            assertEquals(1234, metadata.getStartOffset());
            assertEquals(targetStreamEpoch, metadata.getEpoch());
            assertEquals(1, metadata.getRangeId());
            assertEquals(2345, metadata.getEndOffset());
            assertEquals(StreamState.OPEN, metadata.getState());

            assertThrows(CompletionException.class, () -> metadataStore.openStream(streamId, streamEpoch, 1).join());
            assertDoesNotThrow(() -> metadataStore.openStream(streamId, streamEpoch + 1, 1).join());

            metadataStore.closeStream(metadata.getStreamId(), metadata.getEpoch(), 1).join();
        }

        try (SqlSession session = getSessionFactory().openSession()) {
            StreamMapper streamMapper = session.getMapper(StreamMapper.class);
            RangeMapper rangeMapper = session.getMapper(RangeMapper.class);

            Stream stream = streamMapper.getByStreamId(streamId);
            assertEquals(streamId, stream.getId());
            assertEquals(1234, stream.getStartOffset());
            assertEquals(targetStreamEpoch, stream.getEpoch());
            assertEquals(1, stream.getRangeId());
            assertEquals(StreamState.CLOSED, stream.getState());

            Range range = rangeMapper.get(stream.getRangeId(), streamId, null);
            assertEquals(1, range.getRangeId());
            assertEquals(streamId, range.getStreamId());
            assertEquals(targetStreamEpoch, range.getEpoch());
            assertEquals(2345, range.getStartOffset());
            assertEquals(2345, range.getEndOffset());

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
            assertThrows(CompletionException.class, () -> metadataStore.openStream(streamId, streamEpoch, nodeId + 1).join());

            StreamMetadata metadata = metadataStore.openStream(streamId, streamEpoch, nodeId).join();
            assertNotNull(metadata);
            assertEquals(streamId, metadata.getStreamId());
            assertEquals(1234, metadata.getStartOffset());
            assertEquals(targetStreamEpoch, metadata.getEpoch());
            assertEquals(1, metadata.getRangeId());
            assertEquals(2345, metadata.getEndOffset());
            assertEquals(StreamState.OPEN, metadata.getState());

            metadataStore.closeStream(metadata.getStreamId(), metadata.getEpoch(), nodeId).join();
        }

        try (SqlSession session = getSessionFactory().openSession()) {
            StreamMapper streamMapper = session.getMapper(StreamMapper.class);
            RangeMapper rangeMapper = session.getMapper(RangeMapper.class);

            Stream stream = streamMapper.getByStreamId(streamId);
            assertEquals(streamId, stream.getId());
            assertEquals(1234, stream.getStartOffset());
            assertEquals(targetStreamEpoch, stream.getEpoch());
            assertEquals(1, stream.getRangeId());
            assertEquals(StreamState.CLOSED, stream.getState());

            Range range = rangeMapper.get(stream.getRangeId(), streamId, null);
            assertEquals(1, range.getRangeId());
            assertEquals(streamId, range.getStreamId());
            assertEquals(targetStreamEpoch, range.getEpoch());
            assertEquals(2345, range.getStartOffset());
            assertEquals(2345, range.getEndOffset());

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

            metadataStore.closeStream(streamId, streamEpoch, 1).join();
            metadataStore.closeStream(streamId, streamEpoch, 1).join();

            try (SqlSession session = getSessionFactory().openSession()) {
                StreamMapper streamMapper = session.getMapper(StreamMapper.class);
                RangeMapper rangeMapper = session.getMapper(RangeMapper.class);

                Stream stream = streamMapper.getByStreamId(streamId);
                assertEquals(streamId, stream.getId());
                assertEquals(1234, stream.getStartOffset());
                assertEquals(streamEpoch, stream.getEpoch());
                assertEquals(0, stream.getRangeId());
                assertEquals(StreamState.CLOSED, stream.getState());

                Range range = rangeMapper.get(stream.getRangeId(), streamId, null);
                assertEquals(0, range.getRangeId());
                assertEquals(streamId, range.getStreamId());
                assertEquals(streamEpoch, range.getEpoch());
                assertEquals(1234, range.getStartOffset());
                assertEquals(2345, range.getEndOffset());

                session.commit();
            }

            StreamMetadata metadata = metadataStore.openStream(streamId, streamEpoch, 1).join();
            assertNotNull(metadata);
            assertEquals(streamId, metadata.getStreamId());
            assertEquals(1234, metadata.getStartOffset());
            assertEquals(targetStreamEpoch, metadata.getEpoch());
            assertEquals(1, metadata.getRangeId());
            assertEquals(2345, metadata.getEndOffset());
            assertEquals(StreamState.OPEN, metadata.getState());
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
            assertEquals(streamId, dataStreamId);
            streamId = metadataStore.getStream(1, 2, null, StreamRole.STREAM_ROLE_OPS).get().getStreamId();
            assertEquals(streamId, opsStreamId);

            streamId = metadataStore.getStream(1, 2, 3L, StreamRole.STREAM_ROLE_RETRY).get().getStreamId();
            assertEquals(streamId, retryStreamId);
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
            assertEquals(5, got.getMaxDeliveryAttempt());
            assertEquals(GroupType.GROUP_TYPE_STANDARD, got.getGroupType());
            assertEquals(1L, got.getDeadLetterTopicId());
            assertEquals("G1", got.getName());
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
            assertEquals(1, metadataList.size());
        }
    }

    @Test
    public void testListOpenStream() throws IOException, ExecutionException, InterruptedException {
        long streamId;
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

        try (DefaultMetadataStore metadataStore = new DefaultMetadataStore(client, getSessionFactory(), config)) {
            metadataStore.start();
            Awaitility.await().with().atMost(10, TimeUnit.SECONDS)
                .pollInterval(100, TimeUnit.MILLISECONDS)
                .until(metadataStore::isLeader);
            List<StreamMetadata> streams = metadataStore.listOpenStreams(nodeId).get();

            assertFalse(streams.isEmpty());
            StreamMetadata streamMetadata = streams.get(0);
            assertEquals(streamId, streamMetadata.getStreamId());
            assertEquals(StreamState.OPEN, streamMetadata.getState());
            assertEquals(1234, streamMetadata.getStartOffset());
            assertEquals(2345, streamMetadata.getEndOffset());
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

            Stream stream = new Stream();
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
            metadataStore.start();
            Awaitility.await().with().atMost(10, TimeUnit.SECONDS)
                .pollInterval(100, TimeUnit.MILLISECONDS)
                .until(metadataStore::isLeader);

            List<StreamMetadata> streams = metadataStore.listOpenStreams(nodeId).get();

            assertTrue(streams.isEmpty());
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
            assertEquals(1000, offset);

            metadataStore.commitOffset(groupId, topicId, queueId, 2000);

            offset = metadataStore.getConsumerOffset(groupId, topicId, queueId).get();
            assertEquals(2000, offset);
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
                Stream stream = new Stream();
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
            metadataStore.start();
            Awaitility.await().with().atMost(10, TimeUnit.SECONDS)
                .pollInterval(100, TimeUnit.MILLISECONDS)
                .until(metadataStore::isLeader);

            List<StreamMetadata> streams = metadataStore.getStreams(streamIds).get();

            assertFalse(streams.isEmpty());
            assertEquals(count, streams.size());
            long startOffset = 1234;
            for (int i = 0; i < count; i++) {
                StreamMetadata streamMetadata = streams.get(i);
                assertEquals(startOffset, streamMetadata.getStartOffset());
                assertEquals(startOffset + 100, streamMetadata.getEndOffset());
                startOffset += 100;
            }
        }

    }

    @Test
    public void testGetStreams_IsEmpty() throws IOException, ExecutionException, InterruptedException {
        List<Long> streamIds = new ArrayList<>();

        try (DefaultMetadataStore metadataStore = new DefaultMetadataStore(client, getSessionFactory(), config)) {
            metadataStore.start();
            Awaitility.await().with().atMost(10, TimeUnit.SECONDS)
                .pollInterval(100, TimeUnit.MILLISECONDS)
                .until(metadataStore::isLeader);

            assertTrue(metadataStore.getStreams(streamIds).get().isEmpty());
        }

    }

    @Test
    public void addressOfNode_None() throws IOException, ExecutionException, InterruptedException {
        try (MetadataStore metadataStore = new DefaultMetadataStore(client, getSessionFactory(), config)) {
            metadataStore.start();
            Awaitility.await().with().atMost(10, TimeUnit.SECONDS).pollInterval(100, TimeUnit.MILLISECONDS)
                .until(metadataStore::isLeader);
            String address = metadataStore.addressOfNode(1).get();
            assertNull(address);
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
            assertEquals(address, adr);
        }
    }

    @Test
    public void testOnQueueClose() throws IOException {
        long streamId;
        try (SqlSession session = getSessionFactory().openSession()) {
            QueueAssignmentMapper queueAssignmentMapper = session.getMapper(QueueAssignmentMapper.class);
            QueueAssignment assignment = new QueueAssignment();
            assignment.setStatus(AssignmentStatus.ASSIGNMENT_STATUS_YIELDING);
            assignment.setQueueId(1);
            assignment.setTopicId(2);
            assignment.setSrcNodeId(3);
            assignment.setDstNodeId(4);
            queueAssignmentMapper.create(assignment);

            StreamMapper streamMapper = session.getMapper(StreamMapper.class);
            Stream stream = new Stream();
            stream.setId(1L);
            stream.setQueueId(1);
            stream.setRangeId(2);
            stream.setSrcNodeId(3);
            stream.setDstNodeId(4);
            stream.setState(StreamState.CLOSING);
            stream.setStreamRole(StreamRole.STREAM_ROLE_DATA);
            stream.setTopicId(2L);
            streamMapper.create(stream);
            streamId = stream.getId();
            session.commit();
        }

        try (MetadataStore metadataStore = new DefaultMetadataStore(client, getSessionFactory(), config);
             SqlSession session = metadataStore.openSession()) {
            metadataStore.start();
            Awaitility.await().with().atMost(10, TimeUnit.SECONDS).pollInterval(100, TimeUnit.MILLISECONDS)
                .until(metadataStore::isLeader);

            metadataStore.onQueueClosed(2, 1).join();

            QueueAssignmentMapper assignmentMapper = session.getMapper(QueueAssignmentMapper.class);
            QueueAssignment assignment = assignmentMapper.get(2, 1);
            assertEquals(4, assignment.getDstNodeId());
            assertEquals(4, assignment.getDstNodeId());

            StreamMapper streamMapper = session.getMapper(StreamMapper.class);
            Stream stream = streamMapper.getByStreamId(streamId);
            assertEquals(4, stream.getDstNodeId());
            assertEquals(4, stream.getDstNodeId());
        }
    }

    @Test
    public void testOnQueueClose_Remote() throws IOException {
        Mockito.when(client.notifyQueueClose(ArgumentMatchers.anyString(), ArgumentMatchers.anyLong(),
            ArgumentMatchers.anyInt())).thenReturn(CompletableFuture.failedFuture(new ControllerException(Code.MOCK_FAILURE_VALUE, "Mock failure")));
        ElectionService electionService = Mockito.mock(ElectionService.class);
        Mockito.when(electionService.leaderAddress()).thenReturn(Optional.of("localhost:1234"));

        try (MetadataStore metadataStore = new DefaultMetadataStore(client, getSessionFactory(), config)) {
            MetadataStore spy = Mockito.spy(metadataStore);
            Mockito.when(spy.isLeader()).thenReturn(false);
            Mockito.doReturn(electionService).when(spy).electionService();
            Mockito.when(spy.onQueueClosed(ArgumentMatchers.anyLong(), ArgumentMatchers.anyInt())).thenCallRealMethod();
            assertThrows(CompletionException.class, () -> spy.onQueueClosed(2, 1).join());
        }
    }

    @Test
    public void testGetNode() throws IOException {
        try (MetadataStore metadataStore = new DefaultMetadataStore(client, getSessionFactory(), config)) {
            metadataStore.start();
            awaitElectedAsLeader(metadataStore);
            Optional<BrokerNode> brokerNode = metadataStore.getNode(config.nodeId());
            Assertions.assertTrue(brokerNode.isPresent());
            Assertions.assertTrue(metadataStore.leaderAddress().isPresent());

            try (SqlSession session = metadataStore.openSession()) {
                NodeMapper mapper = session.getMapper(NodeMapper.class);
                Node node = new Node();
                node.setName("n1");
                node.setAddress("localhost:2345");
                node.setInstanceId("i-2345");
                node.setVolumeId("v-2345");
                mapper.create(node);
                session.commit();
                Assertions.assertTrue(metadataStore.getNode(node.getId()).isPresent());
            }
        }
    }
}
