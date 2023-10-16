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

import apache.rocketmq.controller.v1.AssignmentStatus;
import apache.rocketmq.controller.v1.ConsumerGroup;
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
import com.automq.rocketmq.common.system.StreamConstants;
import com.automq.rocketmq.controller.exception.ControllerException;
import com.automq.rocketmq.controller.metadata.ControllerClient;
import com.automq.rocketmq.common.config.ControllerConfig;
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

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
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
        ControllerConfig config = Mockito.mock(ControllerConfig.class);
        Mockito.when(config.nodeId()).thenReturn(1);
        Mockito.when(config.scanIntervalInSecs()).thenReturn(1);
        Mockito.when(config.leaseLifeSpanInSecs()).thenReturn(1);
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
    void testCreateTopic() throws IOException, ControllerException, ExecutionException, InterruptedException {
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
        List<MessageType> messageTypes = new ArrayList<>() {
            {
                add(MessageType.NORMAL);
                add(MessageType.FIFO);
                add(MessageType.TRANSACTION);
            }
        };
        try (MetadataStore metadataStore = new DefaultMetadataStore(client, getSessionFactory(), config)) {
            metadataStore.start();
            Awaitility.await().with().atMost(10, TimeUnit.SECONDS)
                .pollInterval(100, TimeUnit.MILLISECONDS)
                .until(metadataStore::isLeader);

            Awaitility.await().with().atMost(10, TimeUnit.SECONDS)
                .pollInterval(100, TimeUnit.MILLISECONDS)
                .until(metadataStore::hasAliveBrokerNodes);

            topicId = metadataStore.createTopic(topicName, queueNum, messageTypes).get();
        }

        try (SqlSession session = getSessionFactory().openSession()) {
            TopicMapper topicMapper = session.getMapper(TopicMapper.class);
            List<Topic> topics = topicMapper.list(null, null);
            topics.stream().filter(topic -> topic.getName().equals("t1")).forEach(topic -> Assertions.assertEquals(4, topic.getQueueNum()));

            QueueAssignmentMapper assignmentMapper = session.getMapper(QueueAssignmentMapper.class);
            List<QueueAssignment> assignments = assignmentMapper.list(topicId, null, null, null, null);
            Assertions.assertEquals(4, assignments.size());

            StreamMapper streamMapper = session.getMapper(StreamMapper.class);
            List<Stream> streams = streamMapper.list(topicId, null, null);
            // By default, we create 3 streams for each message queue: data, ops, snapshot
            Assertions.assertEquals(queueNum * 3, streams.size());
        }
    }

    @Test
    void testUpdateTopic() throws IOException, ControllerException, ExecutionException, InterruptedException {
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
        List<MessageType> messageTypes = new ArrayList<>() {
            {
                add(MessageType.NORMAL);
                add(MessageType.FIFO);
                add(MessageType.TRANSACTION);
            }
        };
        Gson gson = new Gson();
        List<MessageType> updateMessageTypes = new ArrayList<>() {
            {
                add(MessageType.NORMAL);
                add(MessageType.TRANSACTION);
            }
        };

        try (MetadataStore metadataStore = new DefaultMetadataStore(client, getSessionFactory(), config)) {
            metadataStore.start();
            Awaitility.await().with().atMost(10, TimeUnit.SECONDS)
                .pollInterval(100, TimeUnit.MILLISECONDS)
                .until(metadataStore::isLeader);

            Awaitility.await().with().atMost(10, TimeUnit.SECONDS)
                .pollInterval(100, TimeUnit.MILLISECONDS)
                .until(metadataStore::hasAliveBrokerNodes);

            topicId = metadataStore.createTopic(topicName, queueNum, messageTypes).get();

            metadataStore.updateTopic(topicId, topicName, null, updateMessageTypes).get();
        }

        try (SqlSession session = getSessionFactory().openSession()) {
            TopicMapper topicMapper = session.getMapper(TopicMapper.class);
            List<Topic> topics = topicMapper.list(null, null);
            topics.stream().filter(topic -> topic.getName().equals("t1")).forEach(topic -> Assertions.assertEquals(4, topic.getQueueNum()));

            topics.stream().filter(topic -> topic.getName().equals("t1")).forEach(topic -> Assertions.assertEquals(gson.toJson(updateMessageTypes), topic.getAcceptMessageTypes()));

            QueueAssignmentMapper assignmentMapper = session.getMapper(QueueAssignmentMapper.class);
            List<QueueAssignment> assignments = assignmentMapper.list(topicId, null, null, null, null);
            Assertions.assertEquals(4, assignments.size());

            StreamMapper streamMapper = session.getMapper(StreamMapper.class);
            List<Stream> streams = streamMapper.list(topicId, null, null);
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
        String messageType = "[\"NORMAL\",\"DELAY\"]";
        Gson gson = new Gson();
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
            Assertions.assertEquals(messageType, gson.toJson(topic.getAcceptMessageTypesList()));
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
    public void testListStreamObjects() throws IOException, ExecutionException, InterruptedException {
        long streamId, startOffset, endOffset;
        startOffset = 2000L;
        endOffset = 2111L;
        int limit = 1;
        try (SqlSession session = getSessionFactory().openSession()) {
            S3StreamObjectMapper s3StreamObjectMapper = session.getMapper(S3StreamObjectMapper.class);

            com.automq.rocketmq.controller.metadata.database.dao.S3StreamObject s3StreamObject = new com.automq.rocketmq.controller.metadata.database.dao.S3StreamObject();
            s3StreamObject.setObjectId(11L);
            s3StreamObject.setObjectSize(123L);
            s3StreamObject.setStreamId(111L);
            s3StreamObject.setStartOffset(1234L);
            s3StreamObject.setEndOffset(2345L);
            s3StreamObject.setBaseDataTimestamp(System.currentTimeMillis());

            streamId = s3StreamObject.getStreamId();

            s3StreamObjectMapper.create(s3StreamObject);
            session.commit();
        }

        try (DefaultMetadataStore metadataStore = new DefaultMetadataStore(client, getSessionFactory(), config)) {
            List<S3StreamObject> s3StreamObjects = metadataStore.listStreamObjects(streamId, startOffset, endOffset, limit).get();
            S3StreamObject s3StreamObject = s3StreamObjects.get(0);
            Assertions.assertEquals(11, s3StreamObject.getObjectId());
            Assertions.assertEquals(123, s3StreamObject.getObjectSize());
            Assertions.assertEquals(111, s3StreamObject.getStreamId());
            Assertions.assertEquals(1234, s3StreamObject.getStartOffset());
            Assertions.assertEquals(2345, s3StreamObject.getEndOffset());
        }
    }

    @Test
    public void testListWALObjects_WithPrams() throws IOException, ExecutionException, InterruptedException {
        long streamId, startOffset, endOffset;
        streamId = 1234567890;
        startOffset = 0L;
        endOffset = 9L;
        int limit = 1;
        Gson gson = new Gson();
        String subStreamsJson = """
            {
              "1234567890": {
                "streamId_": 1234567890,
                "startOffset_": 0,
                "endOffset_": 10
              },
              "9876543210": {
                "streamId_": 9876543210,
                "startOffset_": 5,
                "endOffset_": 15
              },
              "5678901234": {
                "streamId_": 5678901234,
                "startOffset_": 2,
                "endOffset_": 8
              },
              "4321098765": {
                "streamId_": 4321098765,
                "startOffset_": 7,
                "endOffset_": 12
              }
            }""";
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

            S3WalObjectMapper s3WALObjectMapper = session.getMapper(S3WalObjectMapper.class);
            S3WalObject s3WalObject = new S3WalObject();
            s3WalObject.setObjectId(123L);
            s3WalObject.setNodeId(1);
            s3WalObject.setObjectSize(22L);
            s3WalObject.setSequenceId(999L);
            s3WalObject.setSubStreams(subStreamsJson);
            s3WalObject.setBaseDataTimestamp(System.currentTimeMillis());

            s3WALObjectMapper.create(s3WalObject);
            session.commit();
        }
        String expectSubStream = """
            {
              "1234567890": {
                "streamId_": 1234567890,
                "startOffset_": 0,
                "endOffset_": 10
              }}""";

        Map<Long, SubStream> subStreams = gson.fromJson(new String(expectSubStream.getBytes(StandardCharsets.UTF_8)),
            new TypeToken<Map<Long, SubStream>>() {
            }.getType());

        try (DefaultMetadataStore metadataStore = new DefaultMetadataStore(client, getSessionFactory(), config)) {
            Assertions.assertNull(metadataStore.getLease());
            Lease lease = new Lease();
            lease.setNodeId(config.nodeId());
            metadataStore.setLease(lease);
            List<S3WALObject> s3WALObjects = metadataStore.listWALObjects(streamId, startOffset, endOffset, limit).get();

            Assertions.assertFalse(s3WALObjects.isEmpty());
            S3WALObject s3WALObject = s3WALObjects.get(0);
            Assertions.assertEquals(123, s3WALObject.getObjectId());
            Assertions.assertEquals(22, s3WALObject.getObjectSize());
            Assertions.assertEquals(1, s3WALObject.getBrokerId());
            Assertions.assertEquals(999, s3WALObject.getSequenceId());
            Assertions.assertEquals(subStreams, s3WALObject.getSubStreamsMap());
        }

        try (SqlSession session = getSessionFactory().openSession()) {
            S3WalObjectMapper s3WALObjectMapper = session.getMapper(S3WalObjectMapper.class);
            s3WALObjectMapper.delete(123L, 1, null);
            session.commit();
        }
    }

    @Test
    public void testListWALObjects_NotParams() throws IOException, ExecutionException, InterruptedException {
        Gson gson = new Gson();
        String subStreamsJson = """
            {
              "1234567890": {
                "streamId_": 1234567890,
                "startOffset_": 0,
                "endOffset_": 10
              },
              "9876543210": {
                "streamId_": 9876543210,
                "startOffset_": 5,
                "endOffset_": 15
              },
              "5678901234": {
                "streamId_": 5678901234,
                "startOffset_": 2,
                "endOffset_": 8
              },
              "4321098765": {
                "streamId_": 4321098765,
                "startOffset_": 7,
                "endOffset_": 12
              }
            }""";
        try (SqlSession session = getSessionFactory().openSession()) {
            S3WalObjectMapper s3WALObjectMapper = session.getMapper(S3WalObjectMapper.class);
            S3WalObject s3WALObject = new S3WalObject();
            s3WALObject.setObjectId(123L);
            s3WALObject.setNodeId(1);
            s3WALObject.setObjectSize(22L);
            s3WALObject.setSequenceId(999L);
            s3WALObject.setSubStreams(subStreamsJson);

            s3WALObject.setBaseDataTimestamp(System.currentTimeMillis());

            s3WALObjectMapper.create(s3WALObject);
            session.commit();
        }
        Map<Long, SubStream> subStreams = gson.fromJson(new String(subStreamsJson.getBytes(StandardCharsets.UTF_8)), new TypeToken<Map<Long, SubStream>>() {
        }.getType());

        try (DefaultMetadataStore metadataStore = new DefaultMetadataStore(client, getSessionFactory(), config)) {
            Assertions.assertNull(metadataStore.getLease());
            Lease lease = new Lease();
            lease.setNodeId(config.nodeId());
            metadataStore.setLease(lease);
            List<S3WALObject> s3WALObjects = metadataStore.listWALObjects().get();

            Assertions.assertFalse(s3WALObjects.isEmpty());
            S3WALObject s3WALObject = s3WALObjects.get(0);
            Assertions.assertEquals(123, s3WALObject.getObjectId());
            Assertions.assertEquals(22, s3WALObject.getObjectSize());
            Assertions.assertEquals(1, s3WALObject.getBrokerId());
            Assertions.assertEquals(999, s3WALObject.getSequenceId());
            Assertions.assertEquals(subStreams, s3WALObject.getSubStreamsMap());
        }

        try (SqlSession session = getSessionFactory().openSession()) {
            S3WalObjectMapper s3WALObjectMapper = session.getMapper(S3WalObjectMapper.class);
            s3WALObjectMapper.delete(123L, 1, null);
            session.commit();
        }
    }

    @Test
    public void testListObjects_OnlyStream() throws IOException, ExecutionException, InterruptedException {
        long streamId, startOffset, endOffset;
        streamId = 1;
        startOffset = 0L;
        endOffset = 9L;
        int limit = 3;
        Gson gson = new Gson();
        String subStreamsJson = """
            {
              "1": {
                "streamId_": 1,
                "startOffset_": 10,
                "endOffset_": 20
              },
              "2": {
                "streamId_": 1,
                "startOffset_": 20,
                "endOffset_": 30
              },
              "3": {
                "streamId_": 1,
                "startOffset_": 30,
                "endOffset_": 40
              },
              "4": {
                "streamId_": 2,
                "startOffset_": 40,
                "endOffset_": 50
              }
            }""";

        try (SqlSession session = getSessionFactory().openSession()) {
            S3WalObjectMapper s3WALObjectMapper = session.getMapper(S3WalObjectMapper.class);
            S3WalObject s3WALObject = new S3WalObject();
            s3WALObject.setObjectId(123L);
            s3WALObject.setNodeId(1);
            s3WALObject.setObjectSize(22L);
            s3WALObject.setSequenceId(999L);
            s3WALObject.setSubStreams(subStreamsJson);

            s3WALObject.setBaseDataTimestamp(System.currentTimeMillis());

            s3WALObjectMapper.create(s3WALObject);

            S3StreamObjectMapper s3StreamObjectMapper = session.getMapper(S3StreamObjectMapper.class);

            com.automq.rocketmq.controller.metadata.database.dao.S3StreamObject s3StreamObject = new com.automq.rocketmq.controller.metadata.database.dao.S3StreamObject();
            s3StreamObject.setObjectId(122L);
            s3StreamObject.setObjectSize(123L);
            s3StreamObject.setStreamId(streamId);
            s3StreamObject.setStartOffset(0L);
            s3StreamObject.setEndOffset(10L);
            s3StreamObject.setBaseDataTimestamp(System.currentTimeMillis());

            s3StreamObjectMapper.create(s3StreamObject);

            session.commit();
        }
        String expectSubStream = """
            {
              "1": {
                "streamId_": 1,
                "startOffset_": 10,
                "endOffset_": 20
              }}""";
        Map<Long, SubStream> subStreams = gson.fromJson(new String(expectSubStream.getBytes(StandardCharsets.UTF_8)), new TypeToken<Map<Long, SubStream>>() {
        }.getType());
        ControllerConfig config = Mockito.mock(ControllerConfig.class);
        Mockito.when(config.nodeId()).thenReturn(1);
        Mockito.when(config.scanIntervalInSecs()).thenReturn(1);
        Mockito.when(config.leaseLifeSpanInSecs()).thenReturn(1);

        try (DefaultMetadataStore metadataStore = new DefaultMetadataStore(client, getSessionFactory(), config)) {
            Assertions.assertNull(metadataStore.getLease());
            Lease lease = new Lease();
            lease.setNodeId(config.nodeId());
            metadataStore.setLease(lease);
            Pair<List<S3StreamObject>, List<S3WALObject>> listPair = metadataStore.listObjects(streamId, startOffset, endOffset, limit).get();

            Assertions.assertFalse(listPair.getLeft().isEmpty());
            Assertions.assertTrue(listPair.getRight().isEmpty());
            S3StreamObject s3StreamObject = listPair.getLeft().get(0);
            Assertions.assertEquals(122, s3StreamObject.getObjectId());
            Assertions.assertEquals(123, s3StreamObject.getObjectSize());
            Assertions.assertEquals(streamId, s3StreamObject.getStreamId());
            Assertions.assertEquals(0, s3StreamObject.getStartOffset());
            Assertions.assertEquals(10, s3StreamObject.getEndOffset());

        }

        try (SqlSession session = getSessionFactory().openSession()) {
            S3StreamObjectMapper s3StreamObjectMapper = session.getMapper(S3StreamObjectMapper.class);
            s3StreamObjectMapper.delete(null, streamId, 122L);
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
        Gson gson = new Gson();
        String subStreamsJson = """
            {
              "1": {
                "streamId_": 1,
                "startOffset_": 10,
                "endOffset_": 20
              },
              "2": {
                "streamId_": 1,
                "startOffset_": 20,
                "endOffset_": 30
              },
              "3": {
                "streamId_": 1,
                "startOffset_": 30,
                "endOffset_": 40
              },
              "4": {
                "streamId_": 2,
                "startOffset_": 40,
                "endOffset_": 50
              }
            }""";

        try (SqlSession session = getSessionFactory().openSession()) {
            S3WalObjectMapper s3WALObjectMapper = session.getMapper(S3WalObjectMapper.class);
            S3WalObject s3WALObject = new S3WalObject();
            s3WALObject.setObjectId(123L);
            s3WALObject.setNodeId(1);
            s3WALObject.setObjectSize(22L);
            s3WALObject.setSequenceId(999L);
            s3WALObject.setSubStreams(subStreamsJson);

            s3WALObject.setBaseDataTimestamp(System.currentTimeMillis());

            s3WALObjectMapper.create(s3WALObject);

            S3StreamObjectMapper s3StreamObjectMapper = session.getMapper(S3StreamObjectMapper.class);

            com.automq.rocketmq.controller.metadata.database.dao.S3StreamObject s3StreamObject = new com.automq.rocketmq.controller.metadata.database.dao.S3StreamObject();
            s3StreamObject.setObjectId(122L);
            s3StreamObject.setObjectSize(123L);
            s3StreamObject.setStreamId(streamId);
            s3StreamObject.setStartOffset(0L);
            s3StreamObject.setEndOffset(10L);
            s3StreamObject.setBaseDataTimestamp(System.currentTimeMillis());

            s3StreamObjectMapper.create(s3StreamObject);

            session.commit();
        }
        String expectSubStream = """
            {
              "1": {
                "streamId_": 1,
                "startOffset_": 10,
                "endOffset_": 20
              }}""";
        Map<Long, SubStream> subStreams = gson.fromJson(new String(expectSubStream.getBytes(StandardCharsets.UTF_8)), new TypeToken<Map<Long, SubStream>>() {
        }.getType());

        try (DefaultMetadataStore metadataStore = new DefaultMetadataStore(client, getSessionFactory(), config)) {
            Assertions.assertNull(metadataStore.getLease());
            Lease lease = new Lease();
            lease.setNodeId(config.nodeId());
            metadataStore.setLease(lease);
            Pair<List<S3StreamObject>, List<S3WALObject>> listPair = metadataStore.listObjects(streamId, startOffset, endOffset, limit).get();

            Assertions.assertTrue(listPair.getLeft().isEmpty());
            Assertions.assertFalse(listPair.getRight().isEmpty());

            S3WALObject s3WALObject = listPair.getRight().get(0);
            Assertions.assertEquals(123, s3WALObject.getObjectId());
            Assertions.assertEquals(22, s3WALObject.getObjectSize());
            Assertions.assertEquals(1, s3WALObject.getBrokerId());
            Assertions.assertEquals(999, s3WALObject.getSequenceId());
            Assertions.assertEquals(subStreams, s3WALObject.getSubStreamsMap());
        }

        try (SqlSession session = getSessionFactory().openSession()) {
            S3WalObjectMapper s3WALObjectMapper = session.getMapper(S3WalObjectMapper.class);
            s3WALObjectMapper.delete(123L, 1, null);
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
        Gson gson = new Gson();
        String subStreamsJson = """
            {
              "1": {
                "streamId_": 1,
                "startOffset_": 10,
                "endOffset_": 20
              },
              "2": {
                "streamId_": 1,
                "startOffset_": 20,
                "endOffset_": 30
              },
              "3": {
                "streamId_": 1,
                "startOffset_": 30,
                "endOffset_": 40
              },
              "4": {
                "streamId_": 2,
                "startOffset_": 40,
                "endOffset_": 50
              }
            }""";

        try (SqlSession session = getSessionFactory().openSession()) {
            S3WalObjectMapper s3WALObjectMapper = session.getMapper(S3WalObjectMapper.class);
            S3WalObject s3WALObject = new S3WalObject();
            s3WALObject.setObjectId(123L);
            s3WALObject.setNodeId(1);
            s3WALObject.setObjectSize(22L);
            s3WALObject.setSequenceId(999L);
            s3WALObject.setSubStreams(subStreamsJson);

            s3WALObject.setBaseDataTimestamp(System.currentTimeMillis());

            s3WALObjectMapper.create(s3WALObject);

            S3StreamObjectMapper s3StreamObjectMapper = session.getMapper(S3StreamObjectMapper.class);

            com.automq.rocketmq.controller.metadata.database.dao.S3StreamObject s3StreamObject = new com.automq.rocketmq.controller.metadata.database.dao.S3StreamObject();
            s3StreamObject.setObjectId(122L);
            s3StreamObject.setObjectSize(123L);
            s3StreamObject.setStreamId(streamId);
            s3StreamObject.setStartOffset(0L);
            s3StreamObject.setEndOffset(10L);
            s3StreamObject.setBaseDataTimestamp(System.currentTimeMillis());

            s3StreamObjectMapper.create(s3StreamObject);

            session.commit();
        }
        String expectSubStream = """
            {
              "1": {
                "streamId_": 1,
                "startOffset_": 10,
                "endOffset_": 20
              }}""";
        Map<Long, SubStream> subStreams = gson.fromJson(new String(expectSubStream.getBytes(StandardCharsets.UTF_8)), new TypeToken<Map<Long, SubStream>>() {
        }.getType());

        try (DefaultMetadataStore metadataStore = new DefaultMetadataStore(client, getSessionFactory(), config)) {
            Assertions.assertNull(metadataStore.getLease());
            Lease lease = new Lease();
            lease.setNodeId(config.nodeId());
            metadataStore.setLease(lease);
            Pair<List<S3StreamObject>, List<S3WALObject>> listPair = metadataStore.listObjects(streamId, startOffset, endOffset, limit).get();

            Assertions.assertFalse(listPair.getLeft().isEmpty());
            Assertions.assertFalse(listPair.getRight().isEmpty());
            S3StreamObject s3StreamObject = listPair.getLeft().get(0);
            Assertions.assertEquals(122, s3StreamObject.getObjectId());
            Assertions.assertEquals(123, s3StreamObject.getObjectSize());
            Assertions.assertEquals(streamId, s3StreamObject.getStreamId());
            Assertions.assertEquals(0, s3StreamObject.getStartOffset());
            Assertions.assertEquals(10, s3StreamObject.getEndOffset());

            S3WALObject s3WALObject = listPair.getRight().get(0);
            Assertions.assertEquals(123, s3WALObject.getObjectId());
            Assertions.assertEquals(22, s3WALObject.getObjectSize());
            Assertions.assertEquals(1, s3WALObject.getBrokerId());
            Assertions.assertEquals(999, s3WALObject.getSequenceId());
            Assertions.assertEquals(subStreams, s3WALObject.getSubStreamsMap());
        }

        try (SqlSession session = getSessionFactory().openSession()) {
            S3WalObjectMapper s3WALObjectMapper = session.getMapper(S3WalObjectMapper.class);
            s3WALObjectMapper.delete(123L, 1, null);

            S3StreamObjectMapper s3StreamObjectMapper = session.getMapper(S3StreamObjectMapper.class);
            s3StreamObjectMapper.delete(null, streamId, 122L);
            session.commit();
        }
    }

    @Test
    public void testListObjects_Both_DifferentNode() throws IOException, ExecutionException, InterruptedException {
        long streamId, startOffset, endOffset;
        streamId = 1;
        startOffset = 0L;
        endOffset = 50L;
        int limit = 5;
        Gson gson = new Gson();
        String subStreamsJson1 = """
            {
              "1": {
                "streamId_": 1,
                "startOffset_": 10,
                "endOffset_": 20
              },
              "2": {
                "streamId_": 1,
                "startOffset_": 20,
                "endOffset_": 30
              }
            }""";

        String subStreamsJson2 = """
            {
              "1": {
                "streamId_": 1,
                "startOffset_": 20,
                "endOffset_": 40
              },
              "4": {
                "streamId_": 2,
                "startOffset_": 40,
                "endOffset_": 50
              }
            }""";

        try (SqlSession session = getSessionFactory().openSession()) {
            S3WalObjectMapper s3WALObjectMapper = session.getMapper(S3WalObjectMapper.class);
            S3StreamObjectMapper s3StreamObjectMapper = session.getMapper(S3StreamObjectMapper.class);

            S3WalObject s3WALObject = new S3WalObject();
            s3WALObject.setObjectId(123L);
            s3WALObject.setNodeId(1);
            s3WALObject.setObjectSize(22L);
            s3WALObject.setSequenceId(999L);
            s3WALObject.setSubStreams(subStreamsJson1);
            s3WALObject.setBaseDataTimestamp(System.currentTimeMillis());
            s3WALObjectMapper.create(s3WALObject);

            S3WalObject s3WALObject1 = new S3WalObject();
            s3WALObject1.setObjectId(124L);
            s3WALObject1.setNodeId(2);
            s3WALObject1.setObjectSize(24L);
            s3WALObject1.setSequenceId(1000L);
            s3WALObject1.setSubStreams(subStreamsJson2);
            s3WALObject1.setBaseDataTimestamp(System.currentTimeMillis());
            s3WALObjectMapper.create(s3WALObject1);

            com.automq.rocketmq.controller.metadata.database.dao.S3StreamObject s3StreamObject = new com.automq.rocketmq.controller.metadata.database.dao.S3StreamObject();
            s3StreamObject.setObjectId(121L);
            s3StreamObject.setObjectSize(123L);
            s3StreamObject.setStreamId(streamId);
            s3StreamObject.setStartOffset(0L);
            s3StreamObject.setEndOffset(10L);
            s3StreamObject.setBaseDataTimestamp(System.currentTimeMillis());
            s3StreamObjectMapper.create(s3StreamObject);

            com.automq.rocketmq.controller.metadata.database.dao.S3StreamObject s3StreamObject1 = new com.automq.rocketmq.controller.metadata.database.dao.S3StreamObject();
            s3StreamObject.setObjectId(122L);
            s3StreamObject.setObjectSize(124L);
            s3StreamObject.setStreamId(streamId);
            s3StreamObject.setStartOffset(40L);
            s3StreamObject.setEndOffset(50L);
            s3StreamObject.setBaseDataTimestamp(System.currentTimeMillis());
            s3StreamObjectMapper.create(s3StreamObject);

            session.commit();
        }
        String expectSubStream1 = """
            {
              "1": {
                "streamId_": 1,
                "startOffset_": 10,
                "endOffset_": 20
              }}""";
        Map<Long, SubStream> subStreams1 = gson.fromJson(new String(expectSubStream1.getBytes(StandardCharsets.UTF_8)), new TypeToken<Map<Long, SubStream>>() {
        }.getType());

        String expectSubStream2 = """
            {
              "1": {
                "streamId_": 1,
                "startOffset_": 20,
                "endOffset_": 40
              }}""";
        Map<Long, SubStream> subStreams2 = gson.fromJson(new String(expectSubStream2.getBytes(StandardCharsets.UTF_8)), new TypeToken<Map<Long, SubStream>>() {
        }.getType());

        try (DefaultMetadataStore metadataStore = new DefaultMetadataStore(client, getSessionFactory(), config)) {
            Assertions.assertNull(metadataStore.getLease());
            Lease lease = new Lease();
            lease.setNodeId(config.nodeId());
            metadataStore.setLease(lease);
            Pair<List<S3StreamObject>, List<S3WALObject>> listPair = metadataStore.listObjects(streamId, startOffset, endOffset, limit).get();

            Assertions.assertFalse(listPair.getLeft().isEmpty());
            Assertions.assertFalse(listPair.getRight().isEmpty());
            List<S3StreamObject> s3StreamObjects = listPair.getLeft();
            Assertions.assertEquals(2, s3StreamObjects.size());
            S3StreamObject s3StreamObject = s3StreamObjects.get(0);

            Assertions.assertEquals(121, s3StreamObject.getObjectId());
            Assertions.assertEquals(123, s3StreamObject.getObjectSize());
            Assertions.assertEquals(streamId, s3StreamObject.getStreamId());
            Assertions.assertEquals(0, s3StreamObject.getStartOffset());
            Assertions.assertEquals(10, s3StreamObject.getEndOffset());

            S3StreamObject s3StreamObject1 = s3StreamObjects.get(1);
            Assertions.assertEquals(122, s3StreamObject1.getObjectId());
            Assertions.assertEquals(124, s3StreamObject1.getObjectSize());
            Assertions.assertEquals(streamId, s3StreamObject1.getStreamId());
            Assertions.assertEquals(40, s3StreamObject1.getStartOffset());
            Assertions.assertEquals(50, s3StreamObject1.getEndOffset());

            List<S3WALObject> s3WALObjects = listPair.getRight();
            Assertions.assertEquals(2, s3WALObjects.size());
            S3WALObject s3WALObject = s3WALObjects.get(0);
            Assertions.assertEquals(123, s3WALObject.getObjectId());
            Assertions.assertEquals(22, s3WALObject.getObjectSize());
            Assertions.assertEquals(1, s3WALObject.getBrokerId());
            Assertions.assertEquals(999, s3WALObject.getSequenceId());
            Assertions.assertEquals(subStreams1, s3WALObject.getSubStreamsMap());

            S3WALObject s3WALObject1 = s3WALObjects.get(1);
            Assertions.assertEquals(124, s3WALObject1.getObjectId());
            Assertions.assertEquals(24, s3WALObject1.getObjectSize());
            Assertions.assertEquals(2, s3WALObject1.getBrokerId());
            Assertions.assertEquals(1000, s3WALObject1.getSequenceId());
            Assertions.assertEquals(subStreams2, s3WALObject1.getSubStreamsMap());
        }

        try (SqlSession session = getSessionFactory().openSession()) {
            S3WalObjectMapper s3WALObjectMapper = session.getMapper(S3WalObjectMapper.class);
            s3WALObjectMapper.delete(123L, 1, null);
            s3WALObjectMapper.delete(124L, 2, null);

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
        Gson gson = new Gson();
        String subStreamsJson1 = """
            {
              "1": {
                "streamId_": 1,
                "startOffset_": 0,
                "endOffset_": 10
              },
              "2": {
                "streamId_": 1,
                "startOffset_": 20,
                "endOffset_": 30
              }
            }""";

        String subStreamsJson2 = """
            {
              "1": {
                "streamId_": 1,
                "startOffset_": 20,
                "endOffset_": 40
              },
              "4": {
                "streamId_": 2,
                "startOffset_": 40,
                "endOffset_": 50
              }
            }""";

        try (SqlSession session = getSessionFactory().openSession()) {
            S3WalObjectMapper s3WALObjectMapper = session.getMapper(S3WalObjectMapper.class);
            S3StreamObjectMapper s3StreamObjectMapper = session.getMapper(S3StreamObjectMapper.class);

            S3WalObject s3WALObject = new S3WalObject();
            s3WALObject.setObjectId(123L);
            s3WALObject.setNodeId(1);
            s3WALObject.setObjectSize(22L);
            s3WALObject.setSequenceId(999L);
            s3WALObject.setSubStreams(subStreamsJson1);
            s3WALObject.setBaseDataTimestamp(System.currentTimeMillis());
            s3WALObjectMapper.create(s3WALObject);

            S3WalObject s3WALObject1 = new S3WalObject();
            s3WALObject1.setObjectId(124L);
            s3WALObject1.setNodeId(2);
            s3WALObject1.setObjectSize(24L);
            s3WALObject1.setSequenceId(1000L);
            s3WALObject1.setSubStreams(subStreamsJson2);
            s3WALObject1.setBaseDataTimestamp(System.currentTimeMillis());
            s3WALObjectMapper.create(s3WALObject1);

            com.automq.rocketmq.controller.metadata.database.dao.S3StreamObject s3StreamObject = new com.automq.rocketmq.controller.metadata.database.dao.S3StreamObject();
            s3StreamObject.setObjectId(121L);
            s3StreamObject.setObjectSize(123L);
            s3StreamObject.setStreamId(streamId);
            s3StreamObject.setStartOffset(10L);
            s3StreamObject.setEndOffset(20L);
            s3StreamObject.setBaseDataTimestamp(System.currentTimeMillis());
            s3StreamObjectMapper.create(s3StreamObject);

            com.automq.rocketmq.controller.metadata.database.dao.S3StreamObject s3StreamObject1 = new com.automq.rocketmq.controller.metadata.database.dao.S3StreamObject();
            s3StreamObject1.setObjectId(122L);
            s3StreamObject1.setObjectSize(124L);
            s3StreamObject1.setStreamId(streamId);
            s3StreamObject1.setStartOffset(40L);
            s3StreamObject1.setEndOffset(50L);
            s3StreamObject1.setBaseDataTimestamp(System.currentTimeMillis());
            s3StreamObjectMapper.create(s3StreamObject1);

            session.commit();
        }
        String expectSubStream1 = """
            {
              "1": {
                "streamId_": 1,
                "startOffset_": 0,
                "endOffset_": 10
              }}""";
        Map<Long, SubStream> subStreams1 = gson.fromJson(new String(expectSubStream1.getBytes(StandardCharsets.UTF_8)), new TypeToken<Map<Long, SubStream>>() {
        }.getType());

        String expectSubStream2 = """
            {
              "1": {
                "streamId_": 1,
                "startOffset_": 20,
                "endOffset_": 40
              }}""";
        Map<Long, SubStream> subStreams2 = gson.fromJson(new String(expectSubStream2.getBytes(StandardCharsets.UTF_8)), new TypeToken<Map<Long, SubStream>>() {
        }.getType());

        try (DefaultMetadataStore metadataStore = new DefaultMetadataStore(client, getSessionFactory(), config)) {
            Assertions.assertNull(metadataStore.getLease());
            Lease lease = new Lease();
            lease.setNodeId(config.nodeId());
            metadataStore.setLease(lease);
            Pair<List<S3StreamObject>, List<S3WALObject>> listPair = metadataStore.listObjects(streamId, startOffset, endOffset, limit).get();

            Assertions.assertFalse(listPair.getLeft().isEmpty());
            Assertions.assertFalse(listPair.getRight().isEmpty());
            List<S3StreamObject> s3StreamObjects = listPair.getLeft();
            Assertions.assertEquals(1, s3StreamObjects.size());
            S3StreamObject s3StreamObject = s3StreamObjects.get(0);

            Assertions.assertEquals(121, s3StreamObject.getObjectId());
            Assertions.assertEquals(123, s3StreamObject.getObjectSize());
            Assertions.assertEquals(streamId, s3StreamObject.getStreamId());
            Assertions.assertEquals(10, s3StreamObject.getStartOffset());
            Assertions.assertEquals(20, s3StreamObject.getEndOffset());


            List<S3WALObject> s3WALObjects = listPair.getRight();
            Assertions.assertEquals(2, s3WALObjects.size());
            S3WALObject s3WALObject = s3WALObjects.get(0);
            Assertions.assertEquals(123, s3WALObject.getObjectId());
            Assertions.assertEquals(22, s3WALObject.getObjectSize());
            Assertions.assertEquals(1, s3WALObject.getBrokerId());
            Assertions.assertEquals(999, s3WALObject.getSequenceId());
            Assertions.assertEquals(subStreams1, s3WALObject.getSubStreamsMap());

            S3WALObject s3WALObject1 = s3WALObjects.get(1);
            Assertions.assertEquals(124, s3WALObject1.getObjectId());
            Assertions.assertEquals(24, s3WALObject1.getObjectSize());
            Assertions.assertEquals(2, s3WALObject1.getBrokerId());
            Assertions.assertEquals(1000, s3WALObject1.getSequenceId());
            Assertions.assertEquals(subStreams2, s3WALObject1.getSubStreamsMap());
        }

        try (SqlSession session = getSessionFactory().openSession()) {
            S3WalObjectMapper s3WALObjectMapper = session.getMapper(S3WalObjectMapper.class);
            s3WALObjectMapper.delete(123L, 1, null);
            s3WALObjectMapper.delete(124L, 2, null);

            S3StreamObjectMapper s3StreamObjectMapper = session.getMapper(S3StreamObjectMapper.class);
            s3StreamObjectMapper.delete(null, streamId, 122L);
            session.commit();
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
            stream.setStartOffset(1234);
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
            stream.setStartOffset(1234);
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
            stream.setStartOffset(1234);
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
            stream.setStartOffset(1234);
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
            stream.setStartOffset(1234);
            streamMapper.create(stream);
            dataStreamId = stream.getId();

            stream.setStreamRole(StreamRole.STREAM_ROLE_OPS);
            stream.setStartOffset(1234);
            streamMapper.create(stream);
            opsStreamId = stream.getId();

            stream.setStreamRole(StreamRole.STREAM_ROLE_RETRY);
            stream.setGroupId(3L);
            stream.setStartOffset(1234);
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
            ConsumerGroup got = metadataStore.describeConsumerGroup(groupId, null).get();
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
            stream.setEpoch(1);
            stream.setTopicId(2L);
            stream.setQueueId(3);
            stream.setDstNodeId(4);
            stream.setSrcNodeId(5);
            stream.setState(StreamState.OPEN);
            stream.setStartOffset(6);
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
    public void testTrimStream() throws IOException, ControllerException {
        long streamId, streamEpoch = 1, newStartOffset = 2000;
        int nodeId = 1, rangId = 0;
        try (SqlSession session = this.getSessionFactory().openSession()) {
            StreamMapper streamMapper = session.getMapper(StreamMapper.class);
            RangeMapper rangeMapper = session.getMapper(RangeMapper.class);

            com.automq.rocketmq.controller.metadata.database.dao.Stream stream = new com.automq.rocketmq.controller.metadata.database.dao.Stream();
            stream.setSrcNodeId(nodeId);
            stream.setDstNodeId(nodeId);
            stream.setStartOffset(1234);
            stream.setEpoch(0);
            stream.setRangeId(rangId);
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

        try (MetadataStore metadataStore = new DefaultMetadataStore(client, getSessionFactory(), config)) {
            metadataStore.start();
            Awaitility.await().with().atMost(10, TimeUnit.SECONDS).pollInterval(100, TimeUnit.MILLISECONDS)
                .until(metadataStore::isLeader);

            metadataStore.trimStream(streamId, streamEpoch, newStartOffset);
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
    public void testListOpenStream() throws IOException, ExecutionException, InterruptedException {
        long streamId;
        int nodeId = 1, rangId = 0;
        try (SqlSession session = this.getSessionFactory().openSession()) {
            StreamMapper streamMapper = session.getMapper(StreamMapper.class);
            RangeMapper rangeMapper = session.getMapper(RangeMapper.class);

            com.automq.rocketmq.controller.metadata.database.dao.Stream stream = new com.automq.rocketmq.controller.metadata.database.dao.Stream();
            stream.setSrcNodeId(nodeId);
            stream.setDstNodeId(nodeId);
            stream.setStartOffset(1234);
            stream.setEpoch(0);
            stream.setRangeId(rangId);
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
            stream.setStartOffset(1234);
            stream.setEpoch(0);
            stream.setRangeId(rangId);
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
    public void testPrepareS3Objects() throws IOException {
        long objectId;

        try (DefaultMetadataStore metadataStore = new DefaultMetadataStore(client, getSessionFactory(), config)) {
            Assertions.assertNull(metadataStore.getLease());
            Lease lease = new Lease();
            lease.setNodeId(config.nodeId());
            metadataStore.setLease(lease);
            metadataStore.setRole(Role.Leader);
            objectId = metadataStore.prepareS3Objects(3, 5).get();
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
    public void testCommitStreamObject() throws IOException, ControllerException {
        long objectId, streamId = 1;

        try (DefaultMetadataStore metadataStore = new DefaultMetadataStore(client, getSessionFactory(), config)) {
            Assertions.assertNull(metadataStore.getLease());
            Lease lease = new Lease();
            lease.setNodeId(config.nodeId());
            metadataStore.setLease(lease);
            metadataStore.setRole(Role.Leader);
            objectId = metadataStore.prepareS3Objects(3, 5).get();
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
            com.automq.rocketmq.controller.metadata.database.dao.S3StreamObject object = new com.automq.rocketmq.controller.metadata.database.dao.S3StreamObject();
            object.setObjectId(objectId);
            object.setObjectSize(222L);
            object.setStreamId(streamId);
            object.setBaseDataTimestamp(1L);
            object.setStartOffset(1234L);
            object.setEndOffset(2345L);
            s3StreamObjectMapper.create(object);

            com.automq.rocketmq.controller.metadata.database.dao.S3StreamObject object1 = new com.automq.rocketmq.controller.metadata.database.dao.S3StreamObject();
            object1.setObjectId(objectId + 1);
            object1.setObjectSize(333L);
            object1.setStreamId(streamId);
            object1.setBaseDataTimestamp(2L);
            object1.setStartOffset(2345L);
            object1.setEndOffset(3456L);
            s3StreamObjectMapper.create(object1);

            session.commit();
        }

        long time = System.currentTimeMillis();

        try (DefaultMetadataStore metadataStore = new DefaultMetadataStore(client, getSessionFactory(), config)) {
            Assertions.assertNull(metadataStore.getLease());
            Lease lease = new Lease();
            lease.setNodeId(config.nodeId());
            metadataStore.setLease(lease);
            metadataStore.setRole(Role.Leader);

            List<Long> compactedObjects = new ArrayList<>();
            compactedObjects.add(objectId);
            compactedObjects.add(objectId + 1);
            metadataStore.commitStreamObject(news3StreamObject, compactedObjects);
        }

        try (SqlSession session = getSessionFactory().openSession()) {
            S3ObjectMapper s3ObjectMapper = session.getMapper(S3ObjectMapper.class);
            S3Object s3Object = s3ObjectMapper.getById(objectId);
            Assertions.assertEquals(S3ObjectState.BOS_WILL_DELETE, s3Object.getState());

            S3Object s3Object1 = s3ObjectMapper.getById(objectId);
            Assertions.assertEquals(S3ObjectState.BOS_WILL_DELETE, s3Object1.getState());


            S3StreamObjectMapper s3StreamObjectMapper = session.getMapper(S3StreamObjectMapper.class);
            for (long index = objectId; index < objectId + 2; index++) {
                com.automq.rocketmq.controller.metadata.database.dao.S3StreamObject object = s3StreamObjectMapper.getByObjectId(index);
                Assertions.assertNull(object);
            }

            com.automq.rocketmq.controller.metadata.database.dao.S3StreamObject object = s3StreamObjectMapper.getByObjectId(objectId + 2);
            Assertions.assertEquals(111L, object.getObjectSize());
            Assertions.assertEquals(streamId, object.getStreamId());
            Assertions.assertEquals(1L, object.getBaseDataTimestamp());
            if (object.getCommittedTimestamp() - time > 5 * 60) {
                Assertions.fail();
            }
        }
    }

    @Test
    public void testCommitStreamObject_NoCompacted() throws IOException, ControllerException {
        long objectId, streamId = 1;

        try (DefaultMetadataStore metadataStore = new DefaultMetadataStore(client, getSessionFactory(), config)) {
            Assertions.assertNull(metadataStore.getLease());
            Lease lease = new Lease();
            lease.setNodeId(config.nodeId());
            metadataStore.setLease(lease);
            metadataStore.setRole(Role.Leader);
            objectId = metadataStore.prepareS3Objects(3, 5).get();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        S3StreamObject news3StreamObject = S3StreamObject.newBuilder()
            .setObjectId(objectId + 2)
            .setStreamId(streamId)
            .setObjectSize(111L)
            .build();

        long time = System.currentTimeMillis();

        try (DefaultMetadataStore metadataStore = new DefaultMetadataStore(client, getSessionFactory(), config)) {
            Assertions.assertNull(metadataStore.getLease());
            Lease lease = new Lease();
            lease.setNodeId(config.nodeId());
            metadataStore.setLease(lease);
            metadataStore.setLease(lease);
            metadataStore.setRole(Role.Leader);

            metadataStore.commitStreamObject(news3StreamObject, Collections.emptyList());
        }

        try (SqlSession session = getSessionFactory().openSession()) {
            S3ObjectMapper s3ObjectMapper = session.getMapper(S3ObjectMapper.class);
            S3StreamObjectMapper s3StreamObjectMapper = session.getMapper(S3StreamObjectMapper.class);
            S3Object s3Object = s3ObjectMapper.getById(objectId + 2);
            Assertions.assertEquals(S3ObjectState.BOS_COMMITTED, s3Object.getState());

            com.automq.rocketmq.controller.metadata.database.dao.S3StreamObject object = s3StreamObjectMapper.getByObjectId(objectId + 2);
            Assertions.assertTrue(object.getBaseDataTimestamp() > 1);
            if (object.getBaseDataTimestamp() - time > 5 * 60) {
                Assertions.fail();
            }
            Assertions.assertNotNull(object.getCommittedTimestamp());
            Assertions.assertTrue(object.getCommittedTimestamp() > 0);
            Assertions.assertEquals(111L, object.getObjectSize());
            Assertions.assertEquals(streamId, object.getStreamId());
            if (object.getCommittedTimestamp() - time > 5 * 60) {
                Assertions.fail();
            }
        }
    }

    @Test
    public void testCommitStreamObject_ObjectNotExist() throws IOException {
        long streamId = 1;
        int nodeId = 1;

        S3StreamObject s3StreamObject = S3StreamObject.newBuilder()
            .setObjectId(1)
            .setStreamId(streamId)
            .setObjectSize(111L)
            .build();

        try (SqlSession session = this.getSessionFactory().openSession()) {
            S3StreamObjectMapper s3StreamObjectMapper = session.getMapper(S3StreamObjectMapper.class);
            com.automq.rocketmq.controller.metadata.database.dao.S3StreamObject object =
                new com.automq.rocketmq.controller.metadata.database.dao.S3StreamObject();
            object.setObjectId(2L);
            object.setStreamId(streamId);
            object.setBaseDataTimestamp(1L);
            object.setStartOffset(0L);
            object.setEndOffset(2L);
            object.setObjectSize(2139L);
            s3StreamObjectMapper.create(object);
        }

        List<Long> compactedObjects = new ArrayList<>();
        try (DefaultMetadataStore metadataStore = new DefaultMetadataStore(client, getSessionFactory(), config)) {
            Assertions.assertNull(metadataStore.getLease());
            Lease lease = new Lease();
            lease.setNodeId(config.nodeId());
            metadataStore.setLease(lease);
            metadataStore.setRole(Role.Leader);

            Assertions.assertThrows(ExecutionException.class, () -> metadataStore.commitStreamObject(s3StreamObject, compactedObjects).get());
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
            com.automq.rocketmq.controller.metadata.database.dao.S3StreamObject object =
                new com.automq.rocketmq.controller.metadata.database.dao.S3StreamObject();
            object.setObjectId(2L);
            object.setStreamId(streamId);
            object.setBaseDataTimestamp(1L);
            object.setStartOffset(0L);
            object.setEndOffset(2L);
            object.setObjectSize(2139L);
            s3StreamObjectMapper.create(object);
        }

        List<Long> compactedObjects = new ArrayList<>();
        try (DefaultMetadataStore metadataStore = new DefaultMetadataStore(client, getSessionFactory(), config)) {
            Assertions.assertNull(metadataStore.getLease());
            Lease lease = new Lease();
            lease.setNodeId(config.nodeId());
            metadataStore.setLease(lease);
            metadataStore.setRole(Role.Leader);

            Assertions.assertThrows(ExecutionException.class, () -> metadataStore.commitStreamObject(s3StreamObject, compactedObjects).get());
        }

    }

    @Test
    public void testCommitWALObject() throws IOException, ExecutionException, InterruptedException {
        long objectId, streamId = 1;
        int nodeId = 1;

        try (DefaultMetadataStore metadataStore = new DefaultMetadataStore(client, getSessionFactory(), config)) {
            Assertions.assertNull(metadataStore.getLease());
            Lease lease = new Lease();
            lease.setNodeId(config.nodeId());
            metadataStore.setLease(lease);
            metadataStore.setRole(Role.Leader);
            objectId = metadataStore.prepareS3Objects(5, 5).get();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        S3WALObject walObject = S3WALObject.newBuilder()
            .setObjectId(objectId + 4)
            .setObjectSize(222L)
            .setBrokerId(nodeId)
            .build();

        String expectSubStream = """
            {
              "1234567890": {
                "streamId_": 1234567890,
                "startOffset_": 0,
                "endOffset_": 10
              }}""";

        try (SqlSession session = this.getSessionFactory().openSession()) {
            S3WalObjectMapper s3WALObjectMapper = session.getMapper(S3WalObjectMapper.class);
            S3WalObject s3WALObject = new S3WalObject();
            s3WALObject.setObjectId(objectId + 2);
            s3WALObject.setObjectSize(333L);
            s3WALObject.setBaseDataTimestamp(3);
            s3WALObject.setSequenceId(1L);
            s3WALObject.setNodeId(nodeId);
            s3WALObject.setSubStreams(expectSubStream.replace("1234567890", String.valueOf(streamId)));
            s3WALObjectMapper.create(s3WALObject);

            S3WalObject s3WalObject1 = new S3WalObject();
            s3WalObject1.setObjectId(objectId + 3);
            s3WalObject1.setObjectSize(444L);
            s3WalObject1.setBaseDataTimestamp(4);
            s3WalObject1.setSequenceId(2L);
            s3WalObject1.setNodeId(nodeId);
            s3WalObject1.setSubStreams(expectSubStream.replace("1234567890", String.valueOf(streamId)));
            s3WALObjectMapper.create(s3WalObject1);

            session.commit();
        }

        long time = System.currentTimeMillis();
        List<Long> compactedObjects = new ArrayList<>();
        compactedObjects.add(objectId + 2);
        compactedObjects.add(objectId + 3);


        S3StreamObject s3StreamObject = S3StreamObject.newBuilder()
            .setObjectId(objectId)
            .setStreamId(streamId)
            .setObjectSize(111L)
            .setBaseDataTimestamp(1L)
            .setStartOffset(111L)
            .setEndOffset(222L)
            .build();


        S3StreamObject s3StreamObject1 = S3StreamObject.newBuilder()
            .setObjectId(objectId + 1)
            .setStreamId(streamId)
            .setObjectSize(222L)
            .setBaseDataTimestamp(2L)
            .setStartOffset(222L)
            .setEndOffset(333L)
            .build();

        List<S3StreamObject> s3StreamObjects = new ArrayList<>();
        s3StreamObjects.add(s3StreamObject);
        s3StreamObjects.add(s3StreamObject1);

        try (DefaultMetadataStore metadataStore = new DefaultMetadataStore(client, getSessionFactory(), config)) {
            Assertions.assertNull(metadataStore.getLease());
            Lease lease = new Lease();
            lease.setNodeId(config.nodeId());
            metadataStore.setLease(lease);
            metadataStore.setRole(Role.Leader);

            metadataStore.commitWalObject(walObject, s3StreamObjects, compactedObjects).get();
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
                com.automq.rocketmq.controller.metadata.database.dao.S3StreamObject object = s3StreamObjectMapper.getByObjectId(index);
                if (object.getCommittedTimestamp() - time > 5 * 60) {
                    Assertions.fail();
                }
            }

            S3Object s3Object = s3ObjectMapper.getById(objectId + 4);
            Assertions.assertEquals(222L, s3Object.getObjectSize());
            Assertions.assertEquals(StreamConstants.NOOP_STREAM_ID, s3Object.getStreamId());

            long baseTime = 3;
            S3WalObjectMapper s3WALObjectMapper = session.getMapper(S3WalObjectMapper.class);
            S3WalObject object = s3WALObjectMapper.getByObjectId(objectId + 4);
            Assertions.assertEquals(1L, object.getSequenceId());
            Assertions.assertEquals(baseTime, object.getBaseDataTimestamp());
            if (object.getCommittedTimestamp() - time > 5 * 60) {
                Assertions.fail();
            }
        }
    }

    @Test
    public void testCommitWalObject_ObjectNotExist() throws IOException, ExecutionException, InterruptedException {
        long streamId = 1;

        S3WALObject walObject = S3WALObject.newBuilder()
            .setObjectId(3)
            .setBrokerId(-1)
            .build();

        try (SqlSession session = this.getSessionFactory().openSession()) {
            S3StreamObjectMapper s3StreamObjectMapper = session.getMapper(S3StreamObjectMapper.class);
            com.automq.rocketmq.controller.metadata.database.dao.S3StreamObject object =
                new com.automq.rocketmq.controller.metadata.database.dao.S3StreamObject();
            object.setObjectId(2L);
            object.setStreamId(streamId);
            object.setBaseDataTimestamp(1L);
            object.setStartOffset(0L);
            object.setEndOffset(2L);
            object.setObjectSize(2139L);
            s3StreamObjectMapper.create(object);
        }

        List<Long> compactedObjects = new ArrayList<>();
        try (DefaultMetadataStore metadataStore = new DefaultMetadataStore(client, getSessionFactory(), config)) {
            Assertions.assertNull(metadataStore.getLease());
            Lease lease = new Lease();
            lease.setNodeId(config.nodeId());
            metadataStore.setLease(lease);
            metadataStore.setRole(Role.Leader);

            List<S3StreamObject> s3StreamObjects = metadataStore.listStreamObjects(streamId, 0, 334, 2).get();

            Assertions.assertThrows(ExecutionException.class, () -> metadataStore.commitWalObject(walObject, s3StreamObjects, compactedObjects).get());
        }

    }

    @Test
    public void testCommitWalObject_WalNotExist() throws IOException, ExecutionException, InterruptedException {
        long streamId;
        int nodeId = 1;
        long objectId;

        S3WALObject walObject = S3WALObject.newBuilder()
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
            stream.setTopicId(2);
            stream.setRangeId(1);
            stream.setEpoch(3);
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
            Calendar calendar = Calendar.getInstance();
            calendar.add(Calendar.HOUR, 1);
            s3Object.setExpiredTimestamp(calendar.getTime());
            objectMapper.prepare(s3Object);
            objectId = s3Object.getId();

            session.commit();
        }

        List<Long> compactedObjects = new ArrayList<>();
        try (DefaultMetadataStore metadataStore = new DefaultMetadataStore(client, getSessionFactory(), config);
             SqlSession session = getSessionFactory().openSession()) {
            metadataStore.start();
            Awaitility.await().with()
                .pollInterval(100, TimeUnit.MILLISECONDS)
                .atMost(10, TimeUnit.SECONDS)
                .until(metadataStore::isLeader);

            S3StreamObject streamObject = S3StreamObject.newBuilder()
                .setObjectId(objectId)
                .setStreamId(streamId)
                .setBaseDataTimestamp(1)
                .setStartOffset(0)
                .setEndOffset(2)
                .setObjectSize(2139)
                .build();

            List<S3StreamObject> s3StreamObjects = new ArrayList<>();
            s3StreamObjects.add(streamObject);
            metadataStore.commitWalObject(walObject, s3StreamObjects, compactedObjects).get();
        }

        try (SqlSession session = getSessionFactory().openSession()) {
            S3StreamObjectMapper mapper = session.getMapper(S3StreamObjectMapper.class);
            com.automq.rocketmq.controller.metadata.database.dao.S3StreamObject s3StreamObject = mapper.getByObjectId(objectId);
            Assertions.assertTrue(s3StreamObject.getCommittedTimestamp() > 0);

            S3ObjectMapper objectMapper = session.getMapper(S3ObjectMapper.class);
            S3Object s3Object = objectMapper.getById(objectId);
            Assertions.assertNotNull(s3Object.getCommittedTimestamp());
            Assertions.assertEquals(S3ObjectState.BOS_COMMITTED, s3Object.getState());

            RangeMapper rangeMapper = session.getMapper(RangeMapper.class);
            Range range = rangeMapper.get(1, streamId, null);
            Assertions.assertEquals(2, range.getEndOffset());
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

}