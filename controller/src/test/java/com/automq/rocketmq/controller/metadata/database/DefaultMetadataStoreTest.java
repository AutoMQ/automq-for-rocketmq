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
import com.automq.rocketmq.controller.exception.ControllerException;
import com.automq.rocketmq.controller.metadata.ControllerClient;
import com.automq.rocketmq.controller.metadata.ControllerConfig;
import com.automq.rocketmq.controller.metadata.DatabaseTestBase;
import com.automq.rocketmq.controller.metadata.MetadataStore;
import com.automq.rocketmq.controller.metadata.Role;
import com.automq.rocketmq.controller.metadata.database.dao.Group;
import com.automq.rocketmq.controller.metadata.database.dao.Lease;
import com.automq.rocketmq.controller.metadata.database.dao.Node;
import com.automq.rocketmq.controller.metadata.database.dao.QueueAssignment;
import com.automq.rocketmq.controller.metadata.database.dao.S3Object;
import com.automq.rocketmq.controller.metadata.database.dao.Stream;
import com.automq.rocketmq.controller.metadata.database.dao.Range;
import com.automq.rocketmq.controller.metadata.database.dao.Topic;
import com.automq.rocketmq.controller.metadata.database.mapper.GroupMapper;
import com.automq.rocketmq.controller.metadata.database.mapper.NodeMapper;
import com.automq.rocketmq.controller.metadata.database.mapper.QueueAssignmentMapper;
import com.automq.rocketmq.controller.metadata.database.mapper.RangeMapper;
import com.automq.rocketmq.controller.metadata.database.mapper.S3ObjectMapper;
import com.automq.rocketmq.controller.metadata.database.mapper.S3StreamObjectMapper;
import com.automq.rocketmq.controller.metadata.database.mapper.S3WALObjectMapper;
import com.automq.rocketmq.controller.metadata.database.mapper.StreamMapper;
import com.automq.rocketmq.controller.metadata.database.mapper.TopicMapper;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
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
    void testRegisterNode() throws IOException, ExecutionException, InterruptedException, ControllerException {
        ControllerConfig config = Mockito.mock(ControllerConfig.class);
        Mockito.when(config.nodeId()).thenReturn(1);
        Mockito.when(config.scanIntervalInSecs()).thenReturn(1);
        Mockito.when(config.leaseLifeSpanInSecs()).thenReturn(1);
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
        }
    }

    @Test
    void testRegisterBroker_badArguments() throws IOException, ControllerException {
        ControllerConfig config = Mockito.mock(ControllerConfig.class);
        Mockito.when(config.nodeId()).thenReturn(1);
        Mockito.when(config.scanIntervalInSecs()).thenReturn(1);
        Mockito.when(config.leaseLifeSpanInSecs()).thenReturn(1);
        try (DefaultMetadataStore metadataStore = new DefaultMetadataStore(client, getSessionFactory(), config)) {
            metadataStore.start();
            Awaitility.await().with().atMost(10, TimeUnit.SECONDS)
                .pollInterval(100, TimeUnit.MILLISECONDS)
                .until(metadataStore::isLeader);
            String name = "test-broker -0";
            String address = "localhost:1234";
            String instanceId = "i-register";
            Assertions.assertThrows(ControllerException.class, () -> metadataStore.registerBrokerNode("", address, instanceId));
            Assertions.assertThrows(ControllerException.class, () -> metadataStore.registerBrokerNode(name, null, instanceId));
            Assertions.assertThrows(ControllerException.class, () -> metadataStore.registerBrokerNode(name, address, ""));
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
        ControllerConfig config = Mockito.mock(ControllerConfig.class);
        Mockito.when(config.nodeId()).thenReturn(1);
        Mockito.when(config.scanIntervalInSecs()).thenReturn(1);
        Mockito.when(config.leaseLifeSpanInSecs()).thenReturn(1);
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

        ControllerConfig config = Mockito.mock(ControllerConfig.class);
        Mockito.when(config.nodeId()).thenReturn(nodeId);
        Mockito.when(config.scanIntervalInSecs()).thenReturn(1);
        Mockito.when(config.leaseLifeSpanInSecs()).thenReturn(2);
        Mockito.when(config.nodeAliveIntervalInSecs()).thenReturn(10);

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
        ControllerConfig config = Mockito.mock(ControllerConfig.class);
        Mockito.when(config.nodeId()).thenReturn(1);
        Mockito.when(config.scanIntervalInSecs()).thenReturn(1);
        Mockito.when(config.leaseLifeSpanInSecs()).thenReturn(2);
        try (MetadataStore metadataStore = new DefaultMetadataStore(client, getSessionFactory(), config)) {
            Assertions.assertThrows(ControllerException.class, metadataStore::leaderAddress);
        }
    }

    @Test
    void testLeaderAddress_NoLeaderNode() throws IOException {
        ControllerConfig config = Mockito.mock(ControllerConfig.class);
        Mockito.when(config.nodeId()).thenReturn(1);
        Mockito.when(config.scanIntervalInSecs()).thenReturn(1);
        Mockito.when(config.leaseLifeSpanInSecs()).thenReturn(2);
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

        ControllerConfig config = Mockito.mock(ControllerConfig.class);
        Mockito.when(config.nodeId()).thenReturn(nodeId);
        Mockito.when(config.scanIntervalInSecs()).thenReturn(1);
        Mockito.when(config.leaseLifeSpanInSecs()).thenReturn(2);
        Mockito.when(config.nodeAliveIntervalInSecs()).thenReturn(10);
        long topicId;
        int queueNum = 4;
        String topicName = "t1";
        try (MetadataStore metadataStore = new DefaultMetadataStore(client, getSessionFactory(), config)) {
            metadataStore.start();
            Awaitility.await().with().atMost(10, TimeUnit.SECONDS)
                .pollInterval(100, TimeUnit.MILLISECONDS)
                .until(metadataStore::isLeader);

            topicId = metadataStore.createTopic(topicName, queueNum).get();
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
            Assertions.assertEquals(queueNum * 2, streams.size());
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

        ControllerConfig config = Mockito.mock(ControllerConfig.class);
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
    public void testDeleteTopic() throws IOException, ExecutionException, InterruptedException, ControllerException {
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
            topicMapper.create(topic);
            topicId = topic.getId();
            session.commit();
        }

        ControllerConfig config = Mockito.mock(ControllerConfig.class);
        Mockito.when(config.nodeId()).thenReturn(nodeId);
        Mockito.when(config.scanIntervalInSecs()).thenReturn(1);
        Mockito.when(config.leaseLifeSpanInSecs()).thenReturn(2);
        Mockito.when(config.nodeAliveIntervalInSecs()).thenReturn(10);
        try (MetadataStore metadataStore = new DefaultMetadataStore(client, getSessionFactory(), config)) {
            metadataStore.start();
            Awaitility.await().with().atMost(10, TimeUnit.SECONDS).pollInterval(100, TimeUnit.MILLISECONDS)
                .until(metadataStore::isLeader);

            metadataStore.deleteTopic(topicId).get();
        }
    }

    @Test
    public void testDeleteTopic_NotFound() throws IOException, ControllerException {
        ControllerConfig config = Mockito.mock(ControllerConfig.class);
        Mockito.when(config.nodeId()).thenReturn(1);
        Mockito.when(config.scanIntervalInSecs()).thenReturn(1);
        Mockito.when(config.leaseLifeSpanInSecs()).thenReturn(2);
        try (MetadataStore metadataStore = new DefaultMetadataStore(client, getSessionFactory(), config)) {
            metadataStore.start();
            Awaitility.await().with().atMost(10, TimeUnit.SECONDS).pollInterval(100, TimeUnit.MILLISECONDS)
                .until(metadataStore::isLeader);
            Assertions.assertThrows(ControllerException.class, () -> metadataStore.deleteTopic(1));
        }
    }

    @Test
    public void testDescribeTopic() throws IOException, ExecutionException, InterruptedException, ControllerException {
        long topicId;
        try (SqlSession session = getSessionFactory().openSession()) {

            TopicMapper topicMapper = session.getMapper(TopicMapper.class);
            Topic topic = new Topic();
            topic.setName("T1");
            topic.setStatus(TopicStatus.TOPIC_STATUS_ACTIVE);
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

        ControllerConfig config = Mockito.mock(ControllerConfig.class);
        Mockito.when(config.nodeId()).thenReturn(1);
        Mockito.when(config.scanIntervalInSecs()).thenReturn(1);
        Mockito.when(config.leaseLifeSpanInSecs()).thenReturn(2);
        try (MetadataStore metadataStore = new DefaultMetadataStore(client, getSessionFactory(), config)) {
            metadataStore.start();
            Awaitility.await().with().atMost(10, TimeUnit.SECONDS).pollInterval(100, TimeUnit.MILLISECONDS)
                .until(metadataStore::isLeader);
            apache.rocketmq.controller.v1.Topic topic = metadataStore.describeTopic(topicId, null).get();
            Assertions.assertEquals("T1", topic.getName());
            Assertions.assertEquals(1, topic.getAssignmentsCount());
            Assertions.assertEquals(1, topic.getReassignmentsCount());
        }
    }

    @Test
    public void testMarkMessageQueueAssignable() throws IOException, ControllerException, ExecutionException, InterruptedException {
        try (SqlSession session = getSessionFactory().openSession()) {
            QueueAssignmentMapper assignmentMapper = session.getMapper(QueueAssignmentMapper.class);
            QueueAssignment assignment = new QueueAssignment();
            assignment.setQueueId(1);
            assignment.setTopicId(2);
            assignment.setStatus(AssignmentStatus.ASSIGNMENT_STATUS_YIELDING);
            assignmentMapper.create(assignment);
            session.commit();
        }

        ControllerConfig config = Mockito.mock(ControllerConfig.class);
        Mockito.when(config.nodeId()).thenReturn(1);
        Mockito.when(config.scanIntervalInSecs()).thenReturn(1);
        Mockito.when(config.leaseLifeSpanInSecs()).thenReturn(2);
        try (MetadataStore metadataStore = new DefaultMetadataStore(client, getSessionFactory(), config)) {
            metadataStore.start();
            Awaitility.await().with().atMost(10, TimeUnit.SECONDS).pollInterval(100, TimeUnit.MILLISECONDS)
                .until(metadataStore::isLeader);
            metadataStore.markMessageQueueAssignable(2, 1);

            List<QueueAssignment> assignments = metadataStore.listAssignments(2L, null, null, null).get();
            for (QueueAssignment assignment : assignments) {
                if (assignment.getQueueId() == 1) {
                    Assertions.assertEquals(AssignmentStatus.ASSIGNMENT_STATUS_ASSIGNABLE, assignment.getStatus());
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
            s3StreamObject.setObjectId(11);
            s3StreamObject.setObjectSize(123);
            s3StreamObject.setStreamId(111);
            s3StreamObject.setStartOffset(1234);
            s3StreamObject.setEndOffset(2345);
            s3StreamObject.setBaseDataTimestamp(System.currentTimeMillis());

            streamId = s3StreamObject.getStreamId();

            s3StreamObjectMapper.create(s3StreamObject);
            session.commit();
        }

        ControllerConfig config = Mockito.mock(ControllerConfig.class);

        Mockito.when(config.nodeId()).thenReturn(1);
        Mockito.when(config.scanIntervalInSecs()).thenReturn(1);
        Mockito.when(config.leaseLifeSpanInSecs()).thenReturn(2);
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
            S3WALObjectMapper s3WALObjectMapper = session.getMapper(S3WALObjectMapper.class);
            com.automq.rocketmq.controller.metadata.database.dao.S3WALObject s3WALObject = new com.automq.rocketmq.controller.metadata.database.dao.S3WALObject();
            s3WALObject.setObjectId(123);
            s3WALObject.setBrokerId(1);
            s3WALObject.setObjectSize(22);
            s3WALObject.setSequenceId(999);
            s3WALObject.setSubStreams(subStreamsJson);

            s3WALObject.setBaseDataTimestamp(System.currentTimeMillis());

            s3WALObjectMapper.create(s3WALObject);
            session.commit();
        }
        String expectSubStream = """
            {
              "1234567890": {
                "streamId_": 1234567890,
                "startOffset_": 0,
                "endOffset_": 10
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
            S3WALObjectMapper s3WALObjectMapper = session.getMapper(S3WALObjectMapper.class);
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
            S3WALObjectMapper s3WALObjectMapper = session.getMapper(S3WALObjectMapper.class);
            com.automq.rocketmq.controller.metadata.database.dao.S3WALObject s3WALObject = new com.automq.rocketmq.controller.metadata.database.dao.S3WALObject();
            s3WALObject.setObjectId(123);
            s3WALObject.setBrokerId(1);
            s3WALObject.setObjectSize(22);
            s3WALObject.setSequenceId(999);
            s3WALObject.setSubStreams(subStreamsJson);

            s3WALObject.setBaseDataTimestamp(System.currentTimeMillis());

            s3WALObjectMapper.create(s3WALObject);
            session.commit();
        }
        Map<Long, SubStream> subStreams = gson.fromJson(new String(subStreamsJson.getBytes(StandardCharsets.UTF_8)), new TypeToken<Map<Long, SubStream>>() {
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
            S3WALObjectMapper s3WALObjectMapper = session.getMapper(S3WALObjectMapper.class);
            s3WALObjectMapper.delete(123L, 1, null);
            session.commit();
        }
    }

    @Test
    public void testOpenStream_WithCloseStream_AtStart() throws IOException, ExecutionException,
        InterruptedException {
        long streamEpoch = 0;
        long streamId;
        try (SqlSession session = this.getSessionFactory().openSession()) {
            StreamMapper streamMapper = session.getMapper(StreamMapper.class);
            Stream stream = new Stream();
            stream.setRangeId(-1);
            stream.setState(StreamState.UNINITIALIZED);
            stream.setStreamRole(StreamRole.STREAM_ROLE_DATA);
            streamMapper.create(stream);
            streamId = stream.getId();
            session.commit();
        }

        ControllerConfig config = Mockito.mock(ControllerConfig.class);
        Mockito.when(config.nodeId()).thenReturn(1);
        Mockito.when(config.scanIntervalInSecs()).thenReturn(1);
        Mockito.when(config.leaseLifeSpanInSecs()).thenReturn(1);

        try (DefaultMetadataStore metadataStore = new DefaultMetadataStore(client, getSessionFactory(), config)) {
            Assertions.assertNull(metadataStore.getLease());
            Lease lease = new Lease();
            lease.setNodeId(config.nodeId());
            metadataStore.setLease(lease);
            metadataStore.setRole(Role.Leader);

            StreamMetadata metadata = metadataStore.openStream(streamId, streamEpoch).get();
            Assertions.assertNotNull(metadata);
            Assertions.assertEquals(streamId, metadata.getStreamId());
            Assertions.assertEquals(0, metadata.getStartOffset());
            Assertions.assertEquals(streamEpoch, metadata.getEpoch());
            Assertions.assertEquals(0, metadata.getRangeId());
            Assertions.assertEquals(StreamState.OPEN, metadata.getState());

            metadataStore.closeStream(streamId, streamEpoch);
        }

        try (SqlSession session = getSessionFactory().openSession()) {
            StreamMapper streamMapper = session.getMapper(StreamMapper.class);
            RangeMapper rangeMapper = session.getMapper(RangeMapper.class);

            Stream stream = streamMapper.getByStreamId(streamId);
            Assertions.assertEquals(streamId, stream.getId());
            Assertions.assertEquals(0, stream.getStartOffset());
            Assertions.assertEquals(streamEpoch, stream.getEpoch());
            Assertions.assertEquals(0, stream.getRangeId());
            Assertions.assertEquals(StreamState.CLOSED, stream.getState());

            Range range = rangeMapper.get(stream.getRangeId(), streamId, null);
            Assertions.assertEquals(0, range.getRangeId());
            Assertions.assertEquals(streamId, range.getStreamId());
            Assertions.assertEquals(streamEpoch, range.getEpoch());
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
            stream.setStartOffset(1234);
            streamMapper.create(stream);
            streamId = stream.getId();

            Range range = new Range();
            range.setRangeId(0);
            range.setStreamId(streamId);
            range.setEpoch(1L);
            range.setStartOffset(1234L);
            range.setEndOffset(2345L);
            range.setBrokerId(1);
            rangeMapper.create(range);
            session.commit();
        }

        ControllerConfig config = Mockito.mock(ControllerConfig.class);
        Mockito.when(config.nodeId()).thenReturn(1);
        Mockito.when(config.scanIntervalInSecs()).thenReturn(1);
        Mockito.when(config.leaseLifeSpanInSecs()).thenReturn(1);

        try (DefaultMetadataStore metadataStore = new DefaultMetadataStore(client, getSessionFactory(), config)) {
            Assertions.assertNull(metadataStore.getLease());
            Lease lease = new Lease();
            lease.setNodeId(config.nodeId());
            metadataStore.setLease(lease);
            metadataStore.setRole(Role.Leader);

            StreamMetadata metadata = metadataStore.openStream(streamId, streamEpoch).get();
            Assertions.assertNotNull(metadata);
            Assertions.assertEquals(streamId, metadata.getStreamId());
            Assertions.assertEquals(1234, metadata.getStartOffset());
            Assertions.assertEquals(streamEpoch, metadata.getEpoch());
            Assertions.assertEquals(1, metadata.getRangeId());
            Assertions.assertEquals(StreamState.OPEN, metadata.getState());

            metadataStore.closeStream(streamId, streamEpoch);
        }

        try (SqlSession session = getSessionFactory().openSession()) {
            StreamMapper streamMapper = session.getMapper(StreamMapper.class);
            RangeMapper rangeMapper = session.getMapper(RangeMapper.class);

            Stream stream = streamMapper.getByStreamId(streamId);
            Assertions.assertEquals(streamId, stream.getId());
            Assertions.assertEquals(1234, stream.getStartOffset());
            Assertions.assertEquals(streamEpoch, stream.getEpoch());
            Assertions.assertEquals(1, stream.getRangeId());
            Assertions.assertEquals(StreamState.CLOSED, stream.getState());

            Range range = rangeMapper.get(stream.getRangeId(), streamId, null);
            Assertions.assertEquals(1, range.getRangeId());
            Assertions.assertEquals(streamId, range.getStreamId());
            Assertions.assertEquals(streamEpoch, range.getEpoch());
            Assertions.assertEquals(2345, range.getStartOffset());
            Assertions.assertEquals(2345, range.getEndOffset());

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
            stream.setState(StreamState.CLOSED);
            stream.setStreamRole(StreamRole.STREAM_ROLE_DATA);
            stream.setStartOffset(1234);
            streamMapper.create(stream);
            dataStreamId = stream.getId();

            stream.setState(StreamState.CLOSED);
            stream.setStreamRole(StreamRole.STREAM_ROLE_OPS);
            stream.setStartOffset(1234);
            streamMapper.create(stream);
            opsStreamId = stream.getId();

            stream.setState(StreamState.CLOSED);
            stream.setStreamRole(StreamRole.STREAM_ROLE_RETRY);
            stream.setGroupId(3L);
            stream.setStartOffset(1234);
            streamMapper.create(stream);
            retryStreamId = stream.getId();
            session.commit();
        }

        ControllerConfig config = Mockito.mock(ControllerConfig.class);
        Mockito.when(config.nodeId()).thenReturn(1);
        Mockito.when(config.scanIntervalInSecs()).thenReturn(1);
        Mockito.when(config.leaseLifeSpanInSecs()).thenReturn(1);

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

        ControllerConfig config = Mockito.mock(ControllerConfig.class);
        Mockito.when(config.nodeId()).thenReturn(1);
        Mockito.when(config.scanIntervalInSecs()).thenReturn(1);
        Mockito.when(config.leaseLifeSpanInSecs()).thenReturn(2);
        Mockito.when(config.nodeAliveIntervalInSecs()).thenReturn(10);
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

        ControllerConfig config = Mockito.mock(ControllerConfig.class);
        Mockito.when(config.nodeId()).thenReturn(4);
        Mockito.when(config.scanIntervalInSecs()).thenReturn(1);
        Mockito.when(config.leaseLifeSpanInSecs()).thenReturn(2);
        Mockito.when(config.nodeAliveIntervalInSecs()).thenReturn(10);
        try (DefaultMetadataStore metadataStore = new DefaultMetadataStore(null, getSessionFactory(), config)) {
            metadataStore.start();
            Awaitility.await().with().atMost(3, TimeUnit.SECONDS).pollInterval(100, TimeUnit.MILLISECONDS)
                .until(metadataStore::isLeader);
            List<StreamMetadata> metadataList = metadataStore.listOpenStreams(4, 1L).get();
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
            range.setBrokerId(nodeId);
            rangeMapper.create(range);

            session.commit();
        }

        ControllerConfig config = Mockito.mock(ControllerConfig.class);
        Mockito.when(config.nodeId()).thenReturn(nodeId);
        Mockito.when(config.scanIntervalInSecs()).thenReturn(1);
        Mockito.when(config.leaseLifeSpanInSecs()).thenReturn(2);
        Mockito.when(config.nodeAliveIntervalInSecs()).thenReturn(10);
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
            Assertions.assertEquals(nodeId, range.getBrokerId());
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
            range.setBrokerId(nodeId);
            rangeMapper.create(range);

            session.commit();
        }


        ControllerConfig config = Mockito.mock(ControllerConfig.class);
        Mockito.when(config.nodeId()).thenReturn(nodeId);
        Mockito.when(config.scanIntervalInSecs()).thenReturn(1);
        Mockito.when(config.leaseLifeSpanInSecs()).thenReturn(2);
        Mockito.when(config.nodeAliveIntervalInSecs()).thenReturn(10);

        try (DefaultMetadataStore metadataStore = new DefaultMetadataStore(client, getSessionFactory(), config)) {
            Assertions.assertNull(metadataStore.getLease());
            Lease lease = new Lease();
            lease.setNodeId(config.nodeId());
            metadataStore.setLease(lease);
            metadataStore.setRole(Role.Leader);
            List<StreamMetadata> streams = metadataStore.listOpenStreams(nodeId, 0).get();

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
            range.setBrokerId(nodeId);
            rangeMapper.create(range);

            session.commit();
        }


        ControllerConfig config = Mockito.mock(ControllerConfig.class);
        Mockito.when(config.nodeId()).thenReturn(nodeId);
        Mockito.when(config.scanIntervalInSecs()).thenReturn(1);
        Mockito.when(config.leaseLifeSpanInSecs()).thenReturn(2);
        Mockito.when(config.nodeAliveIntervalInSecs()).thenReturn(10);

        try (DefaultMetadataStore metadataStore = new DefaultMetadataStore(client, getSessionFactory(), config)) {
            Assertions.assertNull(metadataStore.getLease());
            Lease lease = new Lease();
            lease.setNodeId(config.nodeId());
            metadataStore.setLease(lease);
            metadataStore.setRole(Role.Leader);
            List<StreamMetadata> streams = metadataStore.listOpenStreams(nodeId, 0).get();

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
        int nodeId = 1;

        ControllerConfig config = Mockito.mock(ControllerConfig.class);
        Mockito.when(config.nodeId()).thenReturn(nodeId);
        Mockito.when(config.scanIntervalInSecs()).thenReturn(1);
        Mockito.when(config.leaseLifeSpanInSecs()).thenReturn(2);
        Mockito.when(config.nodeAliveIntervalInSecs()).thenReturn(10);
        long time = System.currentTimeMillis();
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
                if (object.getPreparedTimestamp() - time > 5 * 60) {
                    Assertions.fail();
                }
                if (object.getExpiredTimestamp() - time - 5 * 60 * 1000 > 5 * 60) {
                    Assertions.fail();
                }
            }
        }
    }

    @Test
    public void testCommitStreamObject() throws IOException, ControllerException {
        long objectId, streamId = 1;
        int nodeId = 1;

        ControllerConfig config = Mockito.mock(ControllerConfig.class);
        Mockito.when(config.nodeId()).thenReturn(nodeId);
        Mockito.when(config.scanIntervalInSecs()).thenReturn(1);
        Mockito.when(config.leaseLifeSpanInSecs()).thenReturn(2);
        Mockito.when(config.nodeAliveIntervalInSecs()).thenReturn(10);

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

        S3StreamObject s3StreamObject = S3StreamObject.newBuilder()
            .setObjectId(objectId + 2)
            .setStreamId(streamId)
            .setObjectSize(111L)
            .build();

        try (SqlSession session = this.getSessionFactory().openSession()) {
            S3StreamObjectMapper s3StreamObjectMapper = session.getMapper(S3StreamObjectMapper.class);
            com.automq.rocketmq.controller.metadata.database.dao.S3StreamObject object = new com.automq.rocketmq.controller.metadata.database.dao.S3StreamObject();
            object.setObjectId(objectId);
            object.setStreamId(streamId);
            object.setBaseDataTimestamp(1);
            s3StreamObjectMapper.create(object);

            com.automq.rocketmq.controller.metadata.database.dao.S3StreamObject object1 = new com.automq.rocketmq.controller.metadata.database.dao.S3StreamObject();
            object1.setObjectId(objectId + 1);
            object1.setStreamId(streamId);
            object1.setBaseDataTimestamp(2);
            s3StreamObjectMapper.create(object1);

            session.commit();
        }
        long time = System.currentTimeMillis();
        List<Long> compactedObjects = new ArrayList<>();
        compactedObjects.add(objectId);
        compactedObjects.add(objectId + 1);
        try (DefaultMetadataStore metadataStore = new DefaultMetadataStore(client, getSessionFactory(), config)) {
            Assertions.assertNull(metadataStore.getLease());
            Lease lease = new Lease();
            lease.setNodeId(config.nodeId());
            metadataStore.setLease(lease);
            metadataStore.setRole(Role.Leader);

            metadataStore.commitStreamObject(s3StreamObject, compactedObjects);
        }


        try (SqlSession session = getSessionFactory().openSession()) {
            S3ObjectMapper s3ObjectMapper = session.getMapper(S3ObjectMapper.class);
            for (long index = objectId; index < objectId + 2; index++) {
                S3Object object = s3ObjectMapper.getById(index);
                Assertions.assertEquals(S3ObjectState.BOS_DELETED, object.getState());
                if (object.getMarkedForDeletionTimestamp() - time > 5 * 60) {
                    Assertions.fail();
                }
            }


            S3StreamObjectMapper s3StreamObjectMapper = session.getMapper(S3StreamObjectMapper.class);
            for (long index = objectId; index < objectId + 2; index++) {
                com.automq.rocketmq.controller.metadata.database.dao.S3StreamObject object = s3StreamObjectMapper.getByObjectId(index);
                Assertions.assertNull(object);
            }

            com.automq.rocketmq.controller.metadata.database.dao.S3StreamObject object = s3StreamObjectMapper.getByObjectId(objectId + 2);
            Assertions.assertEquals(1L, object.getBaseDataTimestamp());
            if (object.getCommittedTimestamp() - time > 5 * 60) {
                Assertions.fail();
            }
        }
    }

    @Test
    public void testCommitWALObject() throws IOException, ExecutionException, InterruptedException, ControllerException {
        long objectId, streamId = 1;
        int nodeId = 1;

        ControllerConfig config = Mockito.mock(ControllerConfig.class);
        Mockito.when(config.nodeId()).thenReturn(nodeId);
        Mockito.when(config.scanIntervalInSecs()).thenReturn(1);
        Mockito.when(config.leaseLifeSpanInSecs()).thenReturn(2);
        Mockito.when(config.nodeAliveIntervalInSecs()).thenReturn(10);

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
            .setSequenceId(11)
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
            S3StreamObjectMapper s3StreamObjectMapper = session.getMapper(S3StreamObjectMapper.class);
            com.automq.rocketmq.controller.metadata.database.dao.S3StreamObject object = new com.automq.rocketmq.controller.metadata.database.dao.S3StreamObject();
            object.setObjectId(objectId);
            object.setStreamId(streamId);
            object.setBaseDataTimestamp(1);
            object.setStartOffset(111);
            object.setEndOffset(222);
            s3StreamObjectMapper.create(object);

            com.automq.rocketmq.controller.metadata.database.dao.S3StreamObject object1 = new com.automq.rocketmq.controller.metadata.database.dao.S3StreamObject();
            object1.setObjectId(objectId + 1);
            object1.setStreamId(streamId);
            object1.setBaseDataTimestamp(2);
            object1.setStartOffset(222);
            object1.setEndOffset(333);
            s3StreamObjectMapper.create(object1);

            S3WALObjectMapper s3WALObjectMapper = session.getMapper(S3WALObjectMapper.class);
            com.automq.rocketmq.controller.metadata.database.dao.S3WALObject s3WALObject = new com.automq.rocketmq.controller.metadata.database.dao.S3WALObject();
            s3WALObject.setObjectId(objectId + 2);
            s3WALObject.setObjectSize(333);
            s3WALObject.setBaseDataTimestamp(3);
            s3WALObject.setSequenceId(1);
            s3WALObject.setSubStreams(expectSubStream.replace("1234567890", String.valueOf(streamId)));
            s3WALObjectMapper.create(s3WALObject);

            com.automq.rocketmq.controller.metadata.database.dao.S3WALObject s3WALObject1 = new com.automq.rocketmq.controller.metadata.database.dao.S3WALObject();
            s3WALObject1.setObjectId(objectId + 3);
            s3WALObject1.setObjectSize(444);
            s3WALObject1.setBaseDataTimestamp(4);
            s3WALObject.setSequenceId(2);
            s3WALObject1.setSubStreams(expectSubStream.replace("1234567890", String.valueOf(streamId)));
            s3WALObjectMapper.create(s3WALObject1);

            session.commit();
        }


        long time = System.currentTimeMillis();
        List<Long> compactedObjects = new ArrayList<>();
        compactedObjects.add(objectId + 2);
        compactedObjects.add(objectId + 3);
        try (DefaultMetadataStore metadataStore = new DefaultMetadataStore(client, getSessionFactory(), config)) {
            Assertions.assertNull(metadataStore.getLease());
            Lease lease = new Lease();
            lease.setNodeId(config.nodeId());
            metadataStore.setLease(lease);
            metadataStore.setRole(Role.Leader);

            List<S3StreamObject> s3StreamObjects = metadataStore.listStreamObjects(streamId, 222, 111, 2).get();

            metadataStore.commitWalObject(walObject, s3StreamObjects, compactedObjects);
        }


        try (SqlSession session = getSessionFactory().openSession()) {
            S3ObjectMapper s3ObjectMapper = session.getMapper(S3ObjectMapper.class);
            for (long index = objectId; index < objectId + 2; index++) {
                S3Object object = s3ObjectMapper.getById(index);
                Assertions.assertEquals(S3ObjectState.BOS_COMMITTED, object.getState());
                if (object.getCommittedTimestamp() - time > 5 * 60) {
                    Assertions.fail();
                }
            }

            for (long index = objectId + 2; index < objectId + 4; index++) {
                S3Object object = s3ObjectMapper.getById(index);
                Assertions.assertEquals(S3ObjectState.BOS_DELETED, object.getState());
                if (object.getMarkedForDeletionTimestamp() - time > 5 * 60) {
                    Assertions.fail();
                }
            }


            S3StreamObjectMapper s3StreamObjectMapper = session.getMapper(S3StreamObjectMapper.class);
            for (long index = objectId; index < objectId + 2; index++) {
                com.automq.rocketmq.controller.metadata.database.dao.S3StreamObject object = s3StreamObjectMapper.getByObjectId(index);
                if (object.getCommittedTimestamp() - time > 5 * 60) {
                    Assertions.fail();
                }
            }

            long baseTime = time;
            S3WALObjectMapper s3WALObjectMapper = session.getMapper(S3WALObjectMapper.class);
            for (long index = objectId + 2; index < objectId + 4; index++) {
                com.automq.rocketmq.controller.metadata.database.dao.S3WALObject s3WALObject = s3WALObjectMapper.getByObjectId(index);
                baseTime = Math.min(baseTime, s3WALObject.getBaseDataTimestamp());
            }

            com.automq.rocketmq.controller.metadata.database.dao.S3WALObject object = s3WALObjectMapper.getByObjectId(objectId + 4);
            Assertions.assertEquals(baseTime, object.getBaseDataTimestamp());
            if (object.getCommittedTimestamp() - time > 5 * 60) {
                Assertions.fail();
            }
        }
    }
}