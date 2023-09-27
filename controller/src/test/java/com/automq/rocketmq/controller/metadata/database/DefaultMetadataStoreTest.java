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

import apache.rocketmq.controller.v1.S3StreamObject;
import apache.rocketmq.controller.v1.S3WALObject;
import apache.rocketmq.controller.v1.StreamMetadata;
import apache.rocketmq.controller.v1.StreamState;
import apache.rocketmq.controller.v1.SubStream;
import com.automq.rocketmq.controller.exception.ControllerException;
import com.automq.rocketmq.controller.metadata.ControllerClient;
import com.automq.rocketmq.controller.metadata.ControllerConfig;
import com.automq.rocketmq.controller.metadata.DatabaseTestBase;
import com.automq.rocketmq.controller.metadata.MetadataStore;
import com.automq.rocketmq.controller.metadata.Role;
import com.automq.rocketmq.controller.metadata.database.dao.Lease;
import com.automq.rocketmq.controller.metadata.database.dao.Node;
import com.automq.rocketmq.controller.metadata.database.dao.QueueAssignment;
import com.automq.rocketmq.controller.metadata.database.dao.AssignmentStatus;
import com.automq.rocketmq.controller.metadata.database.dao.Stream;
import com.automq.rocketmq.controller.metadata.database.dao.Range;
import com.automq.rocketmq.controller.metadata.database.dao.StreamRole;
import com.automq.rocketmq.controller.metadata.database.dao.Topic;
import com.automq.rocketmq.controller.metadata.database.dao.TopicStatus;
import com.automq.rocketmq.controller.metadata.database.mapper.NodeMapper;
import com.automq.rocketmq.controller.metadata.database.mapper.QueueAssignmentMapper;
import com.automq.rocketmq.controller.metadata.database.mapper.RangeMapper;
import com.automq.rocketmq.controller.metadata.database.mapper.S3StreamObjectMapper;
import com.automq.rocketmq.controller.metadata.database.mapper.S3WALObjectMapper;
import com.automq.rocketmq.controller.metadata.database.mapper.StreamMapper;
import com.automq.rocketmq.controller.metadata.database.mapper.TopicMapper;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
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
    void testRegisterNode() throws ControllerException, IOException {
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
            Node node = metadataStore.registerBrokerNode(name, address, instanceId);
            Assertions.assertTrue(node.getId() > 0);
        }
    }

    @Test
    void testRegisterBroker_badArguments() throws IOException {
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
     *
     * @throws IOException
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
    void testCreateTopic() throws ControllerException, IOException {
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

            topicId = metadataStore.createTopic(topicName, queueNum);
        }

        try (SqlSession session = getSessionFactory().openSession()) {
            TopicMapper topicMapper = session.getMapper(TopicMapper.class);
            List<Topic> topics = topicMapper.list(null, null);
            topics.stream().filter(topic -> topic.getName().equals("t1")).forEach(topic -> {
                Assertions.assertEquals(4, topic.getQueueNum());
            });

            QueueAssignmentMapper assignmentMapper = session.getMapper(QueueAssignmentMapper.class);
            List<QueueAssignment> assignments = assignmentMapper.list(topicId, null, null, null, null);
            Assertions.assertEquals(4, assignments.size());

            StreamMapper streamMapper = session.getMapper(StreamMapper.class);
            List<Stream> streams = streamMapper.list(topicId, null, null);
            Assertions.assertEquals(queueNum * 2, streams.size());
        }
    }

    @Test
    public void testListAssignments() throws IOException {
        try (SqlSession session = getSessionFactory().openSession()) {
            QueueAssignmentMapper mapper = session.getMapper(QueueAssignmentMapper.class);
            QueueAssignment assignment = new QueueAssignment();
            assignment.setTopicId(1);
            assignment.setStatus(AssignmentStatus.ASSIGNED);
            assignment.setDstNodeId(2);
            assignment.setSrcNodeId(3);
            assignment.setQueueId(4);
            int affectedRows = mapper.create(assignment);
            Assertions.assertEquals(1, affectedRows);
            session.commit();
        }

        ControllerConfig config = Mockito.mock(ControllerConfig.class);
        try (MetadataStore metadataStore = new DefaultMetadataStore(client, getSessionFactory(), config)) {
            List<QueueAssignment> assignmentList = metadataStore.listAssignments(null, null, null, null);
            Assertions.assertEquals(1, assignmentList.size());
            QueueAssignment assignment = assignmentList.get(0);
            Assertions.assertEquals(1, assignment.getTopicId());
            Assertions.assertEquals(AssignmentStatus.ASSIGNED, assignment.getStatus());
            Assertions.assertEquals(2, assignment.getDstNodeId());
            Assertions.assertEquals(3, assignment.getSrcNodeId());
            Assertions.assertEquals(4, assignment.getQueueId());
        }
    }

    @Test
    public void testDeleteTopic() throws IOException {
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
            topic.setStatus(TopicStatus.ACTIVE);
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

            metadataStore.deleteTopic(topicId);
        } catch (ControllerException e) {
            Assertions.fail(e);
        }
    }

    @Test
    public void testDeleteTopic_NotFound() throws IOException {
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
    public void testDescribeTopic() throws IOException, ControllerException {
        long topicId;
        try (SqlSession session = getSessionFactory().openSession()) {

            TopicMapper topicMapper = session.getMapper(TopicMapper.class);
            Topic topic = new Topic();
            topic.setName("T1");
            topic.setStatus(TopicStatus.ACTIVE);
            topicMapper.create(topic);
            topicId = topic.getId();

            QueueAssignmentMapper assignmentMapper = session.getMapper(QueueAssignmentMapper.class);
            QueueAssignment assignment = new QueueAssignment();
            assignment.setTopicId(topicId);
            assignment.setStatus(AssignmentStatus.ASSIGNED);
            assignment.setQueueId(1);
            assignment.setDstNodeId(2);
            assignment.setSrcNodeId(3);
            assignmentMapper.create(assignment);

            assignment = new QueueAssignment();
            assignment.setTopicId(topicId);
            assignment.setStatus(AssignmentStatus.YIELDING);
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
            apache.rocketmq.controller.v1.Topic topic = metadataStore.describeTopic(topicId, null);
            Assertions.assertEquals("T1", topic.getName());
            Assertions.assertEquals(1, topic.getAssignmentsCount());
            Assertions.assertEquals(1, topic.getReassignmentsCount());
        }
    }

    @Test
    public void testMarkMessageQueueAssignable() throws IOException, ControllerException {
        try (SqlSession session = getSessionFactory().openSession()) {
            QueueAssignmentMapper assignmentMapper = session.getMapper(QueueAssignmentMapper.class);
            QueueAssignment assignment = new QueueAssignment();
            assignment.setQueueId(1);
            assignment.setTopicId(2);
            assignment.setStatus(AssignmentStatus.YIELDING);
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

            List<QueueAssignment> assignments = metadataStore.listAssignments(2L, null, null, null);
            for (QueueAssignment assignment : assignments) {
                if (assignment.getQueueId() == 1) {
                    Assertions.assertEquals(AssignmentStatus.ASSIGNABLE, assignment.getStatus());
                }
            }
        }
    }

    @Test
    public void testListStreamObjects() throws IOException {
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
            List<S3StreamObject> s3StreamObjects = metadataStore.listStreamObjects(streamId, startOffset, endOffset, limit);
            S3StreamObject s3StreamObject = s3StreamObjects.get(0);
            Assertions.assertEquals(11, s3StreamObject.getObjectId());
            Assertions.assertEquals(123, s3StreamObject.getObjectSize());
            Assertions.assertEquals(111, s3StreamObject.getStreamId());
            Assertions.assertEquals(1234, s3StreamObject.getStartOffset());
            Assertions.assertEquals(2345, s3StreamObject.getEndOffset());
        }
    }

    @Test
    public void testListWALObjects_WithPrams() throws IOException {
        long streamId, startOffset, endOffset;
        streamId = 1234567890;
        startOffset = 0L;
        endOffset = 9L;
        int limit = 1;
        Gson gson = new Gson();
        String subStreamsJson = "{\n" +
            "  \"1234567890\": {\n" +
            "    \"streamId_\": 1234567890,\n" +
            "    \"startOffset_\": 0,\n" +
            "    \"endOffset_\": 10\n" +
            "  },\n" +
            "  \"9876543210\": {\n" +
            "    \"streamId_\": 9876543210,\n" +
            "    \"startOffset_\": 5,\n" +
            "    \"endOffset_\": 15\n" +
            "  },\n" +
            "  \"5678901234\": {\n" +
            "    \"streamId_\": 5678901234,\n" +
            "    \"startOffset_\": 2,\n" +
            "    \"endOffset_\": 8\n" +
            "  },\n" +
            "  \"4321098765\": {\n" +
            "    \"streamId_\": 4321098765,\n" +
            "    \"startOffset_\": 7,\n" +
            "    \"endOffset_\": 12\n" +
            "  }\n" +
            "}";
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
        String expectSubStream = "{\n" +
            "  \"1234567890\": {\n" +
            "    \"streamId_\": 1234567890,\n" +
            "    \"startOffset_\": 0,\n" +
            "    \"endOffset_\": 10\n" +
            "  }" +
            "}";
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
            List<S3WALObject> s3WALObjects = metadataStore.listWALObjects(streamId, startOffset, endOffset, limit);

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
    public void testListWALObjects_NotParms() throws IOException {
        Gson gson = new Gson();
        String subStreamsJson = "{\n" +
            "  \"1234567890\": {\n" +
            "    \"streamId_\": 1234567890,\n" +
            "    \"startOffset_\": 0,\n" +
            "    \"endOffset_\": 10\n" +
            "  },\n" +
            "  \"9876543210\": {\n" +
            "    \"streamId_\": 9876543210,\n" +
            "    \"startOffset_\": 5,\n" +
            "    \"endOffset_\": 15\n" +
            "  },\n" +
            "  \"5678901234\": {\n" +
            "    \"streamId_\": 5678901234,\n" +
            "    \"startOffset_\": 2,\n" +
            "    \"endOffset_\": 8\n" +
            "  },\n" +
            "  \"4321098765\": {\n" +
            "    \"streamId_\": 4321098765,\n" +
            "    \"startOffset_\": 7,\n" +
            "    \"endOffset_\": 12\n" +
            "  }\n" +
            "}";
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
        String expectSubStream = subStreamsJson;
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
            List<S3WALObject> s3WALObjects = metadataStore.listWALObjects();

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
    public void testOpenStream_WithCloseStream_AtStart() throws IOException, ControllerException {
        long streamEpoch = 0;
        long streamId;
        try (SqlSession session = this.getSessionFactory().openSession()) {
            StreamMapper streamMapper = session.getMapper(StreamMapper.class);
            Stream stream = new Stream();
            stream.setRangeId(-1);
            stream.setState(StreamState.UNINITIALIZED);
            stream.setStreamRole(StreamRole.DATA);
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

            StreamMetadata metadata = metadataStore.openStream(streamId, streamEpoch);
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
    public void testOpenStream_WithClosedStream() throws IOException, ControllerException {
        long streamId, streamEpoch = 1;
        try (SqlSession session = this.getSessionFactory().openSession()) {
            StreamMapper streamMapper = session.getMapper(StreamMapper.class);
            RangeMapper rangeMapper = session.getMapper(RangeMapper.class);

            Stream stream = new Stream();
            stream.setRangeId(0);
            stream.setState(StreamState.CLOSED);
            stream.setStreamRole(StreamRole.DATA);
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

            StreamMetadata metadata = metadataStore.openStream(streamId, streamEpoch);
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
    public void testTrimStream() throws IOException {
        long streamId,streamEpoch = 1, newStartOffset = 2000;
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
            stream.setStreamRole(StreamRole.DATA);
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
        } catch (ControllerException e) {
            Assertions.fail(e);
        }

        try (SqlSession session = this.getSessionFactory().openSession()) {
            StreamMapper streamMapper = session.getMapper(StreamMapper.class);
            Stream stream = streamMapper.getByStreamId(streamId);
            Assertions.assertEquals(newStartOffset, stream.getStartOffset());
            Assertions.assertEquals(nodeId, stream.getSrcNodeId());
            Assertions.assertEquals(nodeId, stream.getDstNodeId());
            Assertions.assertEquals(0, stream.getRangeId());

            RangeMapper rangeMapper = session.getMapper(RangeMapper.class);
            Range range = rangeMapper.getByRangeId(rangId);
            Assertions.assertEquals(newStartOffset, range.getStartOffset());
            Assertions.assertEquals(2345, range.getEndOffset());
            Assertions.assertEquals(nodeId, range.getBrokerId());
            Assertions.assertEquals(streamId, range.getStreamId());
        }

    }
}