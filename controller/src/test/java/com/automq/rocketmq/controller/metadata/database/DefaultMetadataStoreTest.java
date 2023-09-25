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

import com.automq.rocketmq.controller.exception.ControllerException;
import com.automq.rocketmq.controller.metadata.ControllerClient;
import com.automq.rocketmq.controller.metadata.ControllerConfig;
import com.automq.rocketmq.controller.metadata.DatabaseTestBase;
import com.automq.rocketmq.controller.metadata.MetadataStore;
import com.automq.rocketmq.controller.metadata.database.dao.Node;
import com.automq.rocketmq.controller.metadata.database.dao.QueueAssignment;
import com.automq.rocketmq.controller.metadata.database.dao.QueueAssignmentStatus;
import com.automq.rocketmq.controller.metadata.database.dao.Topic;
import com.automq.rocketmq.controller.metadata.database.dao.TopicStatus;
import com.automq.rocketmq.controller.metadata.database.mapper.NodeMapper;
import com.automq.rocketmq.controller.metadata.database.mapper.QueueAssignmentMapper;
import com.automq.rocketmq.controller.metadata.database.mapper.TopicMapper;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;
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
        try (MetadataStore metadataStore = new DefaultMetadataStore(client, getSessionFactory(), config)) {
            metadataStore.start();
            Awaitility.await().with().atMost(10, TimeUnit.SECONDS)
                .pollInterval(100, TimeUnit.MILLISECONDS)
                .until(metadataStore::isLeader);

            String topicName = "t1";
            int queueNum = 4;
            long topicId = metadataStore.createTopic(topicName, queueNum);

            try (SqlSession session = getSessionFactory().openSession()) {
                TopicMapper mapper = session.getMapper(TopicMapper.class);
                mapper.delete(topicId);

                NodeMapper nodeMapper = session.getMapper(NodeMapper.class);
                nodeMapper.delete(nodeId);

                QueueAssignmentMapper assignmentMapper = session.getMapper(QueueAssignmentMapper.class);
                assignmentMapper.delete(null);

                session.commit();
            }
        }
    }

    @Test
    public void testListAssignments() throws IOException {
        try (SqlSession session = getSessionFactory().openSession()) {
            QueueAssignmentMapper mapper = session.getMapper(QueueAssignmentMapper.class);
            QueueAssignment assignment = new QueueAssignment();
            assignment.setTopicId(1);
            assignment.setStatus(QueueAssignmentStatus.ASSIGNED);
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
            Assertions.assertEquals(QueueAssignmentStatus.ASSIGNED, assignment.getStatus());
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
            assignment.setStatus(QueueAssignmentStatus.ASSIGNED);
            assignment.setQueueId(1);
            assignment.setDstNodeId(2);
            assignment.setSrcNodeId(3);
            assignmentMapper.create(assignment);

            assignment = new QueueAssignment();
            assignment.setTopicId(topicId);
            assignment.setStatus(QueueAssignmentStatus.YIELDING);
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
}