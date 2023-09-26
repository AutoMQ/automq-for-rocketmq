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

package com.automq.rocketmq.controller;

import apache.rocketmq.controller.v1.Code;
import apache.rocketmq.controller.v1.ControllerServiceGrpc;
import apache.rocketmq.controller.v1.CreateGroupReply;
import apache.rocketmq.controller.v1.CreateGroupRequest;
import apache.rocketmq.controller.v1.GroupType;
import apache.rocketmq.controller.v1.HeartbeatReply;
import apache.rocketmq.controller.v1.HeartbeatRequest;
import apache.rocketmq.controller.v1.ListTopicsReply;
import apache.rocketmq.controller.v1.ListTopicsRequest;
import apache.rocketmq.controller.v1.NodeRegistrationReply;
import apache.rocketmq.controller.v1.NodeRegistrationRequest;
import com.automq.rocketmq.controller.exception.ControllerException;
import com.automq.rocketmq.controller.metadata.ControllerClient;
import com.automq.rocketmq.controller.metadata.ControllerConfig;
import com.automq.rocketmq.controller.metadata.DatabaseTestBase;
import com.automq.rocketmq.controller.metadata.GrpcControllerClient;
import com.automq.rocketmq.controller.metadata.MetadataStore;
import com.automq.rocketmq.controller.metadata.database.DefaultMetadataStore;
import com.automq.rocketmq.controller.metadata.database.dao.Group;
import com.automq.rocketmq.controller.metadata.database.dao.GroupProgress;
import com.automq.rocketmq.controller.metadata.database.dao.Node;
import com.automq.rocketmq.controller.metadata.database.dao.QueueAssignment;
import com.automq.rocketmq.controller.metadata.database.dao.AssignmentStatus;
import com.automq.rocketmq.controller.metadata.database.dao.StreamAffiliation;
import com.automq.rocketmq.controller.metadata.database.dao.Topic;
import com.automq.rocketmq.controller.metadata.database.dao.TopicStatus;
import com.automq.rocketmq.controller.metadata.database.mapper.GroupMapper;
import com.automq.rocketmq.controller.metadata.database.mapper.GroupProgressMapper;
import com.automq.rocketmq.controller.metadata.database.mapper.NodeMapper;
import com.automq.rocketmq.controller.metadata.database.mapper.QueueAssignmentMapper;
import com.automq.rocketmq.controller.metadata.database.mapper.StreamAffiliationMapper;
import com.automq.rocketmq.controller.metadata.database.mapper.TopicMapper;
import io.grpc.Grpc;
import io.grpc.InsecureChannelCredentials;
import io.grpc.ManagedChannel;
import io.grpc.stub.StreamObserver;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.ibatis.session.SqlSession;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class ControllerServiceImplTest extends DatabaseTestBase {

    @Test
    public void testRegisterBrokerNode() throws IOException {
        ControllerConfig config = Mockito.mock(ControllerConfig.class);
        Mockito.when(config.scanIntervalInSecs()).thenReturn(1);
        Mockito.when(config.nodeId()).thenReturn(1);
        Mockito.when(config.leaseLifeSpanInSecs()).thenReturn(1);

        ControllerClient client = Mockito.mock(ControllerClient.class);

        try (DefaultMetadataStore metadataStore = new DefaultMetadataStore(client, getSessionFactory(), config)) {
            metadataStore.start();
            ControllerServiceImpl svc = new ControllerServiceImpl(metadataStore);
            Awaitility.await().with().pollInterval(100, TimeUnit.MILLISECONDS)
                .atMost(10, TimeUnit.SECONDS)
                .until(metadataStore::isLeader);
            NodeRegistrationRequest request = NodeRegistrationRequest.newBuilder()
                .setBrokerName("broker-name")
                .setAddress("localhost:1234")
                .setInstanceId("i-reg-broker")
                .build();

            AtomicInteger nodeId = new AtomicInteger(0);

            StreamObserver<NodeRegistrationReply> observer = new StreamObserver<>() {
                @Override
                public void onNext(NodeRegistrationReply reply) {
                    nodeId.set(reply.getId());
                }

                @Override
                public void onError(Throwable t) {
                    Assertions.fail(t);
                }

                @Override
                public void onCompleted() {

                }
            };

            svc.registerNode(request, observer);

            // It should work if register the broker multiple times. The only side effect is epoch is incremented.
            svc.registerNode(request, observer);

            try (SqlSession session = getSessionFactory().openSession()) {
                NodeMapper mapper = session.getMapper(NodeMapper.class);
                mapper.delete(nodeId.get());
                session.commit();
            }
        }
    }

    @Test
    public void testRegisterBroker_BadRequest() throws IOException {
        ControllerConfig config = Mockito.mock(ControllerConfig.class);
        Mockito.when(config.scanIntervalInSecs()).thenReturn(1);
        Mockito.when(config.nodeId()).thenReturn(1);
        Mockito.when(config.leaseLifeSpanInSecs()).thenReturn(1);

        ControllerClient client = Mockito.mock(ControllerClient.class);

        try (DefaultMetadataStore metadataStore = new DefaultMetadataStore(client, getSessionFactory(), config)) {
            metadataStore.start();
            ControllerServiceImpl svc = new ControllerServiceImpl(metadataStore);
            Awaitility.await().with().pollInterval(100, TimeUnit.MILLISECONDS)
                .atMost(10, TimeUnit.SECONDS)
                .until(metadataStore::isLeader);
            NodeRegistrationRequest request = NodeRegistrationRequest.newBuilder()
                .setBrokerName("")
                .setAddress("localhost:1234")
                .setInstanceId("i-reg-broker")
                .build();

            StreamObserver<NodeRegistrationReply> observer = new StreamObserver<>() {
                @Override
                public void onNext(NodeRegistrationReply value) {
                    Assertions.fail("Should have raised an exception");
                }

                @Override
                public void onError(Throwable t) {
                    // OK, it's expected.
                }

                @Override
                public void onCompleted() {
                    Assertions.fail("Should have raised an exception");
                }
            };
            svc.registerNode(request, observer);
        }
    }

    @Test
    public void testHeartbeatGrpc() throws IOException {
        int nodeId;
        try (SqlSession session = getSessionFactory().openSession()) {
            NodeMapper mapper = session.getMapper(NodeMapper.class);
            Node node = new Node();
            node.setName("b1");
            node.setVolumeId("v-1");
            node.setVpcId("vpc-1");
            node.setAddress("localhost:1234");
            node.setInstanceId("i-1");
            node.setEpoch(1);
            mapper.create(node);
            nodeId = node.getId();
            session.commit();
        }

        ControllerClient controllerClient = Mockito.mock(ControllerClient.class);
        ControllerConfig controllerConfig = Mockito.mock(ControllerConfig.class);
        Mockito.when(controllerConfig.nodeId()).thenReturn(1);
        Mockito.when(controllerConfig.scanIntervalInSecs()).thenReturn(1);
        Mockito.when(controllerConfig.leaseLifeSpanInSecs()).thenReturn(1);
        Mockito.when(controllerConfig.scanIntervalInSecs()).thenReturn(1);
        try (DefaultMetadataStore metadataStore = new DefaultMetadataStore(controllerClient, getSessionFactory(), controllerConfig)) {
            metadataStore.start();
            Awaitility.await().with().pollInterval(100, TimeUnit.MILLISECONDS)
                .atMost(10, TimeUnit.SECONDS)
                .until(metadataStore::isLeader);

            try (ControllerTestServer testServer = new ControllerTestServer(0, new ControllerServiceImpl(metadataStore))) {
                testServer.start();
                int port = testServer.getPort();
                ManagedChannel channel = Grpc.newChannelBuilderForAddress("localhost", port, InsecureChannelCredentials.create()).build();
                ControllerServiceGrpc.ControllerServiceBlockingStub blockingStub = ControllerServiceGrpc.newBlockingStub(channel);
                HeartbeatRequest request = HeartbeatRequest.newBuilder().setId(nodeId).setEpoch(1).build();
                HeartbeatReply reply = blockingStub.heartbeat(request);
                Assertions.assertEquals(Code.OK, reply.getStatus().getCode());
                channel.shutdownNow();
            }
        }
    }

    @Test
    public void testListTopics() throws IOException {
        try (SqlSession session = getSessionFactory().openSession()) {
            TopicMapper topicMapper = session.getMapper(TopicMapper.class);
            for (int i = 0; i < 3; i++) {
                Topic topic = new Topic();
                topic.setStatus(TopicStatus.ACTIVE);
                topic.setName("T" + i);
                topicMapper.create(topic);
            }
            session.commit();
        }

        ControllerClient controllerClient = Mockito.mock(ControllerClient.class);
        ControllerConfig controllerConfig = Mockito.mock(ControllerConfig.class);
        Mockito.when(controllerConfig.nodeId()).thenReturn(1);
        Mockito.when(controllerConfig.scanIntervalInSecs()).thenReturn(1);
        Mockito.when(controllerConfig.leaseLifeSpanInSecs()).thenReturn(2);
        Mockito.when(controllerConfig.scanIntervalInSecs()).thenReturn(1);

        try (DefaultMetadataStore metadataStore = new DefaultMetadataStore(controllerClient, getSessionFactory(), controllerConfig)) {
            metadataStore.start();
            Awaitility.await().with().pollInterval(100, TimeUnit.MILLISECONDS)
                .atMost(10, TimeUnit.SECONDS)
                .until(metadataStore::isLeader);

            try (ControllerTestServer testServer = new ControllerTestServer(0, new ControllerServiceImpl(metadataStore))) {
                testServer.start();
                int port = testServer.getPort();
                ManagedChannel channel = Grpc.newChannelBuilderForAddress("localhost", port, InsecureChannelCredentials.create()).build();
                ControllerServiceGrpc.ControllerServiceBlockingStub blockingStub = ControllerServiceGrpc.newBlockingStub(channel);
                Iterator<ListTopicsReply> replies = blockingStub.listAllTopics(ListTopicsRequest.newBuilder().build());
                while (replies.hasNext()) {
                    ListTopicsReply reply = replies.next();
                    Assertions.assertTrue(reply.getTopic().getName().startsWith("T"));
                    Assertions.assertEquals(Code.OK, reply.getStatus().getCode());
                }
                channel.shutdownNow();
            }
        }
    }

    @Test
    public void testCommitOffset() throws IOException {
        ControllerClient controllerClient = Mockito.mock(ControllerClient.class);
        ControllerConfig controllerConfig = Mockito.mock(ControllerConfig.class);
        Mockito.when(controllerConfig.nodeId()).thenReturn(1);
        Mockito.when(controllerConfig.scanIntervalInSecs()).thenReturn(1);
        Mockito.when(controllerConfig.leaseLifeSpanInSecs()).thenReturn(2);
        Mockito.when(controllerConfig.scanIntervalInSecs()).thenReturn(1);

        try (MetadataStore metadataStore = new DefaultMetadataStore(controllerClient, getSessionFactory(), controllerConfig)) {
            metadataStore.start();
            Awaitility.await().with().pollInterval(100, TimeUnit.MILLISECONDS)
                .atMost(10, TimeUnit.SECONDS)
                .until(metadataStore::isLeader);

            try (ControllerTestServer testServer = new ControllerTestServer(0, new ControllerServiceImpl(metadataStore))) {
                testServer.start();
                int port = testServer.getPort();
                ControllerClient client = new GrpcControllerClient();
                Assertions.assertDoesNotThrow(() -> {
                    client.commitOffset(String.format("localhost:%d", port), 1, 1, 1, 1).get();
                });

                try (SqlSession session = getSessionFactory().openSession()) {
                    GroupProgressMapper mapper = session.getMapper(GroupProgressMapper.class);
                    List<GroupProgress> list = mapper.list(1L, 1L);
                    Assertions.assertFalse(list.isEmpty());
                    for (GroupProgress progress : list) {
                        if (1 == progress.getQueueId()) {
                            Assertions.assertEquals(1, progress.getQueueOffset());
                        }
                    }
                }
            }
        }
    }

    @Test
    public void testCreateGroup() throws IOException, ControllerException, ExecutionException, InterruptedException {
        ControllerClient controllerClient = Mockito.mock(ControllerClient.class);
        ControllerConfig controllerConfig = Mockito.mock(ControllerConfig.class);
        Mockito.when(controllerConfig.nodeId()).thenReturn(1);
        Mockito.when(controllerConfig.scanIntervalInSecs()).thenReturn(1);
        Mockito.when(controllerConfig.leaseLifeSpanInSecs()).thenReturn(2);
        Mockito.when(controllerConfig.scanIntervalInSecs()).thenReturn(1);

        try (MetadataStore metadataStore = new DefaultMetadataStore(controllerClient, getSessionFactory(), controllerConfig)) {
            metadataStore.start();
            Awaitility.await().with().pollInterval(100, TimeUnit.MILLISECONDS)
                .atMost(10, TimeUnit.SECONDS)
                .until(metadataStore::isLeader);

            try (ControllerTestServer testServer = new ControllerTestServer(0, new ControllerServiceImpl(metadataStore))) {
                testServer.start();
                int port = testServer.getPort();
                ControllerClient client = new GrpcControllerClient();

                CreateGroupRequest request = CreateGroupRequest.newBuilder()
                    .setGroupType(GroupType.GROUP_TYPE_STANDARD)
                    .setName("G-abc")
                    .setDeadLetterTopicId(1)
                    .setMaxRetryAttempt(5)
                    .build();

                String target = String.format("localhost:%d", port);

                CreateGroupReply reply = client.createGroup(target, request).get();
                Assertions.assertEquals(reply.getStatus().getCode(), Code.OK);
                Assertions.assertTrue(reply.getGroupId() > 0);

                // Test duplication
                Assertions.assertThrows(ExecutionException.class, () -> client.createGroup(target, request).get());
            }
        }
    }

    @Test
    public void testReassign() throws IOException, ControllerException, ExecutionException, InterruptedException {
        ControllerClient controllerClient = Mockito.mock(ControllerClient.class);
        ControllerConfig controllerConfig = Mockito.mock(ControllerConfig.class);
        Mockito.when(controllerConfig.nodeId()).thenReturn(1);
        Mockito.when(controllerConfig.scanIntervalInSecs()).thenReturn(1);
        Mockito.when(controllerConfig.leaseLifeSpanInSecs()).thenReturn(2);
        Mockito.when(controllerConfig.scanIntervalInSecs()).thenReturn(1);

        long topicId = 1;
        int queueId = 2;
        int srcNodeId = 1;
        int dstNodeId = 2;

        try (SqlSession session = getSessionFactory().openSession()) {
            QueueAssignmentMapper assignmentMapper = session.getMapper(QueueAssignmentMapper.class);
            QueueAssignment assignment = new QueueAssignment();
            assignment.setStatus(AssignmentStatus.ASSIGNED);
            assignment.setTopicId(topicId);
            assignment.setQueueId(queueId);
            assignment.setSrcNodeId(srcNodeId);
            assignment.setDstNodeId(dstNodeId);
            assignmentMapper.create(assignment);
            session.commit();
        }

        try (MetadataStore metadataStore = new DefaultMetadataStore(controllerClient, getSessionFactory(), controllerConfig)) {
            metadataStore.start();
            Awaitility.await().with().pollInterval(100, TimeUnit.MILLISECONDS)
                .atMost(10, TimeUnit.SECONDS)
                .until(metadataStore::isLeader);

            try (ControllerTestServer testServer = new ControllerTestServer(0, new ControllerServiceImpl(metadataStore))) {
                testServer.start();
                int port = testServer.getPort();
                ControllerClient client = new GrpcControllerClient();
                client.reassignMessageQueue(String.format("localhost:%d", port), topicId, queueId, srcNodeId).get();
            }
        }

        try (SqlSession session = getSessionFactory().openSession()) {
            QueueAssignmentMapper assignmentMapper = session.getMapper(QueueAssignmentMapper.class);
            List<QueueAssignment> assignments = assignmentMapper.list(topicId, null, null, AssignmentStatus.YIELDING, null);
            Assertions.assertEquals(1, assignments.size());
            session.commit();
        }
    }

    @Test
    public void testCreateRetryStream() throws IOException, ControllerException, ExecutionException, InterruptedException {
        ControllerClient controllerClient = Mockito.mock(ControllerClient.class);
        ControllerConfig controllerConfig = Mockito.mock(ControllerConfig.class);
        Mockito.when(controllerConfig.nodeId()).thenReturn(1);
        Mockito.when(controllerConfig.scanIntervalInSecs()).thenReturn(1);
        Mockito.when(controllerConfig.leaseLifeSpanInSecs()).thenReturn(2);
        Mockito.when(controllerConfig.scanIntervalInSecs()).thenReturn(1);

        long topicId = 1;
        int queueId = 2;
        int srcNodeId = 1;
        int dstNodeId = 2;
        String groupName = "G1";
        long groupId;

        try (SqlSession session = getSessionFactory().openSession()) {
            QueueAssignmentMapper assignmentMapper = session.getMapper(QueueAssignmentMapper.class);
            QueueAssignment assignment = new QueueAssignment();
            assignment.setStatus(AssignmentStatus.ASSIGNED);
            assignment.setTopicId(topicId);
            assignment.setQueueId(queueId);
            assignment.setSrcNodeId(srcNodeId);
            assignment.setDstNodeId(dstNodeId);
            assignmentMapper.create(assignment);

            GroupMapper groupMapper = session.getMapper(GroupMapper.class);
            Group group = new Group();
            group.setName(groupName);
            group.setGroupType(com.automq.rocketmq.controller.metadata.database.dao.GroupType.STANDARD);
            group.setDeadLetterTopicId(1);
            group.setMaxRetryAttempt(3);
            groupMapper.create(group);
            groupId = group.getId();

            session.commit();
        }

        try (MetadataStore metadataStore = new DefaultMetadataStore(controllerClient, getSessionFactory(), controllerConfig)) {
            metadataStore.start();
            Awaitility.await().with().pollInterval(100, TimeUnit.MILLISECONDS)
                .atMost(10, TimeUnit.SECONDS)
                .until(metadataStore::isLeader);

            try (ControllerTestServer testServer = new ControllerTestServer(0, new ControllerServiceImpl(metadataStore))) {
                testServer.start();
                int port = testServer.getPort();
                ControllerClient client = new GrpcControllerClient();
                client.createRetryStream(String.format("localhost:%d", port), groupName, topicId, queueId).get();
            }
        }

        try (SqlSession session = getSessionFactory().openSession()) {
            StreamAffiliationMapper streamMapper = session.getMapper(StreamAffiliationMapper.class);
            List<StreamAffiliation> streams = streamMapper.list(topicId, queueId, groupId, null);
            Assertions.assertEquals(1, streams.size());
        }
    }

}