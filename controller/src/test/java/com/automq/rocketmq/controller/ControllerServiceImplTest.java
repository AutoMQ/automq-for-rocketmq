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

import apache.rocketmq.controller.v1.AcceptTypes;
import apache.rocketmq.controller.v1.AssignmentStatus;
import apache.rocketmq.controller.v1.CloseStreamReply;
import apache.rocketmq.controller.v1.CloseStreamRequest;
import apache.rocketmq.common.v1.Code;
import apache.rocketmq.controller.v1.ConsumerGroup;
import apache.rocketmq.controller.v1.ControllerServiceGrpc;
import apache.rocketmq.controller.v1.CreateGroupReply;
import apache.rocketmq.controller.v1.CreateGroupRequest;
import apache.rocketmq.controller.v1.CreateTopicReply;
import apache.rocketmq.controller.v1.CreateTopicRequest;
import apache.rocketmq.controller.v1.DeleteTopicReply;
import apache.rocketmq.controller.v1.DeleteTopicRequest;
import apache.rocketmq.controller.v1.GroupStatus;
import apache.rocketmq.controller.v1.GroupType;
import apache.rocketmq.controller.v1.HeartbeatReply;
import apache.rocketmq.controller.v1.HeartbeatRequest;
import apache.rocketmq.controller.v1.ListOpenStreamsRequest;
import apache.rocketmq.controller.v1.ListTopicsReply;
import apache.rocketmq.controller.v1.ListTopicsRequest;
import apache.rocketmq.controller.v1.MessageType;
import apache.rocketmq.controller.v1.NodeRegistrationReply;
import apache.rocketmq.controller.v1.NodeRegistrationRequest;
import apache.rocketmq.controller.v1.OpenStreamReply;
import apache.rocketmq.controller.v1.OpenStreamRequest;
import apache.rocketmq.controller.v1.StreamMetadata;
import apache.rocketmq.controller.v1.StreamRole;
import apache.rocketmq.controller.v1.StreamState;
import apache.rocketmq.controller.v1.SubscriptionMode;
import apache.rocketmq.controller.v1.TopicStatus;
import apache.rocketmq.controller.v1.UpdateGroupRequest;
import apache.rocketmq.controller.v1.UpdateTopicReply;
import apache.rocketmq.controller.v1.UpdateTopicRequest;
import com.automq.rocketmq.common.exception.ControllerException;
import com.automq.rocketmq.controller.server.ControllerServiceImpl;
import com.automq.rocketmq.controller.store.DatabaseTestBase;
import com.automq.rocketmq.controller.client.GrpcControllerClient;
import com.automq.rocketmq.controller.server.store.DefaultMetadataStore;
import com.automq.rocketmq.metadata.dao.Group;
import com.automq.rocketmq.metadata.dao.GroupCriteria;
import com.automq.rocketmq.metadata.dao.GroupProgress;
import com.automq.rocketmq.metadata.dao.Node;
import com.automq.rocketmq.metadata.dao.QueueAssignment;
import com.automq.rocketmq.metadata.dao.Range;
import com.automq.rocketmq.metadata.dao.Stream;
import com.automq.rocketmq.metadata.dao.StreamCriteria;
import com.automq.rocketmq.metadata.dao.Topic;
import com.automq.rocketmq.metadata.mapper.GroupMapper;
import com.automq.rocketmq.metadata.mapper.GroupProgressMapper;
import com.automq.rocketmq.metadata.mapper.NodeMapper;
import com.automq.rocketmq.metadata.mapper.QueueAssignmentMapper;
import com.automq.rocketmq.metadata.mapper.RangeMapper;
import com.automq.rocketmq.metadata.mapper.StreamMapper;
import com.automq.rocketmq.metadata.mapper.TopicMapper;
import com.google.protobuf.util.JsonFormat;
import io.grpc.Grpc;
import io.grpc.InsecureChannelCredentials;
import io.grpc.ManagedChannel;
import io.grpc.stub.StreamObserver;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletionException;
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
            node.setAddress("localhost:2345");
            node.setInstanceId("i-1");
            node.setEpoch(1);
            mapper.create(node);
            nodeId = node.getId();
            session.commit();
        }
        Mockito.when(config.nodeId()).thenReturn(nodeId);

        ControllerClient controllerClient = Mockito.mock(ControllerClient.class);
        try (DefaultMetadataStore metadataStore = new DefaultMetadataStore(controllerClient, getSessionFactory(), config)) {
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
    public void testCreateTopic() throws IOException {
        AcceptTypes acceptTypes = AcceptTypes.newBuilder()
            .addTypes(MessageType.NORMAL)
            .addTypes(MessageType.DELAY)
            .build();
        ControllerClient controllerClient = Mockito.mock(ControllerClient.class);
        try (DefaultMetadataStore metadataStore = new DefaultMetadataStore(controllerClient, getSessionFactory(), config)) {
            metadataStore.start();
            Node node = new Node();
            node.setId(config.nodeId());
            node.setName(config.name());
            metadataStore.addBrokerNode(node);
            Awaitility.await().with().pollInterval(100, TimeUnit.MILLISECONDS)
                .atMost(10, TimeUnit.SECONDS)
                .until(metadataStore::isLeader);
            try (ControllerTestServer testServer = new ControllerTestServer(0, new ControllerServiceImpl(metadataStore))) {
                testServer.start();
                int port = testServer.getPort();
                ManagedChannel channel = Grpc.newChannelBuilderForAddress("localhost", port, InsecureChannelCredentials.create()).build();
                ControllerServiceGrpc.ControllerServiceBlockingStub blockingStub = ControllerServiceGrpc.newBlockingStub(channel);
                CreateTopicReply reply = blockingStub.withDeadlineAfter(10, TimeUnit.SECONDS)
                    .createTopic(CreateTopicRequest.newBuilder()
                        .setTopic("T1")
                        .setRetentionHours(1)
                        .setAcceptTypes(acceptTypes)
                        .setCount(4)
                        .build());

                Assertions.assertEquals(Code.OK, reply.getStatus().getCode());
                try (SqlSession session = getSessionFactory().openSession()) {
                    TopicMapper topicMapper = session.getMapper(TopicMapper.class);
                    Topic topic = topicMapper.get(reply.getTopicId(), null);
                    Assertions.assertEquals("T1", topic.getName());
                    Assertions.assertEquals(1, topic.getRetentionHours());
                    Assertions.assertEquals(4, topic.getQueueNum());
                    AcceptTypes.Builder builder = AcceptTypes.newBuilder();
                    JsonFormat.parser().merge(topic.getAcceptMessageTypes(), builder);
                    Assertions.assertEquals(acceptTypes, builder.build());
                }
                channel.shutdownNow();
            }
        }
    }

    @Test
    public void testListTopics() throws IOException {
        AcceptTypes acceptTypes = AcceptTypes.newBuilder()
            .addTypes(MessageType.NORMAL)
            .addTypes(MessageType.DELAY)
            .build();
        try (SqlSession session = getSessionFactory().openSession()) {
            TopicMapper topicMapper = session.getMapper(TopicMapper.class);
            for (int i = 0; i < 3; i++) {
                Topic topic = new Topic();
                topic.setStatus(TopicStatus.TOPIC_STATUS_ACTIVE);
                topic.setName("T" + i);
                topic.setAcceptMessageTypes(JsonFormat.printer().print(acceptTypes));
                topic.setQueueNum(0);
                topicMapper.create(topic);
            }
            session.commit();
        }

        ControllerClient controllerClient = Mockito.mock(ControllerClient.class);

        try (DefaultMetadataStore metadataStore = new DefaultMetadataStore(controllerClient, getSessionFactory(), config)) {
            metadataStore.start();
            Awaitility.await().with().pollInterval(100, TimeUnit.MILLISECONDS)
                .atMost(10, TimeUnit.SECONDS)
                .until(metadataStore::isLeader);

            try (ControllerTestServer testServer = new ControllerTestServer(0, new ControllerServiceImpl(metadataStore))) {
                testServer.start();
                int port = testServer.getPort();
                ManagedChannel channel = Grpc.newChannelBuilderForAddress("localhost", port, InsecureChannelCredentials.create()).build();
                ControllerServiceGrpc.ControllerServiceBlockingStub blockingStub = ControllerServiceGrpc.newBlockingStub(channel);
                Iterator<ListTopicsReply> replies = blockingStub.listTopics(ListTopicsRequest.newBuilder().build());
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
    public void testUpdateTopics() throws IOException {
        long topicId;
        try (SqlSession session = getSessionFactory().openSession()) {
            TopicMapper topicMapper = session.getMapper(TopicMapper.class);
            Topic topic = new Topic();
            topic.setStatus(TopicStatus.TOPIC_STATUS_ACTIVE);
            topic.setName("T" + 1);
            topic.setAcceptMessageTypes("[\"NORMAL\",\"DELAY\"]");
            topic.setQueueNum(0);
            topicMapper.create(topic);
            session.commit();
            topicId = topic.getId();
        }

        ControllerClient controllerClient = Mockito.mock(ControllerClient.class);

        AcceptTypes acceptTypes = AcceptTypes.newBuilder()
            .addTypes(MessageType.NORMAL)
            .addTypes(MessageType.FIFO)
            .addTypes(MessageType.DELAY)
            .build();

        try (DefaultMetadataStore metadataStore = new DefaultMetadataStore(controllerClient, getSessionFactory(), config)) {
            metadataStore.start();
            Awaitility.await().with().pollInterval(100, TimeUnit.MILLISECONDS)
                .atMost(10, TimeUnit.SECONDS)
                .until(metadataStore::isLeader);

            try (ControllerTestServer testServer = new ControllerTestServer(0, new ControllerServiceImpl(metadataStore))) {
                testServer.start();
                int port = testServer.getPort();
                ManagedChannel channel = Grpc.newChannelBuilderForAddress("localhost", port, InsecureChannelCredentials.create()).build();
                ControllerServiceGrpc.ControllerServiceBlockingStub blockingStub = ControllerServiceGrpc.newBlockingStub(channel);
                UpdateTopicReply reply = blockingStub.updateTopic(UpdateTopicRequest.newBuilder()
                    .setTopicId(topicId)
                    .setAcceptTypes(acceptTypes)
                    .build());
                Assertions.assertTrue(reply.getTopic().getName().startsWith("T"));
                Assertions.assertEquals(Code.OK, reply.getStatus().getCode());
                Assertions.assertEquals(acceptTypes, reply.getTopic().getAcceptTypes());
                channel.shutdownNow();
            }
        }
    }

    @Test
    public void testDeleteTopic_NotFound() throws IOException {
        ControllerClient client = Mockito.mock(ControllerClient.class);

        try (DefaultMetadataStore metadataStore = new DefaultMetadataStore(client, getSessionFactory(), config)) {
            metadataStore.start();
            Awaitility.await().with().pollInterval(100, TimeUnit.MILLISECONDS)
                .atMost(10, TimeUnit.SECONDS)
                .until(metadataStore::isLeader);

            try (ControllerTestServer testServer = new ControllerTestServer(0, new ControllerServiceImpl(metadataStore))) {
                testServer.start();
                int port = testServer.getPort();
                ManagedChannel channel = Grpc.newChannelBuilderForAddress("localhost", port, InsecureChannelCredentials.create()).build();
                ControllerServiceGrpc.ControllerServiceBlockingStub blockingStub = ControllerServiceGrpc.newBlockingStub(channel);
                DeleteTopicRequest request = DeleteTopicRequest.newBuilder()
                    .setTopicId(1)
                    .build();
                DeleteTopicReply reply = blockingStub.deleteTopic(request);
                Assertions.assertEquals(404, reply.getStatus().getCode().getNumber());
                channel.shutdownNow();
            }
        }
    }

    @Test
    public void testCommitOffset() throws IOException {
        ControllerClient controllerClient = Mockito.mock(ControllerClient.class);

        try (MetadataStore metadataStore = new DefaultMetadataStore(controllerClient, getSessionFactory(), config)) {
            metadataStore.start();
            Awaitility.await().with().pollInterval(100, TimeUnit.MILLISECONDS)
                .atMost(10, TimeUnit.SECONDS)
                .until(metadataStore::isLeader);

            try (ControllerTestServer testServer = new ControllerTestServer(0, new ControllerServiceImpl(metadataStore));
                 ControllerClient client = new GrpcControllerClient(config)
            ) {
                testServer.start();
                int port = testServer.getPort();
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
    public void testCreateGroup() throws IOException, ExecutionException, InterruptedException {
        ControllerClient controllerClient = Mockito.mock(ControllerClient.class);

        // Create dead letter topic first.
        long topicId;
        try (SqlSession session = getSessionFactory().openSession()) {
            TopicMapper topicMapper = session.getMapper(TopicMapper.class);
            Topic topic = new Topic();
            topic.setName("T1");
            topic.setStatus(TopicStatus.TOPIC_STATUS_ACTIVE);
            topic.setQueueNum(4);
            topic.setAcceptMessageTypes("{}");
            topicMapper.create(topic);
            topicId = topic.getId();
            session.commit();
        }

        try (MetadataStore metadataStore = new DefaultMetadataStore(controllerClient, getSessionFactory(), config)) {
            metadataStore.start();
            Awaitility.await().with().pollInterval(100, TimeUnit.MILLISECONDS)
                .atMost(10, TimeUnit.SECONDS)
                .until(metadataStore::isLeader);

            try (ControllerTestServer testServer = new ControllerTestServer(0, new ControllerServiceImpl(metadataStore));
                 ControllerClient client = new GrpcControllerClient(config)
            ) {
                testServer.start();
                int port = testServer.getPort();

                CreateGroupRequest request = CreateGroupRequest.newBuilder()
                    .setGroupType(GroupType.GROUP_TYPE_STANDARD)
                    .setName("G-abc")
                    .setDeadLetterTopicId(topicId)
                    .setMaxDeliveryAttempt(5)
                    .setSubMode(SubscriptionMode.SUB_MODE_POP)
                    .build();

                String target = String.format("localhost:%d", port);

                CreateGroupReply reply = client.createGroup(target, request).get();
                Assertions.assertEquals(reply.getStatus().getCode(), Code.OK);
                Assertions.assertTrue(reply.getGroupId() > 0);

                // Test duplication
                client.createGroup(target, request).whenComplete(
                    (res, e) -> Assertions.assertEquals(ExecutionException.class, e.getClass()));

                request = CreateGroupRequest.newBuilder()
                    .setGroupType(GroupType.GROUP_TYPE_STANDARD)
                    .setName("G-def")
                    .setDeadLetterTopicId(topicId)
                    .setMaxDeliveryAttempt(5)
                    .build();
                try {
                    client.createGroup(target, request).join();
                    Assertions.fail("Should have failed as required field subMode is missing");
                } catch (CompletionException e) {
                    ControllerException cause = (ControllerException) e.getCause();
                    Assertions.assertEquals(Code.BAD_REQUEST_VALUE, cause.getErrorCode());
                    Assertions.assertEquals("SubMode is required", cause.getMessage());
                }
            }
        }
    }

    @Test
    public void testDescribeGroup() throws IOException, ExecutionException, InterruptedException {
        ControllerClient controllerClient = Mockito.mock(ControllerClient.class);

        long groupId;

        try (SqlSession session = getSessionFactory().openSession()) {
            GroupMapper mapper = session.getMapper(GroupMapper.class);
            Group group = new Group();
            group.setStatus(GroupStatus.GROUP_STATUS_ACTIVE);
            group.setName("G1");
            group.setMaxDeliveryAttempt(1);
            group.setGroupType(GroupType.GROUP_TYPE_FIFO);
            group.setDeadLetterTopicId(2L);
            mapper.create(group);
            groupId = group.getId();
            session.commit();
        }

        try (MetadataStore metadataStore = new DefaultMetadataStore(controllerClient, getSessionFactory(), config)) {
            metadataStore.start();
            Awaitility.await().with().pollInterval(100, TimeUnit.MILLISECONDS)
                .atMost(10, TimeUnit.SECONDS)
                .until(metadataStore::isLeader);

            try (ControllerTestServer testServer = new ControllerTestServer(0, new ControllerServiceImpl(metadataStore));
                 ControllerClient client = new GrpcControllerClient(config)
            ) {
                testServer.start();
                int port = testServer.getPort();

                String target = String.format("localhost:%d", port);

                ConsumerGroup cg = client.describeGroup(target, "G1").get();
                Assertions.assertEquals(cg.getGroupId(), groupId);
                Assertions.assertEquals(cg.getMaxDeliveryAttempt(), 1);
                Assertions.assertEquals(cg.getGroupType(), GroupType.GROUP_TYPE_FIFO);
            }
        }
    }

    @Test
    public void testDeleteGroup() throws IOException {
        ControllerClient controllerClient = Mockito.mock(ControllerClient.class);

        long groupId;

        try (SqlSession session = getSessionFactory().openSession()) {
            GroupMapper mapper = session.getMapper(GroupMapper.class);
            Group group = new Group();
            group.setStatus(GroupStatus.GROUP_STATUS_ACTIVE);
            group.setName("G1");
            group.setMaxDeliveryAttempt(1);
            group.setGroupType(GroupType.GROUP_TYPE_FIFO);
            group.setDeadLetterTopicId(2L);
            mapper.create(group);
            groupId = group.getId();
            session.commit();
        }

        try (MetadataStore metadataStore = new DefaultMetadataStore(controllerClient, getSessionFactory(), config)) {
            metadataStore.start();
            Awaitility.await().with().pollInterval(100, TimeUnit.MILLISECONDS)
                .atMost(10, TimeUnit.SECONDS)
                .until(metadataStore::isLeader);

            try (ControllerTestServer testServer = new ControllerTestServer(0, new ControllerServiceImpl(metadataStore));
                 ControllerClient client = new GrpcControllerClient(config)
            ) {
                testServer.start();
                int port = testServer.getPort();
                String target = String.format("localhost:%d", port);
                Assertions.assertDoesNotThrow(() -> client.deleteGroup(target, groupId).get());

                try (SqlSession session = getSessionFactory().openSession()) {
                    GroupMapper mapper = session.getMapper(GroupMapper.class);
                    List<Group> groups = mapper.byCriteria(GroupCriteria.newBuilder().setGroupId(groupId).build());
                    Assertions.assertEquals(1, groups.size());
                    Group g = groups.get(0);
                    Assertions.assertEquals(GroupStatus.GROUP_STATUS_DELETED, g.getStatus());
                }
            }
        }
    }

    @Test
    public void testUpdateGroup() throws IOException {
        ControllerClient controllerClient = Mockito.mock(ControllerClient.class);

        long groupId;

        try (SqlSession session = getSessionFactory().openSession()) {
            GroupMapper mapper = session.getMapper(GroupMapper.class);
            Group group = new Group();
            group.setStatus(GroupStatus.GROUP_STATUS_ACTIVE);
            group.setName("G1");
            group.setMaxDeliveryAttempt(1);
            group.setGroupType(GroupType.GROUP_TYPE_FIFO);
            group.setDeadLetterTopicId(2L);
            mapper.create(group);
            groupId = group.getId();
            session.commit();
        }

        try (MetadataStore metadataStore = new DefaultMetadataStore(controllerClient, getSessionFactory(), config)) {
            metadataStore.start();
            Awaitility.await().with().pollInterval(100, TimeUnit.MILLISECONDS)
                .atMost(10, TimeUnit.SECONDS)
                .until(metadataStore::isLeader);

            try (ControllerTestServer testServer = new ControllerTestServer(0, new ControllerServiceImpl(metadataStore));
                 ControllerClient client = new GrpcControllerClient(config)
            ) {
                testServer.start();
                int port = testServer.getPort();
                String target = String.format("localhost:%d", port);
                UpdateGroupRequest request = UpdateGroupRequest.newBuilder()
                    .setGroupId(groupId)
                    .setGroupType(GroupType.GROUP_TYPE_STANDARD)
                    .setName("G2")
                    .setMaxRetryAttempt(2)
                    .setDeadLetterTopicId(4L)
                    .build();
                client.updateGroup(target, request).join();
            }
        }

        try (SqlSession session = getSessionFactory().openSession()) {
            GroupMapper mapper = session.getMapper(GroupMapper.class);
            List<Group> groups = mapper.byCriteria(GroupCriteria.newBuilder().setGroupId(groupId).build());
            Assertions.assertEquals(1, groups.size());
            Group group = groups.get(0);
            Assertions.assertEquals("G2", group.getName());
            Assertions.assertEquals(GroupType.GROUP_TYPE_STANDARD, group.getGroupType());
            Assertions.assertEquals(2, group.getMaxDeliveryAttempt());
            Assertions.assertEquals(4, group.getDeadLetterTopicId());
        }
    }

    @Test
    public void testReassign() throws IOException, ExecutionException, InterruptedException {
        ControllerClient controllerClient = Mockito.mock(ControllerClient.class);

        long topicId = 1;
        int queueId = 2;
        int srcNodeId = 1;
        int dstNodeId = 2;

        try (SqlSession session = getSessionFactory().openSession()) {
            QueueAssignmentMapper assignmentMapper = session.getMapper(QueueAssignmentMapper.class);
            QueueAssignment assignment = new QueueAssignment();
            assignment.setStatus(AssignmentStatus.ASSIGNMENT_STATUS_ASSIGNED);
            assignment.setTopicId(topicId);
            assignment.setQueueId(queueId);
            assignment.setSrcNodeId(srcNodeId);
            assignment.setDstNodeId(dstNodeId);
            assignmentMapper.create(assignment);
            session.commit();
        }

        try (MetadataStore metadataStore = new DefaultMetadataStore(controllerClient, getSessionFactory(), config)) {
            metadataStore.start();
            Awaitility.await().with().pollInterval(100, TimeUnit.MILLISECONDS)
                .atMost(10, TimeUnit.SECONDS)
                .until(metadataStore::isLeader);

            try (ControllerTestServer testServer = new ControllerTestServer(0, new ControllerServiceImpl(metadataStore));
                 ControllerClient client = new GrpcControllerClient(config)
            ) {
                testServer.start();
                int port = testServer.getPort();
                client.reassignMessageQueue(String.format("localhost:%d", port), topicId, queueId, srcNodeId).get();
            }
        }

        try (SqlSession session = getSessionFactory().openSession()) {
            QueueAssignmentMapper assignmentMapper = session.getMapper(QueueAssignmentMapper.class);
            List<QueueAssignment> assignments = assignmentMapper.list(topicId, null, null, AssignmentStatus.ASSIGNMENT_STATUS_YIELDING, null);
            Assertions.assertEquals(1, assignments.size());
            session.commit();
        }
    }

    @Test
    public void testOpenStream() throws IOException, ExecutionException, InterruptedException {
        ControllerClient controllerClient = Mockito.mock(ControllerClient.class);

        long topicId = 1;
        int queueId = 2;
        int srcNodeId = 1;
        int dstNodeId = 2;
        long streamId, streamEpoch;

        try (SqlSession session = getSessionFactory().openSession()) {
            StreamMapper streamMapper = session.getMapper(StreamMapper.class);

            Stream stream = new Stream();
            stream.setStreamRole(StreamRole.STREAM_ROLE_DATA);
            stream.setTopicId(topicId);
            stream.setQueueId(queueId);
            stream.setState(StreamState.UNINITIALIZED);
            stream.setSrcNodeId(srcNodeId);
            stream.setDstNodeId(dstNodeId);
            streamMapper.create(stream);
            streamId = stream.getId();
            streamEpoch = stream.getEpoch();
            session.commit();
        }

        try (MetadataStore metadataStore = new DefaultMetadataStore(controllerClient, getSessionFactory(), config)) {
            metadataStore.start();
            Awaitility.await().with().pollInterval(100, TimeUnit.MILLISECONDS)
                .atMost(10, TimeUnit.SECONDS)
                .until(metadataStore::isLeader);

            try (ControllerTestServer testServer = new ControllerTestServer(0, new ControllerServiceImpl(metadataStore));
                 ControllerClient client = new GrpcControllerClient(config)
            ) {
                testServer.start();
                int port = testServer.getPort();
                OpenStreamRequest request = OpenStreamRequest.newBuilder()
                    .setStreamId(streamId)
                    .setStreamEpoch(streamEpoch)
                    .setBrokerId(2)
                    .build();
                client.openStream(String.format("localhost:%d", port), request).get();
            }
        }

        try (SqlSession session = getSessionFactory().openSession()) {
            StreamMapper streamMapper = session.getMapper(StreamMapper.class);
            StreamCriteria criteria = StreamCriteria.newBuilder()
                .withTopicId(topicId)
                .withQueueId(queueId)
                .build();
            List<Stream> streams = streamMapper.byCriteria(criteria);
            Assertions.assertEquals(1, streams.size());
            Assertions.assertEquals(StreamState.OPEN, streams.get(0).getState());
        }
    }

    @Test
    public void testOpenStream_NotFound() throws IOException, ExecutionException, InterruptedException {
        ControllerClient controllerClient = Mockito.mock(ControllerClient.class);

        long topicId = 1;
        int queueId = 2;
        int srcNodeId = 1;
        int dstNodeId = 2;
        long streamId;

        try (SqlSession session = getSessionFactory().openSession()) {
            StreamMapper streamMapper = session.getMapper(StreamMapper.class);

            Stream stream = new Stream();
            stream.setStreamRole(StreamRole.STREAM_ROLE_DATA);
            stream.setTopicId(topicId);
            stream.setQueueId(queueId);
            stream.setState(StreamState.CLOSED);
            stream.setEpoch(1L);
            stream.setRangeId(0);
            stream.setSrcNodeId(srcNodeId);
            stream.setDstNodeId(dstNodeId);
            streamMapper.create(stream);
            streamId = stream.getId();

            RangeMapper rangeMapper = session.getMapper(RangeMapper.class);
            Range range = new Range();
            range.setRangeId(0);
            range.setStartOffset(0L);
            range.setNodeId(2);
            range.setStreamId(streamId);
            range.setEpoch(1L);
            range.setEndOffset(100L);
            rangeMapper.create(range);

            session.commit();
        }

        try (MetadataStore metadataStore = new DefaultMetadataStore(controllerClient, getSessionFactory(), config)) {
            metadataStore.start();
            Awaitility.await().with().pollInterval(100, TimeUnit.MILLISECONDS)
                .atMost(10, TimeUnit.SECONDS)
                .until(metadataStore::isLeader);

            try (ControllerTestServer testServer = new ControllerTestServer(0, new ControllerServiceImpl(metadataStore));
                 ControllerClient client = new GrpcControllerClient(config)
            ) {
                testServer.start();
                int port = testServer.getPort();
                OpenStreamRequest request = OpenStreamRequest.newBuilder()
                    .setStreamId(streamId + 100)
                    .setStreamEpoch(1)
                    .setBrokerEpoch(1)
                    .build();
                OpenStreamReply reply = client.openStream(String.format("localhost:%d", port), request).get();
                Assertions.assertEquals(Code.NOT_FOUND, reply.getStatus().getCode());
            }
        }
    }

    @Test
    public void testOpenStream_Fenced() throws IOException, ExecutionException, InterruptedException {
        ControllerClient controllerClient = Mockito.mock(ControllerClient.class);

        long topicId = 1;
        int queueId = 2;
        int srcNodeId = 1;
        int dstNodeId = 2;
        long streamId;

        try (SqlSession session = getSessionFactory().openSession()) {
            StreamMapper streamMapper = session.getMapper(StreamMapper.class);

            Stream stream = new Stream();
            stream.setStreamRole(StreamRole.STREAM_ROLE_DATA);
            stream.setTopicId(topicId);
            stream.setQueueId(queueId);
            stream.setState(StreamState.CLOSED);
            stream.setEpoch(1L);
            stream.setRangeId(0);
            stream.setSrcNodeId(srcNodeId);
            stream.setDstNodeId(dstNodeId);
            streamMapper.create(stream);
            streamId = stream.getId();

            RangeMapper rangeMapper = session.getMapper(RangeMapper.class);
            Range range = new Range();
            range.setRangeId(0);
            range.setStartOffset(0L);
            range.setNodeId(2);
            range.setStreamId(streamId);
            range.setEpoch(1L);
            range.setEndOffset(100L);
            rangeMapper.create(range);

            session.commit();
        }

        try (MetadataStore metadataStore = new DefaultMetadataStore(controllerClient, getSessionFactory(), config)) {
            metadataStore.start();
            Awaitility.await().with().pollInterval(100, TimeUnit.MILLISECONDS)
                .atMost(10, TimeUnit.SECONDS)
                .until(metadataStore::isLeader);

            try (ControllerTestServer testServer = new ControllerTestServer(0, new ControllerServiceImpl(metadataStore));
                 ControllerClient client = new GrpcControllerClient(config)
            ) {
                testServer.start();
                int port = testServer.getPort();
                OpenStreamRequest request = OpenStreamRequest.newBuilder()
                    .setStreamId(streamId)
                    .setStreamEpoch(0)
                    .setBrokerId(2)
                    .build();
                OpenStreamReply reply = client.openStream(String.format("localhost:%d", port), request).get();
                Assertions.assertEquals(Code.FENCED, reply.getStatus().getCode());
            }
        }
    }

    @Test
    public void testCloseStream() throws IOException, ExecutionException, InterruptedException {
        ControllerClient controllerClient = Mockito.mock(ControllerClient.class);

        long topicId = 1;
        int queueId = 2;
        int srcNodeId = 1;
        int dstNodeId = 2;
        long streamId;

        try (SqlSession session = getSessionFactory().openSession()) {
            StreamMapper streamMapper = session.getMapper(StreamMapper.class);

            Stream stream = new Stream();
            stream.setStreamRole(StreamRole.STREAM_ROLE_DATA);
            stream.setTopicId(topicId);
            stream.setQueueId(queueId);
            stream.setEpoch(1L);
            stream.setState(StreamState.OPEN);
            stream.setStartOffset(0L);
            stream.setSrcNodeId(srcNodeId);
            stream.setDstNodeId(dstNodeId);
            streamMapper.create(stream);
            streamId = stream.getId();

            RangeMapper rangeMapper = session.getMapper(RangeMapper.class);
            Range range = new Range();
            range.setRangeId(0);
            range.setStartOffset(0L);
            range.setNodeId(1);
            range.setStreamId(streamId);
            range.setEpoch(1L);
            range.setEndOffset(100L);
            rangeMapper.create(range);

            session.commit();
        }

        try (MetadataStore metadataStore = new DefaultMetadataStore(controllerClient, getSessionFactory(), config)) {
            metadataStore.start();
            Awaitility.await().with().pollInterval(100, TimeUnit.MILLISECONDS)
                .atMost(3, TimeUnit.SECONDS)
                .until(metadataStore::isLeader);

            try (ControllerTestServer testServer = new ControllerTestServer(0, new ControllerServiceImpl(metadataStore));
                 ControllerClient client = new GrpcControllerClient(config)
            ) {
                testServer.start();
                int port = testServer.getPort();
                CloseStreamRequest request = CloseStreamRequest.newBuilder()
                    .setStreamId(streamId)
                    .setBrokerId(2)
                    .setStreamEpoch(1)
                    .setBrokerEpoch(1)
                    .build();
                client.closeStream(String.format("localhost:%d", port), request).get();
            }
        }

        try (SqlSession session = getSessionFactory().openSession()) {
            StreamMapper streamMapper = session.getMapper(StreamMapper.class);
            StreamCriteria criteria = StreamCriteria.newBuilder()
                .withTopicId(topicId)
                .withQueueId(queueId)
                .build();
            List<Stream> streams = streamMapper.byCriteria(criteria);
            Assertions.assertEquals(1, streams.size());
            Assertions.assertEquals(StreamState.CLOSED, streams.get(0).getState());
        }
    }

    @Test
    public void testCloseStream_NotFound() throws IOException, ExecutionException, InterruptedException {
        ControllerClient controllerClient = Mockito.mock(ControllerClient.class);

        long topicId = 1;
        int queueId = 2;
        int srcNodeId = 1;
        int dstNodeId = 2;
        long streamId;

        try (SqlSession session = getSessionFactory().openSession()) {
            StreamMapper streamMapper = session.getMapper(StreamMapper.class);

            Stream stream = new Stream();
            stream.setStreamRole(StreamRole.STREAM_ROLE_DATA);
            stream.setTopicId(topicId);
            stream.setQueueId(queueId);
            stream.setEpoch(1L);
            stream.setState(StreamState.OPEN);
            stream.setStartOffset(0L);
            stream.setSrcNodeId(srcNodeId);
            stream.setDstNodeId(dstNodeId);
            streamMapper.create(stream);
            streamId = stream.getId();

            RangeMapper rangeMapper = session.getMapper(RangeMapper.class);
            Range range = new Range();
            range.setRangeId(0);
            range.setStartOffset(0L);
            range.setNodeId(1);
            range.setStreamId(streamId);
            range.setEpoch(1L);
            range.setEndOffset(100L);
            rangeMapper.create(range);

            session.commit();
        }

        try (MetadataStore metadataStore = new DefaultMetadataStore(controllerClient, getSessionFactory(), config)) {
            metadataStore.start();
            Awaitility.await().with().pollInterval(100, TimeUnit.MILLISECONDS)
                .atMost(10, TimeUnit.SECONDS)
                .until(metadataStore::isLeader);

            try (ControllerTestServer testServer = new ControllerTestServer(0, new ControllerServiceImpl(metadataStore));
                 ControllerClient client = new GrpcControllerClient(config)
            ) {
                testServer.start();
                int port = testServer.getPort();
                CloseStreamRequest request = CloseStreamRequest.newBuilder()
                    .setStreamId(streamId + 100) // Not found
                    .setBrokerId(2)
                    .setStreamEpoch(1)
                    .setBrokerEpoch(1)
                    .build();
                CloseStreamReply reply = client.closeStream(String.format("localhost:%d", port), request).get();
                Assertions.assertEquals(Code.NOT_FOUND, reply.getStatus().getCode());
            }
        }
    }

    @Test
    public void testListOpenStreams() throws IOException, ExecutionException, InterruptedException {
        ControllerClient controllerClient = Mockito.mock(ControllerClient.class);

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

        try (MetadataStore metadataStore = new DefaultMetadataStore(controllerClient, getSessionFactory(), config)) {
            metadataStore.start();
            Awaitility.await().with().pollInterval(100, TimeUnit.MILLISECONDS)
                .atMost(10, TimeUnit.SECONDS)
                .until(metadataStore::isLeader);

            try (ControllerTestServer testServer = new ControllerTestServer(0, new ControllerServiceImpl(metadataStore));
                 ControllerClient client = new GrpcControllerClient(config)
            ) {
                testServer.start();
                int port = testServer.getPort();

                ListOpenStreamsRequest request = ListOpenStreamsRequest.newBuilder()
                    .setBrokerId(1)
                    .setBrokerEpoch(2)
                    .build();
                client.listOpenStreams(String.format("localhost:%d", port), request).get();
            }
        }
    }

    @Test
    public void testCreateTopic_OpenStream_CloseStream() throws IOException, ExecutionException, InterruptedException, ControllerException {
        ControllerClient controllerClient = Mockito.mock(ControllerClient.class);

        long streamId;
        int rangeId;
        int nodeId = 1;

        String topicName = "t1";
        int queueNum = 4;
        AcceptTypes acceptTypes = AcceptTypes.newBuilder()
            .addTypes(MessageType.NORMAL)
            .addTypes(MessageType.FIFO)
            .addTypes(MessageType.DELAY)
            .build();

        try (MetadataStore metadataStore = new DefaultMetadataStore(controllerClient, getSessionFactory(), config)) {
            metadataStore.start();
            Awaitility.await().with().pollInterval(100, TimeUnit.MILLISECONDS)
                .atMost(10, TimeUnit.SECONDS)
                .until(metadataStore::isLeader);

            try (ControllerTestServer testServer = new ControllerTestServer(0, new ControllerServiceImpl(metadataStore));
                 ControllerClient client = new GrpcControllerClient(config);
                 SqlSession session = getSessionFactory().openSession()
            ) {
                testServer.start();
                int port = testServer.getPort();
                String name = "broker-0";
                String address = "localhost:1234";
                String instanceId = "i-register";

                Node node = new Node();
                node.setName(name);
                node.setAddress(address);
                node.setInstanceId(instanceId);
                node.setId(nodeId);
                metadataStore.addBrokerNode(node);

                Long topicId = client.createTopic(String.format("localhost:%d", port), CreateTopicRequest.newBuilder()
                    .setTopic(topicName)
                    .setCount(queueNum)
                    .setAcceptTypes(acceptTypes)
                    .build()).get();

                Assertions.assertThrows(ExecutionException.class, () -> client.createTopic(String.format("localhost:%d", port), CreateTopicRequest.newBuilder()
                    .setTopic(topicName)
                    .setCount(queueNum)
                    .setAcceptTypes(acceptTypes)
                    .build()).get());

                StreamMetadata metadata = metadataStore.getStream(topicId, 0, null, StreamRole.STREAM_ROLE_DATA).get();
                streamId = metadata.getStreamId();
                OpenStreamRequest request = OpenStreamRequest.newBuilder()
                    .setStreamId(metadata.getStreamId())
                    .setStreamEpoch(metadata.getEpoch())
                    .setBrokerId(nodeId)
                    .build();

                OpenStreamReply reply = client.openStream(String.format("localhost:%d", port), request).get();
                StreamMetadata openStream = reply.getStreamMetadata();
                Assertions.assertEquals(0, openStream.getStartOffset());
                Assertions.assertEquals(metadata.getEpoch() + 1, openStream.getEpoch());
                Assertions.assertEquals(0, openStream.getRangeId());
                Assertions.assertEquals(StreamState.OPEN, openStream.getState());
                rangeId = openStream.getRangeId();

                client.closeStream(String.format("localhost:%d", port), CloseStreamRequest.newBuilder()
                    .setStreamId(streamId)
                    .setStreamEpoch(openStream.getEpoch())
                    .setBrokerId(nodeId)
                    .build()).get();

                // Verify
                StreamMapper streamMapper = session.getMapper(StreamMapper.class);
                RangeMapper rangeMapper = session.getMapper(RangeMapper.class);

                Stream stream = streamMapper.getByStreamId(streamId);
                Assertions.assertEquals(streamId, stream.getId());
                Assertions.assertEquals(0, stream.getStartOffset());
                Assertions.assertEquals(openStream.getEpoch(), stream.getEpoch());
                Assertions.assertEquals(0, stream.getRangeId());
                Assertions.assertEquals(StreamState.CLOSED, stream.getState());

                Range range = rangeMapper.get(rangeId, streamId, null);
                Assertions.assertEquals(0, range.getRangeId());
                Assertions.assertEquals(streamId, range.getStreamId());
                Assertions.assertEquals(openStream.getEpoch(), range.getEpoch());
                Assertions.assertEquals(0, range.getStartOffset());
                Assertions.assertEquals(0, range.getEndOffset());
            }
        }
    }
}