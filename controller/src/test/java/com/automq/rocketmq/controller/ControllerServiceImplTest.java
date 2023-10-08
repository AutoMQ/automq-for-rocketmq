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

import apache.rocketmq.controller.v1.AssignmentStatus;
import apache.rocketmq.controller.v1.CloseStreamReply;
import apache.rocketmq.controller.v1.CloseStreamRequest;
import apache.rocketmq.controller.v1.Code;
import apache.rocketmq.controller.v1.CommitStreamObjectRequest;
import apache.rocketmq.controller.v1.CommitWALObjectRequest;
import apache.rocketmq.controller.v1.ControllerServiceGrpc;
import apache.rocketmq.controller.v1.CreateGroupReply;
import apache.rocketmq.controller.v1.CreateGroupRequest;
import apache.rocketmq.controller.v1.CreateTopicRequest;
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
import apache.rocketmq.controller.v1.PrepareS3ObjectsReply;
import apache.rocketmq.controller.v1.PrepareS3ObjectsRequest;
import apache.rocketmq.controller.v1.S3ObjectState;
import apache.rocketmq.controller.v1.StreamMetadata;
import apache.rocketmq.controller.v1.StreamRole;
import apache.rocketmq.controller.v1.StreamState;
import apache.rocketmq.controller.v1.TopicStatus;
import apache.rocketmq.controller.v1.TrimStreamRequest;
import apache.rocketmq.controller.v1.UpdateTopicReply;
import apache.rocketmq.controller.v1.UpdateTopicRequest;
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
import com.automq.rocketmq.controller.metadata.database.dao.Range;
import com.automq.rocketmq.controller.metadata.database.dao.S3Object;
import com.automq.rocketmq.controller.metadata.database.dao.S3StreamObject;
import com.automq.rocketmq.controller.metadata.database.dao.S3WALObject;
import com.automq.rocketmq.controller.metadata.database.dao.Stream;
import com.automq.rocketmq.controller.metadata.database.dao.Topic;
import com.automq.rocketmq.controller.metadata.database.mapper.GroupMapper;
import com.automq.rocketmq.controller.metadata.database.mapper.GroupProgressMapper;
import com.automq.rocketmq.controller.metadata.database.mapper.NodeMapper;
import com.automq.rocketmq.controller.metadata.database.mapper.QueueAssignmentMapper;
import com.automq.rocketmq.controller.metadata.database.mapper.RangeMapper;
import com.automq.rocketmq.controller.metadata.database.mapper.S3ObjectMapper;
import com.automq.rocketmq.controller.metadata.database.mapper.S3StreamObjectMapper;
import com.automq.rocketmq.controller.metadata.database.mapper.S3WALObjectMapper;
import com.automq.rocketmq.controller.metadata.database.mapper.StreamMapper;
import com.automq.rocketmq.controller.metadata.database.mapper.TopicMapper;
import io.grpc.Grpc;
import io.grpc.InsecureChannelCredentials;
import io.grpc.ManagedChannel;
import io.grpc.stub.StreamObserver;

import java.io.IOException;
import java.util.ArrayList;
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
                topic.setStatus(TopicStatus.TOPIC_STATUS_ACTIVE);
                topic.setName("T" + i);
                topic.setAcceptMessageTypes("[\"NORMAL\",\"DELAY\"]");
                topic.setQueueNum(0);
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
        ControllerConfig controllerConfig = Mockito.mock(ControllerConfig.class);
        Mockito.when(controllerConfig.nodeId()).thenReturn(1);
        Mockito.when(controllerConfig.scanIntervalInSecs()).thenReturn(1);
        Mockito.when(controllerConfig.leaseLifeSpanInSecs()).thenReturn(2);
        Mockito.when(controllerConfig.scanIntervalInSecs()).thenReturn(1);

        List<MessageType> messageTypeList = new ArrayList<>();
        messageTypeList.add(MessageType.NORMAL);
        messageTypeList.add(MessageType.FIFO);
        messageTypeList.add(MessageType.DELAY);

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
                UpdateTopicReply reply = blockingStub.updateTopic(UpdateTopicRequest.newBuilder()
                    .setTopicId(topicId)
                    .addAllAcceptMessageTypes(messageTypeList)
                    .build());
                Assertions.assertTrue(reply.getTopic().getName().startsWith("T"));
                Assertions.assertEquals(Code.OK, reply.getStatus().getCode());
                Assertions.assertEquals(messageTypeList, reply.getTopic().getAcceptMessageTypesList());
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

            try (ControllerTestServer testServer = new ControllerTestServer(0, new ControllerServiceImpl(metadataStore));
                 ControllerClient client = new GrpcControllerClient()
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

            try (ControllerTestServer testServer = new ControllerTestServer(0, new ControllerServiceImpl(metadataStore));
                 ControllerClient client = new GrpcControllerClient()
            ) {
                testServer.start();
                int port = testServer.getPort();

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
                client.createGroup(target, request).whenComplete((res, e) -> {
                    Assertions.assertEquals(ExecutionException.class, e.getClass());
                });
            }
        }
    }

    @Test
    public void testReassign() throws IOException, ExecutionException, InterruptedException {
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
            assignment.setStatus(AssignmentStatus.ASSIGNMENT_STATUS_ASSIGNED);
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

            try (ControllerTestServer testServer = new ControllerTestServer(0, new ControllerServiceImpl(metadataStore));
                 ControllerClient client = new GrpcControllerClient()
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
    public void testCreateRetryStream() throws IOException, ExecutionException, InterruptedException {
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
            assignment.setStatus(AssignmentStatus.ASSIGNMENT_STATUS_ASSIGNED);
            assignment.setTopicId(topicId);
            assignment.setQueueId(queueId);
            assignment.setSrcNodeId(srcNodeId);
            assignment.setDstNodeId(dstNodeId);
            assignmentMapper.create(assignment);

            GroupMapper groupMapper = session.getMapper(GroupMapper.class);
            Group group = new Group();
            group.setName(groupName);
            group.setGroupType(GroupType.GROUP_TYPE_STANDARD);
            group.setDeadLetterTopicId(1);
            group.setMaxDeliveryAttempt(3);
            groupMapper.create(group);
            groupId = group.getId();

            session.commit();
        }

        try (MetadataStore metadataStore = new DefaultMetadataStore(controllerClient, getSessionFactory(), controllerConfig)) {
            metadataStore.start();
            Awaitility.await().with().pollInterval(100, TimeUnit.MILLISECONDS)
                .atMost(10, TimeUnit.SECONDS)
                .until(metadataStore::isLeader);

            try (ControllerTestServer testServer = new ControllerTestServer(0, new ControllerServiceImpl(metadataStore));
                 ControllerClient client = new GrpcControllerClient()
            ) {
                testServer.start();
                int port = testServer.getPort();
                client.createRetryStream(String.format("localhost:%d", port), groupName, topicId, queueId).get();
            }
        }

        try (SqlSession session = getSessionFactory().openSession()) {
            StreamMapper streamMapper = session.getMapper(StreamMapper.class);
            List<Stream> streams = streamMapper.list(topicId, queueId, groupId);
            Assertions.assertEquals(1, streams.size());
        }
    }

    @Test
    public void testOpenStream() throws IOException, ExecutionException, InterruptedException {
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

        try (MetadataStore metadataStore = new DefaultMetadataStore(controllerClient, getSessionFactory(), controllerConfig)) {
            metadataStore.start();
            Awaitility.await().with().pollInterval(100, TimeUnit.MILLISECONDS)
                .atMost(10, TimeUnit.SECONDS)
                .until(metadataStore::isLeader);

            try (ControllerTestServer testServer = new ControllerTestServer(0, new ControllerServiceImpl(metadataStore));
                 ControllerClient client = new GrpcControllerClient()
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
            List<Stream> streams = streamMapper.list(topicId, queueId, null);
            Assertions.assertEquals(1, streams.size());
            Assertions.assertEquals(StreamState.OPEN, streams.get(0).getState());
        }
    }

    @Test
    public void testOpenStream_NotFound() throws IOException, ExecutionException, InterruptedException {
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
        long streamId;

        try (SqlSession session = getSessionFactory().openSession()) {
            StreamMapper streamMapper = session.getMapper(StreamMapper.class);

            Stream stream = new Stream();
            stream.setStreamRole(StreamRole.STREAM_ROLE_DATA);
            stream.setTopicId(topicId);
            stream.setQueueId(queueId);
            stream.setState(StreamState.CLOSED);
            stream.setEpoch(1);
            stream.setRangeId(0);
            stream.setSrcNodeId(srcNodeId);
            stream.setDstNodeId(dstNodeId);
            streamMapper.create(stream);
            streamId = stream.getId();

            RangeMapper rangeMapper = session.getMapper(RangeMapper.class);
            Range range = new Range();
            range.setRangeId(0);
            range.setStartOffset(0L);
            range.setBrokerId(2);
            range.setStreamId(streamId);
            range.setEpoch(1L);
            range.setEndOffset(100L);
            rangeMapper.create(range);

            session.commit();
        }

        try (MetadataStore metadataStore = new DefaultMetadataStore(controllerClient, getSessionFactory(), controllerConfig)) {
            metadataStore.start();
            Awaitility.await().with().pollInterval(100, TimeUnit.MILLISECONDS)
                .atMost(10, TimeUnit.SECONDS)
                .until(metadataStore::isLeader);

            try (ControllerTestServer testServer = new ControllerTestServer(0, new ControllerServiceImpl(metadataStore));
                 ControllerClient client = new GrpcControllerClient()
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
        ControllerConfig controllerConfig = Mockito.mock(ControllerConfig.class);
        Mockito.when(controllerConfig.nodeId()).thenReturn(1);
        Mockito.when(controllerConfig.scanIntervalInSecs()).thenReturn(1);
        Mockito.when(controllerConfig.leaseLifeSpanInSecs()).thenReturn(2);
        Mockito.when(controllerConfig.scanIntervalInSecs()).thenReturn(1);

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
            stream.setEpoch(1);
            stream.setRangeId(0);
            stream.setSrcNodeId(srcNodeId);
            stream.setDstNodeId(dstNodeId);
            streamMapper.create(stream);
            streamId = stream.getId();

            RangeMapper rangeMapper = session.getMapper(RangeMapper.class);
            Range range = new Range();
            range.setRangeId(0);
            range.setStartOffset(0L);
            range.setBrokerId(2);
            range.setStreamId(streamId);
            range.setEpoch(1L);
            range.setEndOffset(100L);
            rangeMapper.create(range);

            session.commit();
        }

        try (MetadataStore metadataStore = new DefaultMetadataStore(controllerClient, getSessionFactory(), controllerConfig)) {
            metadataStore.start();
            Awaitility.await().with().pollInterval(100, TimeUnit.MILLISECONDS)
                .atMost(10, TimeUnit.SECONDS)
                .until(metadataStore::isLeader);

            try (ControllerTestServer testServer = new ControllerTestServer(0, new ControllerServiceImpl(metadataStore));
                 ControllerClient client = new GrpcControllerClient()
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
        ControllerConfig controllerConfig = Mockito.mock(ControllerConfig.class);
        Mockito.when(controllerConfig.nodeId()).thenReturn(2);
        Mockito.when(controllerConfig.scanIntervalInSecs()).thenReturn(1);
        Mockito.when(controllerConfig.leaseLifeSpanInSecs()).thenReturn(2);
        Mockito.when(controllerConfig.scanIntervalInSecs()).thenReturn(1);

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
            stream.setEpoch(1);
            stream.setState(StreamState.OPEN);
            stream.setStartOffset(0);
            stream.setSrcNodeId(srcNodeId);
            stream.setDstNodeId(dstNodeId);
            streamMapper.create(stream);
            streamId = stream.getId();

            RangeMapper rangeMapper = session.getMapper(RangeMapper.class);
            Range range = new Range();
            range.setRangeId(0);
            range.setStartOffset(0L);
            range.setBrokerId(1);
            range.setStreamId(streamId);
            range.setEpoch(1L);
            range.setEndOffset(100L);
            rangeMapper.create(range);

            session.commit();
        }

        try (MetadataStore metadataStore = new DefaultMetadataStore(controllerClient, getSessionFactory(), controllerConfig)) {
            metadataStore.start();
            Awaitility.await().with().pollInterval(100, TimeUnit.MILLISECONDS)
                .atMost(3, TimeUnit.SECONDS)
                .until(metadataStore::isLeader);

            try (ControllerTestServer testServer = new ControllerTestServer(0, new ControllerServiceImpl(metadataStore));
                 ControllerClient client = new GrpcControllerClient()
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
            List<Stream> streams = streamMapper.list(topicId, queueId, null);
            Assertions.assertEquals(1, streams.size());
            Assertions.assertEquals(StreamState.CLOSED, streams.get(0).getState());
        }
    }

    @Test
    public void testCloseStream_NotFound() throws IOException, ExecutionException, InterruptedException {
        ControllerClient controllerClient = Mockito.mock(ControllerClient.class);
        ControllerConfig controllerConfig = Mockito.mock(ControllerConfig.class);
        Mockito.when(controllerConfig.nodeId()).thenReturn(2);
        Mockito.when(controllerConfig.scanIntervalInSecs()).thenReturn(1);
        Mockito.when(controllerConfig.leaseLifeSpanInSecs()).thenReturn(2);
        Mockito.when(controllerConfig.scanIntervalInSecs()).thenReturn(1);

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
            stream.setEpoch(1);
            stream.setState(StreamState.OPEN);
            stream.setStartOffset(0);
            stream.setSrcNodeId(srcNodeId);
            stream.setDstNodeId(dstNodeId);
            streamMapper.create(stream);
            streamId = stream.getId();

            RangeMapper rangeMapper = session.getMapper(RangeMapper.class);
            Range range = new Range();
            range.setRangeId(0);
            range.setStartOffset(0L);
            range.setBrokerId(1);
            range.setStreamId(streamId);
            range.setEpoch(1L);
            range.setEndOffset(100L);
            rangeMapper.create(range);

            session.commit();
        }

        try (MetadataStore metadataStore = new DefaultMetadataStore(controllerClient, getSessionFactory(), controllerConfig)) {
            metadataStore.start();
            Awaitility.await().with().pollInterval(100, TimeUnit.MILLISECONDS)
                .atMost(10, TimeUnit.SECONDS)
                .until(metadataStore::isLeader);

            try (ControllerTestServer testServer = new ControllerTestServer(0, new ControllerServiceImpl(metadataStore));
                 ControllerClient client = new GrpcControllerClient()
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
    public void testTrimStream() throws IOException, ControllerException, ExecutionException, InterruptedException {
        ControllerClient controllerClient = Mockito.mock(ControllerClient.class);
        ControllerConfig controllerConfig = Mockito.mock(ControllerConfig.class);
        Mockito.when(controllerConfig.nodeId()).thenReturn(2);
        Mockito.when(controllerConfig.scanIntervalInSecs()).thenReturn(1);
        Mockito.when(controllerConfig.leaseLifeSpanInSecs()).thenReturn(2);
        Mockito.when(controllerConfig.scanIntervalInSecs()).thenReturn(1);

        long topicId = 1;
        int queueId = 2;
        int srcNodeId = 1;
        int dstNodeId = 1;
        long streamId;
        long newStartOffset = 40;

        try (SqlSession session = getSessionFactory().openSession()) {
            StreamMapper streamMapper = session.getMapper(StreamMapper.class);

            Stream stream = new Stream();
            stream.setStreamRole(StreamRole.STREAM_ROLE_DATA);
            stream.setTopicId(topicId);
            stream.setQueueId(queueId);
            stream.setEpoch(1);
            stream.setState(StreamState.OPEN);
            stream.setStartOffset(0);
            stream.setRangeId(0);
            stream.setSrcNodeId(srcNodeId);
            stream.setDstNodeId(dstNodeId);
            streamMapper.create(stream);
            streamId = stream.getId();

            RangeMapper rangeMapper = session.getMapper(RangeMapper.class);
            Range range = new Range();
            range.setRangeId(0);
            range.setStartOffset(0L);
            range.setBrokerId(2);
            range.setStreamId(streamId);
            range.setEpoch(1L);
            range.setEndOffset(100L);
            rangeMapper.create(range);

            session.commit();
        }

        try (MetadataStore metadataStore = new DefaultMetadataStore(controllerClient, getSessionFactory(), controllerConfig)) {
            metadataStore.start();
            Awaitility.await().with().pollInterval(100, TimeUnit.MILLISECONDS)
                .atMost(10, TimeUnit.SECONDS)
                .until(metadataStore::isLeader);

            try (ControllerTestServer testServer = new ControllerTestServer(0, new ControllerServiceImpl(metadataStore));
                 ControllerClient client = new GrpcControllerClient()
            ) {
                testServer.start();
                int port = testServer.getPort();
                TrimStreamRequest request = TrimStreamRequest.newBuilder()
                    .setStreamId(streamId)
                    .setStreamEpoch(1)
                    .setNewStartOffset(newStartOffset)
                    .build();
                client.trimStream(String.format("localhost:%d", port), request).get();
            }
        }

        try (SqlSession session = getSessionFactory().openSession()) {
            StreamMapper streamMapper = session.getMapper(StreamMapper.class);
            List<Stream> streams = streamMapper.list(topicId, queueId, null);
            Assertions.assertEquals(1, streams.size());
            Assertions.assertEquals(newStartOffset, streams.get(0).getStartOffset());

            RangeMapper rangeMapper = session.getMapper(RangeMapper.class);
            List<Range> ranges = rangeMapper.list(null, streamId, null);
            Assertions.assertEquals(1, ranges.size());
            Assertions.assertEquals(newStartOffset, ranges.get(0).getStartOffset());
            Assertions.assertEquals(100, ranges.get(0).getEndOffset());
        }

    }

    @Test
    public void testTrimStream_WithS3Stream() throws IOException, ControllerException, ExecutionException, InterruptedException {
        ControllerClient controllerClient = Mockito.mock(ControllerClient.class);
        ControllerConfig controllerConfig = Mockito.mock(ControllerConfig.class);
        Mockito.when(controllerConfig.nodeId()).thenReturn(2);
        Mockito.when(controllerConfig.scanIntervalInSecs()).thenReturn(1);
        Mockito.when(controllerConfig.leaseLifeSpanInSecs()).thenReturn(2);
        Mockito.when(controllerConfig.scanIntervalInSecs()).thenReturn(1);

        long topicId = 1;
        int queueId = 2;
        int srcNodeId = 1;
        int dstNodeId = 2;
        long streamId;
        long objectId;
        long newStartOffset = 40L;

        try (SqlSession session = getSessionFactory().openSession()) {
            StreamMapper streamMapper = session.getMapper(StreamMapper.class);

            Stream stream = new Stream();
            stream.setStreamRole(StreamRole.STREAM_ROLE_DATA);
            stream.setTopicId(topicId);
            stream.setQueueId(queueId);
            stream.setEpoch(1);
            stream.setState(StreamState.OPEN);
            stream.setStartOffset(0);
            stream.setSrcNodeId(srcNodeId);
            stream.setDstNodeId(dstNodeId);
            streamMapper.create(stream);
            streamId = stream.getId();

            RangeMapper rangeMapper = session.getMapper(RangeMapper.class);
            Range range = new Range();
            range.setRangeId(0);
            range.setStartOffset(0L);
            range.setStreamId(streamId);
            range.setBrokerId(2);
            range.setEpoch(1L);
            range.setEndOffset(100L);
            rangeMapper.create(range);

            S3ObjectMapper s3ObjectMapper = session.getMapper(S3ObjectMapper.class);
            S3Object s3Object = new S3Object();
            s3Object.setStreamId(streamId);
            s3Object.setState(S3ObjectState.BOS_COMMITTED);
            s3Object.setObjectSize(1000L);
            s3ObjectMapper.create(s3Object);
            objectId = s3Object.getId();

            S3StreamObjectMapper s3StreamObjectMapper = session.getMapper(S3StreamObjectMapper.class);
            S3StreamObject s3StreamObject = new S3StreamObject();
            s3StreamObject.setBaseDataTimestamp(System.currentTimeMillis());
            s3StreamObject.setStreamId(streamId);
            s3StreamObject.setObjectId(objectId);
            s3StreamObject.setStartOffset(0L);
            s3StreamObject.setEndOffset(40L);
            s3StreamObject.setObjectSize(1000);
            s3StreamObjectMapper.create(s3StreamObject);

            session.commit();
        }

        try (MetadataStore metadataStore = new DefaultMetadataStore(controllerClient, getSessionFactory(), controllerConfig)) {
            metadataStore.start();
            Awaitility.await().with().pollInterval(100, TimeUnit.MILLISECONDS)
                .atMost(10, TimeUnit.SECONDS)
                .until(metadataStore::isLeader);

            try (ControllerTestServer testServer = new ControllerTestServer(0, new ControllerServiceImpl(metadataStore));
                 ControllerClient client = new GrpcControllerClient()
            ) {
                testServer.start();
                int port = testServer.getPort();
                TrimStreamRequest request = TrimStreamRequest.newBuilder()
                    .setStreamId(streamId)
                    .setStreamEpoch(1)
                    .setNewStartOffset(newStartOffset)
                    .build();
                client.trimStream(String.format("localhost:%d", port), request).get();
            }
        }

        try (SqlSession session = getSessionFactory().openSession()) {
            StreamMapper streamMapper = session.getMapper(StreamMapper.class);
            List<Stream> streams = streamMapper.list(topicId, queueId, null);
            Assertions.assertEquals(1, streams.size());
            Assertions.assertEquals(newStartOffset, streams.get(0).getStartOffset());

            RangeMapper rangeMapper = session.getMapper(RangeMapper.class);
            List<Range> ranges = rangeMapper.list(null, streamId, null);
            Assertions.assertEquals(1, ranges.size());
            Assertions.assertEquals(newStartOffset, ranges.get(0).getStartOffset());
            Assertions.assertEquals(100, ranges.get(0).getEndOffset());

            S3StreamObjectMapper s3StreamObjectMapper = session.getMapper(S3StreamObjectMapper.class);
            List<S3StreamObject> objects = s3StreamObjectMapper.listByStreamId(streamId);
            Assertions.assertEquals(0, objects.size());

            S3ObjectMapper s3ObjectMapper = session.getMapper(S3ObjectMapper.class);
            S3Object s3Object = s3ObjectMapper.getById(objectId);
            long cost = s3Object.getMarkedForDeletionTimestamp() - System.currentTimeMillis();
            if (cost > 5 * 60) {
                Assertions.fail();
            }
        }
    }

    @Test
    public void testTrimStream_WithS3WAL() throws IOException, ControllerException, ExecutionException, InterruptedException {
        ControllerClient controllerClient = Mockito.mock(ControllerClient.class);
        ControllerConfig controllerConfig = Mockito.mock(ControllerConfig.class);
        Mockito.when(controllerConfig.nodeId()).thenReturn(2);
        Mockito.when(controllerConfig.scanIntervalInSecs()).thenReturn(1);
        Mockito.when(controllerConfig.leaseLifeSpanInSecs()).thenReturn(2);
        Mockito.when(controllerConfig.scanIntervalInSecs()).thenReturn(1);

        long topicId = 1;
        int queueId = 2;
        int srcNodeId = 1;
        int dstNodeId = 2;
        long streamId;
        long objectId = 2;
        long newStartOffset = 40L;
        String expectSubStream = """
            {
              "1234567890": {
                "streamId_": 1234567890,
                "startOffset_": 0,
                "endOffset_": 10
              }}""";

        try (SqlSession session = getSessionFactory().openSession()) {
            StreamMapper streamMapper = session.getMapper(StreamMapper.class);

            Stream stream = new Stream();
            stream.setStreamRole(StreamRole.STREAM_ROLE_DATA);
            stream.setTopicId(topicId);
            stream.setQueueId(queueId);
            stream.setEpoch(1);
            stream.setState(StreamState.OPEN);
            stream.setStartOffset(0);
            stream.setSrcNodeId(srcNodeId);
            stream.setDstNodeId(dstNodeId);
            streamMapper.create(stream);
            streamId = stream.getId();

            RangeMapper rangeMapper = session.getMapper(RangeMapper.class);
            Range range = new Range();
            range.setRangeId(0);
            range.setStartOffset(0L);
            range.setStreamId(streamId);
            range.setBrokerId(2);
            range.setEpoch(1L);
            range.setEndOffset(100L);
            rangeMapper.create(range);

            S3ObjectMapper s3ObjectMapper = session.getMapper(S3ObjectMapper.class);
            S3Object s3Object = new S3Object();
            s3Object.setStreamId(streamId);
            s3Object.setState(S3ObjectState.BOS_COMMITTED);
            s3Object.setObjectSize(1000L);
            s3ObjectMapper.create(s3Object);
            objectId = s3Object.getId();

            String replacedJson = expectSubStream.replace("1234567890", streamId + "");

            S3WALObjectMapper s3WALObjectMapper = session.getMapper(S3WALObjectMapper.class);
            S3WALObject s3WALObject = new S3WALObject();
            s3WALObject.setObjectId(objectId);
            s3WALObject.setObjectSize(500);
            s3WALObject.setSequenceId(111);
            s3WALObject.setSubStreams(replacedJson);
            s3WALObject.setBrokerId(2);
            s3WALObjectMapper.create(s3WALObject);
            session.commit();
        }

        try (MetadataStore metadataStore = new DefaultMetadataStore(controllerClient, getSessionFactory(), controllerConfig)) {
            metadataStore.start();
            Awaitility.await().with().pollInterval(100, TimeUnit.MILLISECONDS)
                .atMost(10, TimeUnit.SECONDS)
                .until(metadataStore::isLeader);

            try (ControllerTestServer testServer = new ControllerTestServer(0, new ControllerServiceImpl(metadataStore));
                 ControllerClient client = new GrpcControllerClient()
            ) {
                testServer.start();
                int port = testServer.getPort();
                TrimStreamRequest request = TrimStreamRequest.newBuilder()
                    .setStreamId(streamId)
                    .setStreamEpoch(1)
                    .setNewStartOffset(newStartOffset)
                    .build();
                client.trimStream(String.format("localhost:%d", port), request).get();
            }
        }

        try (SqlSession session = getSessionFactory().openSession()) {
            StreamMapper streamMapper = session.getMapper(StreamMapper.class);
            List<Stream> streams = streamMapper.list(topicId, queueId, null);
            Assertions.assertEquals(1, streams.size());
            Assertions.assertEquals(newStartOffset, streams.get(0).getStartOffset());

            RangeMapper rangeMapper = session.getMapper(RangeMapper.class);
            List<Range> ranges = rangeMapper.list(null, streamId, null);
            Assertions.assertEquals(1, ranges.size());
            Assertions.assertEquals(newStartOffset, ranges.get(0).getStartOffset());
            Assertions.assertEquals(100, ranges.get(0).getEndOffset());

            S3WALObjectMapper s3WALObjectMapper = session.getMapper(S3WALObjectMapper.class);
            S3WALObject object = s3WALObjectMapper.getByObjectId(objectId);
            Assertions.assertEquals(500, object.getObjectSize());
            Assertions.assertEquals(111, object.getSequenceId());

            S3ObjectMapper s3ObjectMapper = session.getMapper(S3ObjectMapper.class);
            S3Object s3Object = s3ObjectMapper.getById(objectId);
            long cost = s3Object.getMarkedForDeletionTimestamp() - System.currentTimeMillis();
            if (cost > 5 * 60) {
                Assertions.fail();
            }
        }
    }

    @Test
    public void testTrimStream_WithRange() throws IOException, ControllerException, ExecutionException, InterruptedException {
        ControllerClient controllerClient = Mockito.mock(ControllerClient.class);
        ControllerConfig controllerConfig = Mockito.mock(ControllerConfig.class);
        Mockito.when(controllerConfig.nodeId()).thenReturn(2);
        Mockito.when(controllerConfig.scanIntervalInSecs()).thenReturn(1);
        Mockito.when(controllerConfig.leaseLifeSpanInSecs()).thenReturn(2);
        Mockito.when(controllerConfig.scanIntervalInSecs()).thenReturn(1);

        long topicId = 1;
        int queueId = 2;
        int srcNodeId = 1;
        int dstNodeId = 2;
        long streamId;
        long objectId = 2;
        long newStartOffset = 60;

        try (SqlSession session = getSessionFactory().openSession()) {
            StreamMapper streamMapper = session.getMapper(StreamMapper.class);

            Stream stream = new Stream();
            stream.setStreamRole(StreamRole.STREAM_ROLE_DATA);
            stream.setTopicId(topicId);
            stream.setQueueId(queueId);
            stream.setEpoch(1);
            stream.setState(StreamState.OPEN);
            stream.setStartOffset(0);
            stream.setSrcNodeId(srcNodeId);
            stream.setDstNodeId(dstNodeId);
            streamMapper.create(stream);
            streamId = stream.getId();

            RangeMapper rangeMapper = session.getMapper(RangeMapper.class);
            Range range = new Range();
            range.setRangeId(0);
            range.setStartOffset(0L);
            range.setStreamId(streamId);
            range.setBrokerId(2);
            range.setEpoch(1L);
            range.setEndOffset(30L);
            rangeMapper.create(range);

            Range range1 = new Range();
            range1.setRangeId(1);
            range1.setStartOffset(50L);
            range1.setStreamId(streamId);
            range1.setBrokerId(2);
            range1.setEpoch(1L);
            range1.setEndOffset(100L);
            rangeMapper.create(range1);

            S3ObjectMapper s3ObjectMapper = session.getMapper(S3ObjectMapper.class);
            S3Object s3Object = new S3Object();
            s3Object.setStreamId(streamId);
            s3Object.setState(S3ObjectState.BOS_COMMITTED);
            s3Object.setObjectSize(1000L);
            s3ObjectMapper.create(s3Object);
            objectId = s3Object.getId();

            S3StreamObjectMapper s3StreamObjectMapper = session.getMapper(S3StreamObjectMapper.class);
            S3StreamObject s3StreamObject = new S3StreamObject();
            s3StreamObject.setBaseDataTimestamp(System.currentTimeMillis());
            s3StreamObject.setStreamId(streamId);
            s3StreamObject.setObjectId(objectId);
            s3StreamObject.setStartOffset(0L);
            s3StreamObject.setEndOffset(40L);
            s3StreamObject.setObjectSize(1000);
            s3StreamObjectMapper.create(s3StreamObject);

            session.commit();
        }

        try (MetadataStore metadataStore = new DefaultMetadataStore(controllerClient, getSessionFactory(), controllerConfig)) {
            metadataStore.start();
            Awaitility.await().with().pollInterval(100, TimeUnit.MILLISECONDS)
                .atMost(10, TimeUnit.SECONDS)
                .until(metadataStore::isLeader);

            try (ControllerTestServer testServer = new ControllerTestServer(0, new ControllerServiceImpl(metadataStore));
                 ControllerClient client = new GrpcControllerClient()
            ) {
                testServer.start();
                int port = testServer.getPort();
                TrimStreamRequest request = TrimStreamRequest.newBuilder()
                    .setStreamId(streamId)
                    .setStreamEpoch(1)
                    .setNewStartOffset(newStartOffset)
                    .build();
                client.trimStream(String.format("localhost:%d", port), request).get();
            }
        }

        try (SqlSession session = getSessionFactory().openSession()) {
            StreamMapper streamMapper = session.getMapper(StreamMapper.class);
            List<Stream> streams = streamMapper.list(topicId, queueId, null);
            Assertions.assertEquals(1, streams.size());
            Assertions.assertEquals(newStartOffset, streams.get(0).getStartOffset());

            RangeMapper rangeMapper = session.getMapper(RangeMapper.class);
            List<Range> ranges = rangeMapper.list(null, streamId, null);
            Assertions.assertEquals(1, ranges.size());
            Assertions.assertEquals(newStartOffset, ranges.get(0).getStartOffset());
            Assertions.assertEquals(100, ranges.get(0).getEndOffset());

            S3StreamObjectMapper s3StreamObjectMapper = session.getMapper(S3StreamObjectMapper.class);
            List<S3StreamObject> objects = s3StreamObjectMapper.listByStreamId(streamId);
            Assertions.assertEquals(0, objects.size());

            S3ObjectMapper s3ObjectMapper = session.getMapper(S3ObjectMapper.class);
            S3Object s3Object = s3ObjectMapper.getById(objectId);
            long cost = s3Object.getMarkedForDeletionTimestamp() - System.currentTimeMillis();
            if (cost > 5 * 60) {
                Assertions.fail();
            }
        }
    }

    @Test
    public void testListOpenStreams() throws IOException, ExecutionException, InterruptedException {
        ControllerClient controllerClient = Mockito.mock(ControllerClient.class);
        ControllerConfig controllerConfig = Mockito.mock(ControllerConfig.class);
        Mockito.when(controllerConfig.nodeId()).thenReturn(2);
        Mockito.when(controllerConfig.scanIntervalInSecs()).thenReturn(1);
        Mockito.when(controllerConfig.leaseLifeSpanInSecs()).thenReturn(2);
        Mockito.when(controllerConfig.scanIntervalInSecs()).thenReturn(1);

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

        try (MetadataStore metadataStore = new DefaultMetadataStore(controllerClient, getSessionFactory(), controllerConfig)) {
            metadataStore.start();
            Awaitility.await().with().pollInterval(100, TimeUnit.MILLISECONDS)
                .atMost(10, TimeUnit.SECONDS)
                .until(metadataStore::isLeader);

            try (ControllerTestServer testServer = new ControllerTestServer(0, new ControllerServiceImpl(metadataStore));
                 ControllerClient client = new GrpcControllerClient()
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
    public void test3StreamObjects_2PC() throws IOException, ExecutionException, InterruptedException {
        ControllerClient controllerClient = Mockito.mock(ControllerClient.class);
        ControllerConfig controllerConfig = Mockito.mock(ControllerConfig.class);
        Mockito.when(controllerConfig.nodeId()).thenReturn(2);
        Mockito.when(controllerConfig.scanIntervalInSecs()).thenReturn(1);
        Mockito.when(controllerConfig.leaseLifeSpanInSecs()).thenReturn(2);
        Mockito.when(controllerConfig.scanIntervalInSecs()).thenReturn(1);
        long objectId, streamId = 1;

        try (MetadataStore metadataStore = new DefaultMetadataStore(controllerClient, getSessionFactory(), controllerConfig)) {
            metadataStore.start();
            Awaitility.await().with().pollInterval(100, TimeUnit.MILLISECONDS)
                .atMost(10, TimeUnit.SECONDS)
                .until(metadataStore::isLeader);

            try (ControllerTestServer testServer = new ControllerTestServer(0, new ControllerServiceImpl(metadataStore));
                 ControllerClient client = new GrpcControllerClient()
            ) {
                testServer.start();
                int port = testServer.getPort();
                PrepareS3ObjectsRequest request = PrepareS3ObjectsRequest.newBuilder()
                    .setPreparedCount(3)
                    .setTimeToLiveMinutes(5)
                    .build();

                PrepareS3ObjectsReply reply = client.prepareS3Objects(String.format("localhost:%d", port), request).get();
                objectId = reply.getFirstObjectId();
            }

            apache.rocketmq.controller.v1.S3StreamObject s3StreamObject = apache.rocketmq.controller.v1.S3StreamObject.newBuilder()
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

            List<Long> compactedObjects = new ArrayList<>();
            compactedObjects.add(objectId);
            compactedObjects.add(objectId + 1);

            try (ControllerTestServer testServer = new ControllerTestServer(0, new ControllerServiceImpl(metadataStore));
                 ControllerClient client = new GrpcControllerClient()
            ) {
                testServer.start();
                int port = testServer.getPort();

                CommitStreamObjectRequest request1 = CommitStreamObjectRequest.newBuilder()
                    .setS3StreamObject(s3StreamObject)
                    .addAllCompactedObjectIds(compactedObjects)
                    .build();

                client.commitStreamObject(String.format("localhost:%d", port), request1).get();

            }

            long time = System.currentTimeMillis();
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

    }

    @Test
    public void test3WALObjects_2PC() throws IOException, ExecutionException, InterruptedException {
        ControllerClient controllerClient = Mockito.mock(ControllerClient.class);
        ControllerConfig controllerConfig = Mockito.mock(ControllerConfig.class);
        Mockito.when(controllerConfig.nodeId()).thenReturn(2);
        Mockito.when(controllerConfig.scanIntervalInSecs()).thenReturn(1);
        Mockito.when(controllerConfig.leaseLifeSpanInSecs()).thenReturn(2);
        Mockito.when(controllerConfig.scanIntervalInSecs()).thenReturn(1);
        long objectId, streamId = 1;
        int nodeId = 2;

        try (MetadataStore metadataStore = new DefaultMetadataStore(controllerClient, getSessionFactory(), controllerConfig)) {
            metadataStore.start();
            Awaitility.await().with().pollInterval(100, TimeUnit.MILLISECONDS)
                .atMost(10, TimeUnit.SECONDS)
                .until(metadataStore::isLeader);

            try (ControllerTestServer testServer = new ControllerTestServer(0, new ControllerServiceImpl(metadataStore));
                 ControllerClient client = new GrpcControllerClient()
            ) {
                testServer.start();
                int port = testServer.getPort();
                PrepareS3ObjectsRequest request = PrepareS3ObjectsRequest.newBuilder()
                    .setPreparedCount(5)
                    .setTimeToLiveMinutes(5)
                    .build();

                PrepareS3ObjectsReply reply = client.prepareS3Objects(String.format("localhost:%d", port), request).get();
                objectId = reply.getFirstObjectId();
            }

            apache.rocketmq.controller.v1.S3WALObject walObject = apache.rocketmq.controller.v1.S3WALObject.newBuilder()
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

            try (ControllerTestServer testServer = new ControllerTestServer(0, new ControllerServiceImpl(metadataStore));
                 ControllerClient client = new GrpcControllerClient()
            ) {
                testServer.start();
                int port = testServer.getPort();

                List<apache.rocketmq.controller.v1.S3StreamObject> s3StreamObjects = metadataStore.listStreamObjects(streamId, 111, 334, 2).get();

                CommitWALObjectRequest request = CommitWALObjectRequest.newBuilder()
                    .setS3WalObject(walObject)
                    .addAllS3StreamObjects(s3StreamObjects)
                    .addAllCompactedObjectIds(compactedObjects)
                    .build();

                client.commitWALObject(String.format("localhost:%d", port), request).get();
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


    @Test
    public void testCreateTopic_OpenStream_CloseStream() throws IOException, ExecutionException, InterruptedException, ControllerException {
        ControllerClient controllerClient = Mockito.mock(ControllerClient.class);
        ControllerConfig controllerConfig = Mockito.mock(ControllerConfig.class);
        Mockito.when(controllerConfig.nodeId()).thenReturn(1);
        Mockito.when(controllerConfig.scanIntervalInSecs()).thenReturn(1);
        Mockito.when(controllerConfig.leaseLifeSpanInSecs()).thenReturn(2);
        Mockito.when(controllerConfig.scanIntervalInSecs()).thenReturn(1);

        long streamId, streamEpoch;
        int rangeId;
        int nodeId = 1;

        String topicName = "t1";
        int queueNum = 4;
        List<MessageType> messageTypeList = new ArrayList<>();
        messageTypeList.add(MessageType.NORMAL);
        messageTypeList.add(MessageType.FIFO);
        messageTypeList.add(MessageType.DELAY);

        try (MetadataStore metadataStore = new DefaultMetadataStore(controllerClient, getSessionFactory(), controllerConfig)) {
            metadataStore.start();
            Awaitility.await().with().pollInterval(100, TimeUnit.MILLISECONDS)
                .atMost(10, TimeUnit.SECONDS)
                .until(metadataStore::isLeader);

            try (ControllerTestServer testServer = new ControllerTestServer(0, new ControllerServiceImpl(metadataStore));
                 ControllerClient client = new GrpcControllerClient()
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
                ((DefaultMetadataStore) metadataStore).addBrokerNode(node);

                Long topicId = client.createTopic(String.format("localhost:%d", port), CreateTopicRequest.newBuilder()
                    .setTopic(topicName)
                    .setCount(queueNum)
                    .addAllAcceptMessageTypes(messageTypeList)
                    .build()).get();

                StreamMetadata metadata = metadataStore.getStream(topicId, 0, null, StreamRole.STREAM_ROLE_DATA).get();
                streamId = metadata.getStreamId();
                streamEpoch = metadata.getEpoch();
                OpenStreamRequest request = OpenStreamRequest.newBuilder()
                    .setStreamId(metadata.getStreamId())
                    .setStreamEpoch(metadata.getEpoch())
                    .setBrokerId(nodeId)
                    .build();

                client.openStream(String.format("localhost:%d", port), request).get();
                OpenStreamReply reply = client.openStream(String.format("localhost:%d", port), request).get();
                StreamMetadata openStream = reply.getStreamMetadata();
                Assertions.assertEquals(0, openStream.getStartOffset());
                Assertions.assertEquals(streamEpoch + 1, openStream.getEpoch());
                Assertions.assertEquals(0, openStream.getRangeId());
                Assertions.assertEquals(StreamState.OPEN, openStream.getState());
                rangeId = openStream.getRangeId();

                client.closeStream(String.format("localhost:%d", port), CloseStreamRequest.newBuilder()
                    .setStreamId(streamId)
                    .setStreamEpoch(streamEpoch + 1)
                    .setBrokerId(nodeId)
                    .build()).get();

                client.closeStream(String.format("localhost:%d", port), CloseStreamRequest.newBuilder()
                    .setStreamId(streamId)
                    .setStreamEpoch(streamEpoch + 1)
                    .setBrokerId(nodeId)
                    .build()).get();
            }
        }
        long targetStreamEpoch = streamEpoch + 1;
        try (SqlSession session = getSessionFactory().openSession()) {
            StreamMapper streamMapper = session.getMapper(StreamMapper.class);
            RangeMapper rangeMapper = session.getMapper(RangeMapper.class);

            Stream stream = streamMapper.getByStreamId(streamId);
            Assertions.assertEquals(streamId, stream.getId());
            Assertions.assertEquals(0, stream.getStartOffset());
            Assertions.assertEquals(targetStreamEpoch, stream.getEpoch());
            Assertions.assertEquals(0, stream.getRangeId());
            Assertions.assertEquals(StreamState.CLOSED, stream.getState());

            Range range = rangeMapper.get(rangeId, streamId, null);
            Assertions.assertEquals(0, range.getRangeId());
            Assertions.assertEquals(streamId, range.getStreamId());
            Assertions.assertEquals(targetStreamEpoch, range.getEpoch());
            Assertions.assertEquals(0, range.getStartOffset());
            Assertions.assertEquals(0, range.getEndOffset());
        }
    }
}