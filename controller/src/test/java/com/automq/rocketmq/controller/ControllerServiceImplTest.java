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
import apache.rocketmq.controller.v1.Code;
import apache.rocketmq.controller.v1.CommitStreamObjectReply;
import apache.rocketmq.controller.v1.CommitStreamObjectRequest;
import apache.rocketmq.controller.v1.CommitWALObjectReply;
import apache.rocketmq.controller.v1.CommitWALObjectRequest;
import apache.rocketmq.controller.v1.ConsumerGroup;
import apache.rocketmq.controller.v1.ControllerServiceGrpc;
import apache.rocketmq.controller.v1.CreateGroupReply;
import apache.rocketmq.controller.v1.CreateGroupRequest;
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
import apache.rocketmq.controller.v1.PrepareS3ObjectsReply;
import apache.rocketmq.controller.v1.PrepareS3ObjectsRequest;
import apache.rocketmq.controller.v1.S3ObjectState;
import apache.rocketmq.controller.v1.StreamMetadata;
import apache.rocketmq.controller.v1.StreamRole;
import apache.rocketmq.controller.v1.StreamState;
import apache.rocketmq.controller.v1.SubStream;
import apache.rocketmq.controller.v1.TopicStatus;
import apache.rocketmq.controller.v1.TrimStreamRequest;
import apache.rocketmq.controller.v1.UpdateTopicReply;
import apache.rocketmq.controller.v1.UpdateTopicRequest;
import com.automq.rocketmq.common.system.StreamConstants;
import com.automq.rocketmq.controller.exception.ControllerException;
import com.automq.rocketmq.controller.metadata.ControllerClient;
import com.automq.rocketmq.controller.metadata.DatabaseTestBase;
import com.automq.rocketmq.controller.metadata.GrpcControllerClient;
import com.automq.rocketmq.controller.metadata.MetadataStore;
import com.automq.rocketmq.controller.metadata.database.DefaultMetadataStore;
import com.automq.rocketmq.controller.metadata.database.dao.Group;
import com.automq.rocketmq.controller.metadata.database.dao.GroupCriteria;
import com.automq.rocketmq.controller.metadata.database.dao.GroupProgress;
import com.automq.rocketmq.controller.metadata.database.dao.Node;
import com.automq.rocketmq.controller.metadata.database.dao.QueueAssignment;
import com.automq.rocketmq.controller.metadata.database.dao.Range;
import com.automq.rocketmq.controller.metadata.database.dao.S3Object;
import com.automq.rocketmq.controller.metadata.database.dao.S3StreamObject;
import com.automq.rocketmq.controller.metadata.database.dao.S3WalObject;
import com.automq.rocketmq.controller.metadata.database.dao.Stream;
import com.automq.rocketmq.controller.metadata.database.dao.Topic;
import com.automq.rocketmq.controller.metadata.database.mapper.GroupMapper;
import com.automq.rocketmq.controller.metadata.database.mapper.GroupProgressMapper;
import com.automq.rocketmq.controller.metadata.database.mapper.NodeMapper;
import com.automq.rocketmq.controller.metadata.database.mapper.QueueAssignmentMapper;
import com.automq.rocketmq.controller.metadata.database.mapper.RangeMapper;
import com.automq.rocketmq.controller.metadata.database.mapper.S3ObjectMapper;
import com.automq.rocketmq.controller.metadata.database.mapper.S3StreamObjectMapper;
import com.automq.rocketmq.controller.metadata.database.mapper.S3WalObjectMapper;
import com.automq.rocketmq.controller.metadata.database.mapper.StreamMapper;
import com.automq.rocketmq.controller.metadata.database.mapper.TopicMapper;
import com.google.protobuf.util.JsonFormat;
import io.grpc.Grpc;
import io.grpc.InsecureChannelCredentials;
import io.grpc.ManagedChannel;
import io.grpc.stub.StreamObserver;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
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
            node.setAddress("localhost:1234");
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
            topic.setAcceptMessageTypes("abc");
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
                    .setMaxRetryAttempt(5)
                    .build();

                String target = String.format("localhost:%d", port);

                CreateGroupReply reply = client.createGroup(target, request).get();
                Assertions.assertEquals(reply.getStatus().getCode(), Code.OK);
                Assertions.assertTrue(reply.getGroupId() > 0);

                // Test duplication
                client.createGroup(target, request).whenComplete(
                    (res, e) -> Assertions.assertEquals(ExecutionException.class, e.getClass()));
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
            List<Stream> streams = streamMapper.list(topicId, queueId, null);
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
            List<Stream> streams = streamMapper.list(topicId, queueId, null);
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
    public void testTrimStream() throws IOException, ExecutionException, InterruptedException {
        ControllerClient controllerClient = Mockito.mock(ControllerClient.class);

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
            stream.setEpoch(1L);
            stream.setState(StreamState.OPEN);
            stream.setStartOffset(0L);
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
    public void testTrimStream_WithS3Stream() throws IOException, ExecutionException, InterruptedException {
        ControllerClient controllerClient = Mockito.mock(ControllerClient.class);

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
            range.setStreamId(streamId);
            range.setNodeId(2);
            range.setEpoch(1L);
            range.setEndOffset(100L);
            rangeMapper.create(range);

            S3ObjectMapper s3ObjectMapper = session.getMapper(S3ObjectMapper.class);
            S3Object s3Object = new S3Object();
            s3Object.setId(nextS3ObjectId());
            s3Object.setStreamId(streamId);
            s3Object.setState(S3ObjectState.BOS_COMMITTED);
            s3Object.setObjectSize(1000L);
            Calendar calendar = Calendar.getInstance();
            calendar.add(Calendar.MINUTE, 5);
            s3Object.setExpiredTimestamp(calendar.getTime());
            s3ObjectMapper.prepare(s3Object);
            objectId = s3Object.getId();

            S3StreamObjectMapper s3StreamObjectMapper = session.getMapper(S3StreamObjectMapper.class);
            S3StreamObject s3StreamObject = new S3StreamObject();
            s3StreamObject.setBaseDataTimestamp(System.currentTimeMillis());
            s3StreamObject.setStreamId(streamId);
            s3StreamObject.setObjectId(objectId);
            s3StreamObject.setStartOffset(0L);
            s3StreamObject.setEndOffset(40L);
            s3StreamObject.setObjectSize(1000L);
            s3StreamObjectMapper.create(s3StreamObject);

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
        }
    }

    @Test
    public void testTrimStream_WithS3WAL() throws IOException, ExecutionException, InterruptedException {
        ControllerClient controllerClient = Mockito.mock(ControllerClient.class);

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
            range.setStreamId(streamId);
            range.setNodeId(2);
            range.setEpoch(1L);
            range.setEndOffset(100L);
            rangeMapper.create(range);

            S3ObjectMapper s3ObjectMapper = session.getMapper(S3ObjectMapper.class);
            S3Object s3Object = new S3Object();
            s3Object.setId(nextS3ObjectId());
            s3Object.setStreamId(streamId);
            s3Object.setState(S3ObjectState.BOS_COMMITTED);
            s3Object.setObjectSize(1000L);
            Calendar calendar = Calendar.getInstance();
            calendar.add(Calendar.MINUTE, 5);
            s3Object.setExpiredTimestamp(calendar.getTime());
            s3ObjectMapper.prepare(s3Object);
            objectId = s3Object.getId();

            S3WalObjectMapper s3WALObjectMapper = session.getMapper(S3WalObjectMapper.class);
            buildS3WalObjs(objectId, 1).stream()
                .map(s3WalObject -> {
                    Map<Long, SubStream> subStreams = buildWalSubStreams(1, 0, 10);
                    s3WalObject.setSubStreams(gson.toJson(subStreams));
                    return s3WalObject;
                }).forEach(s3WALObjectMapper::create);
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

            S3WalObjectMapper s3WALObjectMapper = session.getMapper(S3WalObjectMapper.class);
            S3WalObject object = s3WALObjectMapper.getByObjectId(objectId);
            Assertions.assertEquals(100, object.getObjectSize());
            Assertions.assertEquals(objectId, object.getSequenceId());
        }
    }

    @Test
    public void testTrimStream_WithRange() throws IOException, ExecutionException, InterruptedException {
        ControllerClient controllerClient = Mockito.mock(ControllerClient.class);

        long topicId = 1;
        int queueId = 2;
        int srcNodeId = 1;
        int dstNodeId = 2;
        long streamId;
        long objectId;
        long newStartOffset = 60;

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
            range.setStreamId(streamId);
            range.setNodeId(2);
            range.setEpoch(1L);
            range.setEndOffset(30L);
            rangeMapper.create(range);

            Range range1 = new Range();
            range1.setRangeId(1);
            range1.setStartOffset(50L);
            range1.setStreamId(streamId);
            range1.setNodeId(2);
            range1.setEpoch(1L);
            range1.setEndOffset(100L);
            rangeMapper.create(range1);

            S3ObjectMapper s3ObjectMapper = session.getMapper(S3ObjectMapper.class);
            S3Object s3Object = new S3Object();
            s3Object.setId(nextS3ObjectId());
            s3Object.setStreamId(streamId);
            s3Object.setState(S3ObjectState.BOS_COMMITTED);
            s3Object.setObjectSize(1000L);
            Calendar calendar = Calendar.getInstance();
            calendar.add(Calendar.MINUTE, 5);
            s3Object.setExpiredTimestamp(calendar.getTime());
            s3ObjectMapper.prepare(s3Object);
            objectId = s3Object.getId();

            S3StreamObjectMapper s3StreamObjectMapper = session.getMapper(S3StreamObjectMapper.class);
            buildS3StreamObjs(objectId, 1, 0, 40).forEach(s3StreamObjectMapper::create);
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
    public void test3StreamObjects_2PC() throws IOException, ExecutionException, InterruptedException {
        ControllerClient controllerClient = Mockito.mock(ControllerClient.class);
        long objectId, streamId = 1;

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
                PrepareS3ObjectsRequest request = PrepareS3ObjectsRequest.newBuilder()
                    .setPreparedCount(3)
                    .setTimeToLiveMinutes(5)
                    .build();

                PrepareS3ObjectsReply reply = client.prepareS3Objects(String.format("localhost:%d", port), request).get();
                objectId = reply.getFirstObjectId();
            }

            try (SqlSession session = this.getSessionFactory().openSession()) {
                S3StreamObjectMapper s3StreamObjectMapper = session.getMapper(S3StreamObjectMapper.class);
                buildS3StreamObjs(objectId,2 ,1234, 1234).forEach(s3StreamObjectMapper::create);
                session.commit();
            }

            apache.rocketmq.controller.v1.S3StreamObject s3StreamObject = apache.rocketmq.controller.v1.S3StreamObject.newBuilder()
                .setObjectId(objectId + 2)
                .setStreamId(streamId)
                .setObjectSize(111L)
                .build();

            List<Long> compactedObjects = new ArrayList<>();
            compactedObjects.add(objectId);
            compactedObjects.add(objectId + 1);

            try (ControllerTestServer testServer = new ControllerTestServer(0, new ControllerServiceImpl(metadataStore));
                 ControllerClient client = new GrpcControllerClient(config)
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
                    Assertions.assertEquals(S3ObjectState.BOS_WILL_DELETE, object.getState());
                }

                S3StreamObjectMapper s3StreamObjectMapper = session.getMapper(S3StreamObjectMapper.class);
                for (long index = objectId; index < objectId + 2; index++) {
                    com.automq.rocketmq.controller.metadata.database.dao.S3StreamObject object = s3StreamObjectMapper.getByObjectId(index);
                    Assertions.assertNull(object);
                }

                com.automq.rocketmq.controller.metadata.database.dao.S3StreamObject object = s3StreamObjectMapper.getByObjectId(objectId + 2);
                Assertions.assertTrue(object.getBaseDataTimestamp() > 0);
                Assertions.assertTrue(object.getCommittedTimestamp() > 0);
            }
        }

    }

    @Test
    public void test3StreamObjects_2PC_Expired() throws IOException, ExecutionException, InterruptedException {
        ControllerClient controllerClient = Mockito.mock(ControllerClient.class);
        long objectId, streamId = 1;

        try (MetadataStore metadataStore = new DefaultMetadataStore(controllerClient, getSessionFactory(), config)) {
            metadataStore.start();
            Awaitility.await().with().pollInterval(100, TimeUnit.MILLISECONDS)
                .atMost(10, TimeUnit.SECONDS)
                .until(metadataStore::isLeader);

            try (SqlSession session = this.getSessionFactory().openSession()) {
                S3ObjectMapper s3ObjectMapper = session.getMapper(S3ObjectMapper.class);
                S3Object s3Object = new S3Object();
                s3Object.setId(nextS3ObjectId());
                s3Object.setStreamId(streamId);
                s3Object.setState(S3ObjectState.BOS_COMMITTED);
                s3Object.setExpiredTimestamp(new Date());
                s3ObjectMapper.prepare(s3Object);
                objectId = s3Object.getId();

                S3Object s3Object1 = new S3Object();
                s3Object1.setId(nextS3ObjectId());
                s3Object1.setStreamId(streamId);
                s3Object1.setState(S3ObjectState.BOS_PREPARED);
                s3Object1.setExpiredTimestamp(new Date());
                s3ObjectMapper.prepare(s3Object1);

                S3StreamObjectMapper s3StreamObjectMapper = session.getMapper(S3StreamObjectMapper.class);
                buildS3StreamObjs(objectId,1 ,1234, 1234).forEach(s3StreamObjectMapper::create);
                session.commit();
            }

            apache.rocketmq.controller.v1.S3StreamObject s3StreamObject = apache.rocketmq.controller.v1.S3StreamObject.newBuilder()
                .setObjectId(objectId + 1)
                .setStreamId(streamId)
                .setObjectSize(111L)
                .build();

            List<Long> compactedObjects = new ArrayList<>();
            compactedObjects.add(objectId);

            try (ControllerTestServer testServer = new ControllerTestServer(0, new ControllerServiceImpl(metadataStore));
                 ControllerClient client = new GrpcControllerClient(config)
            ) {
                testServer.start();
                int port = testServer.getPort();

                CommitStreamObjectRequest request = CommitStreamObjectRequest.newBuilder()
                    .setS3StreamObject(s3StreamObject)
                    .addAllCompactedObjectIds(compactedObjects)
                    .build();

                CommitStreamObjectReply commitStreamObjectReply = client.commitStreamObject(String.format("localhost:%d", port), request).get();
                Assertions.assertEquals(Code.ILLEGAL_STATE, commitStreamObjectReply.getStatus().getCode());
            }
        }
    }

    @Test
    public void test3StreamObjects_2PC_NoCompacted() throws IOException, ExecutionException, InterruptedException {
        ControllerClient controllerClient = Mockito.mock(ControllerClient.class);
        long objectId, streamId = 1;

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

            try (ControllerTestServer testServer = new ControllerTestServer(0, new ControllerServiceImpl(metadataStore));
                 ControllerClient client = new GrpcControllerClient(config)
            ) {
                testServer.start();
                int port = testServer.getPort();

                CommitStreamObjectRequest request1 = CommitStreamObjectRequest.newBuilder()
                    .setS3StreamObject(s3StreamObject)
                    .addAllCompactedObjectIds(Collections.emptyList())
                    .build();

                client.commitStreamObject(String.format("localhost:%d", port), request1).get();

            }

            long time = System.currentTimeMillis();
            try (SqlSession session = getSessionFactory().openSession()) {
                S3ObjectMapper s3ObjectMapper = session.getMapper(S3ObjectMapper.class);
                S3StreamObjectMapper s3StreamObjectMapper = session.getMapper(S3StreamObjectMapper.class);
                S3Object s3Object = s3ObjectMapper.getById(objectId + 2);
                Assertions.assertEquals(S3ObjectState.BOS_COMMITTED, s3Object.getState());
                Assertions.assertEquals(111L, s3Object.getObjectSize());
                Assertions.assertEquals(streamId, s3Object.getStreamId());

                com.automq.rocketmq.controller.metadata.database.dao.S3StreamObject object = s3StreamObjectMapper.getByObjectId(objectId + 2);
                Assertions.assertTrue(object.getBaseDataTimestamp() > 1);
                Assertions.assertTrue(object.getBaseDataTimestamp() > 0);
                Assertions.assertTrue(object.getCommittedTimestamp() > 0);
                Assertions.assertTrue(object.getCommittedTimestamp() > 0);
            }
        }

    }

    @Test
    public void test3StreamObjects_2PC_duplicate() throws IOException, ExecutionException, InterruptedException {
        ControllerClient controllerClient = Mockito.mock(ControllerClient.class);
        long objectId, streamId = 1;

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

            try (ControllerTestServer testServer = new ControllerTestServer(0, new ControllerServiceImpl(metadataStore));
                 ControllerClient client = new GrpcControllerClient(config)
            ) {
                testServer.start();
                int port = testServer.getPort();

                CommitStreamObjectRequest request1 = CommitStreamObjectRequest.newBuilder()
                    .setS3StreamObject(s3StreamObject)
                    .addAllCompactedObjectIds(Collections.emptyList())
                    .build();

                client.commitStreamObject(String.format("localhost:%d", port), request1).get();

                client.commitStreamObject(String.format("localhost:%d", port), request1).get();
            }

            long time = System.currentTimeMillis();
            try (SqlSession session = getSessionFactory().openSession()) {
                S3ObjectMapper s3ObjectMapper = session.getMapper(S3ObjectMapper.class);
                S3StreamObjectMapper s3StreamObjectMapper = session.getMapper(S3StreamObjectMapper.class);
                S3Object s3Object = s3ObjectMapper.getById(objectId + 2);
                Assertions.assertEquals(S3ObjectState.BOS_COMMITTED, s3Object.getState());

                com.automq.rocketmq.controller.metadata.database.dao.S3StreamObject object = s3StreamObjectMapper.getByObjectId(objectId + 2);
                Assertions.assertTrue(object.getBaseDataTimestamp() > 0);
                Assertions.assertTrue(object.getCommittedTimestamp() > 0);
            }
        }

    }

    @Test
    public void test3WALObjects_2PC_NoS3Stream() throws IOException, ExecutionException, InterruptedException {
        ControllerClient controllerClient = Mockito.mock(ControllerClient.class);
        long objectId;
        int nodeId = 2;

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
                PrepareS3ObjectsRequest request = PrepareS3ObjectsRequest.newBuilder()
                    .setPreparedCount(5)
                    .setTimeToLiveMinutes(5)
                    .build();

                PrepareS3ObjectsReply reply = client.prepareS3Objects(String.format("localhost:%d", port), request).get();
                objectId = reply.getFirstObjectId();
            }

            try (SqlSession session = this.getSessionFactory().openSession()) {
                S3WalObjectMapper s3WALObjectMapper = session.getMapper(S3WalObjectMapper.class);
                buildS3WalObjs(objectId + 2, 1).stream()
                    .map(s3WalObject -> {
                        Map<Long, SubStream> subStreams = buildWalSubStreams(1, 0, 10);
                        s3WalObject.setSubStreams(gson.toJson(subStreams));
                        return s3WalObject;
                    }).forEach(s3WALObjectMapper::create);

                buildS3WalObjs(objectId + 3, 1).stream()
                    .map(s3WalObject -> {
                        Map<Long, SubStream> subStreams = buildWalSubStreams(1, 10, 10);
                        s3WalObject.setSubStreams(gson.toJson(subStreams));
                        return s3WalObject;
                    }).forEach(s3WALObjectMapper::create);
                session.commit();
            }

            apache.rocketmq.controller.v1.S3WALObject walObject = apache.rocketmq.controller.v1.S3WALObject.newBuilder()
                .setObjectId(objectId + 4)
                .setObjectSize(222L)
                .setBrokerId(nodeId)
                .build();

            List<Long> compactedObjects = new ArrayList<>();
            compactedObjects.add(objectId + 2);
            compactedObjects.add(objectId + 3);

            try (ControllerTestServer testServer = new ControllerTestServer(0, new ControllerServiceImpl(metadataStore));
                 ControllerClient client = new GrpcControllerClient(config)
            ) {
                testServer.start();
                int port = testServer.getPort();

                CommitWALObjectRequest request = CommitWALObjectRequest.newBuilder()
                    .setS3WalObject(walObject)
                    .addAllS3StreamObjects(Collections.emptyList())
                    .addAllCompactedObjectIds(compactedObjects)
                    .build();

                client.commitWALObject(String.format("localhost:%d", port), request).get();
            }

            try (SqlSession session = getSessionFactory().openSession()) {
                S3ObjectMapper s3ObjectMapper = session.getMapper(S3ObjectMapper.class);

                for (long index = objectId + 2; index < objectId + 4; index++) {
                    S3Object object = s3ObjectMapper.getById(index);
                    Assertions.assertEquals(S3ObjectState.BOS_WILL_DELETE, object.getState());
                }

                S3WalObjectMapper s3WALObjectMapper = session.getMapper(S3WalObjectMapper.class);
                S3WalObject object = s3WALObjectMapper.getByObjectId(objectId + 4);
                Assertions.assertTrue(object.getBaseDataTimestamp() > 0);
                Assertions.assertEquals(objectId + 2, object.getSequenceId());
                Assertions.assertTrue(object.getCommittedTimestamp() > 0);
            }
        }

    }

    @Test
    public void test3WALObjects_2PC_NoCompacted() throws IOException, ExecutionException, InterruptedException {
        ControllerClient controllerClient = Mockito.mock(ControllerClient.class);
        long objectId;
        int nodeId = 2;

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
                PrepareS3ObjectsRequest request = PrepareS3ObjectsRequest.newBuilder()
                    .setPreparedCount(5)
                    .setTimeToLiveMinutes(5)
                    .build();

                PrepareS3ObjectsReply reply = client.prepareS3Objects(String.format("localhost:%d", port), request).get();
                objectId = reply.getFirstObjectId();
            }

            long time = System.currentTimeMillis();
            apache.rocketmq.controller.v1.S3WALObject walObject = apache.rocketmq.controller.v1.S3WALObject.newBuilder()
                .setObjectId(objectId + 4)
                .setObjectSize(222L)
                .setBrokerId(nodeId)
                .build();

            try (ControllerTestServer testServer = new ControllerTestServer(0, new ControllerServiceImpl(metadataStore));
                 ControllerClient client = new GrpcControllerClient(config)
            ) {
                testServer.start();
                int port = testServer.getPort();

                CommitWALObjectRequest request = CommitWALObjectRequest.newBuilder()
                    .setS3WalObject(walObject)
                    .addAllS3StreamObjects(Collections.emptyList())
                    .addAllCompactedObjectIds(Collections.emptyList())
                    .build();

                client.commitWALObject(String.format("localhost:%d", port), request).get();
            }

            try (SqlSession session = getSessionFactory().openSession()) {
                S3ObjectMapper s3ObjectMapper = session.getMapper(S3ObjectMapper.class);
                S3Object s3Object = s3ObjectMapper.getById(objectId + 4);
                Assertions.assertEquals(S3ObjectState.BOS_COMMITTED, s3Object.getState());
                Assertions.assertEquals(222L, s3Object.getObjectSize());
                Assertions.assertEquals(StreamConstants.NOOP_STREAM_ID, s3Object.getStreamId());

                S3WalObjectMapper s3WALObjectMapper = session.getMapper(S3WalObjectMapper.class);
                S3WalObject object = s3WALObjectMapper.getByObjectId(objectId + 4);
                Assertions.assertEquals(objectId + 4, object.getSequenceId());
                Assertions.assertTrue(object.getBaseDataTimestamp() > 1);
                Assertions.assertTrue(object.getCommittedTimestamp() > 0);
            }
        }

    }

    @Test
    public void test3WALObjects_2PC_Expired() throws IOException, ExecutionException, InterruptedException {
        ControllerClient controllerClient = Mockito.mock(ControllerClient.class);
        long objectId, streamId = 1;
        int nodeId = 2;

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
                PrepareS3ObjectsRequest request = PrepareS3ObjectsRequest.newBuilder()
                    .setPreparedCount(5)
                    .setTimeToLiveMinutes(0)
                    .build();

                PrepareS3ObjectsReply reply = client.prepareS3Objects(String.format("localhost:%d", port), request).get();
                objectId = reply.getFirstObjectId();
            }

            try (SqlSession session = this.getSessionFactory().openSession()) {
                S3WalObjectMapper s3WALObjectMapper = session.getMapper(S3WalObjectMapper.class);
                buildS3WalObjs(objectId + 2, 1).stream()
                    .map(s3WalObject -> {
                        Map<Long, SubStream> subStreams = buildWalSubStreams(1, 0, 10);
                        s3WalObject.setSubStreams(gson.toJson(subStreams));
                        return s3WalObject;
                    }).forEach(s3WALObjectMapper::create);

                buildS3WalObjs(objectId + 3, 1).stream()
                    .map(s3WalObject -> {
                        Map<Long, SubStream> subStreams = buildWalSubStreams(1, 10, 10);
                        s3WalObject.setSubStreams(gson.toJson(subStreams));
                        return s3WalObject;
                    }).forEach(s3WALObjectMapper::create);

                session.commit();
            }

            List<apache.rocketmq.controller.v1.S3StreamObject> s3StreamObjects = buildS3StreamObjs(objectId, 2, 20, 10)
                .stream().map(s3StreamObject -> apache.rocketmq.controller.v1.S3StreamObject.newBuilder()
                    .setObjectId(s3StreamObject.getObjectId())
                    .setStreamId(s3StreamObject.getStreamId())
                    .setObjectSize(s3StreamObject.getObjectSize())
                    .setBaseDataTimestamp(s3StreamObject.getBaseDataTimestamp())
                    .setStartOffset(s3StreamObject.getStartOffset())
                    .setEndOffset(s3StreamObject.getEndOffset())
                    .build())
                .toList();


            apache.rocketmq.controller.v1.S3WALObject walObject = apache.rocketmq.controller.v1.S3WALObject.newBuilder()
                .setObjectId(objectId + 4)
                .setSequenceId(11)
                .setObjectSize(222L)
                .setBrokerId(nodeId)
                .build();

            List<Long> compactedObjects = new ArrayList<>();
            compactedObjects.add(objectId + 2);
            compactedObjects.add(objectId + 3);

            try (ControllerTestServer testServer = new ControllerTestServer(0, new ControllerServiceImpl(metadataStore));
                 ControllerClient client = new GrpcControllerClient(config)
            ) {
                testServer.start();
                int port = testServer.getPort();

                CommitWALObjectRequest request = CommitWALObjectRequest.newBuilder()
                    .setS3WalObject(walObject)
                    .addAllS3StreamObjects(s3StreamObjects)
                    .addAllCompactedObjectIds(compactedObjects)
                    .build();

                CommitWALObjectReply commitWALObjectReply = client.commitWALObject(String.format("localhost:%d", port), request).get();
                Assertions.assertEquals(Code.ILLEGAL_STATE, commitWALObjectReply.getStatus().getCode());
            }
        }

    }

    @Test
    public void test3WALObjects_2PC() throws IOException, ExecutionException, InterruptedException {
        ControllerClient controllerClient = Mockito.mock(ControllerClient.class);
        long objectId, streamId = 1;
        int nodeId = 2;

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
                PrepareS3ObjectsRequest request = PrepareS3ObjectsRequest.newBuilder()
                    .setPreparedCount(5)
                    .setTimeToLiveMinutes(5)
                    .build();

                PrepareS3ObjectsReply reply = client.prepareS3Objects(String.format("localhost:%d", port), request).get();
                objectId = reply.getFirstObjectId();
            }


            try (SqlSession session = this.getSessionFactory().openSession()) {
                S3WalObjectMapper s3WALObjectMapper = session.getMapper(S3WalObjectMapper.class);
                buildS3WalObjs(objectId + 2, 1).stream()
                    .map(s3WalObject -> {
                        Map<Long, SubStream> subStreams = buildWalSubStreams(1, 0, 10);
                        s3WalObject.setSubStreams(gson.toJson(subStreams));
                        return s3WalObject;
                    }).forEach(s3WALObjectMapper::create);

                buildS3WalObjs(objectId + 3, 1).stream()
                    .map(s3WalObject -> {
                        Map<Long, SubStream> subStreams = buildWalSubStreams(1, 10, 10);
                        s3WalObject.setSubStreams(gson.toJson(subStreams));
                        return s3WalObject;
                    }).forEach(s3WALObjectMapper::create);

                session.commit();
            }

            List<apache.rocketmq.controller.v1.S3StreamObject> s3StreamObjects = buildS3StreamObjs(objectId, 2, 20, 10)
                .stream().map(s3StreamObject -> apache.rocketmq.controller.v1.S3StreamObject.newBuilder()
                    .setObjectId(s3StreamObject.getObjectId())
                    .setStreamId(s3StreamObject.getStreamId())
                    .setObjectSize(s3StreamObject.getObjectSize())
                    .setBaseDataTimestamp(s3StreamObject.getBaseDataTimestamp())
                    .setStartOffset(s3StreamObject.getStartOffset())
                    .setEndOffset(s3StreamObject.getEndOffset())
                    .build())
                .toList();

            apache.rocketmq.controller.v1.S3WALObject walObject = apache.rocketmq.controller.v1.S3WALObject.newBuilder()
                .setObjectId(objectId + 4)
                .setObjectSize(222L)
                .setBrokerId(nodeId)
                .build();

            long time = System.currentTimeMillis();
            List<Long> compactedObjects = new ArrayList<>();
            compactedObjects.add(objectId + 2);
            compactedObjects.add(objectId + 3);

            try (ControllerTestServer testServer = new ControllerTestServer(0, new ControllerServiceImpl(metadataStore));
                 ControllerClient client = new GrpcControllerClient(config)
            ) {
                testServer.start();
                int port = testServer.getPort();

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

                S3WalObjectMapper s3WALObjectMapper = session.getMapper(S3WalObjectMapper.class);
                S3WalObject object = s3WALObjectMapper.getByObjectId(objectId + 4);
                Assertions.assertTrue(object.getBaseDataTimestamp() > 0);
                Assertions.assertEquals(objectId + 2, object.getSequenceId());
                if (object.getCommittedTimestamp() - time > 5 * 60) {
                    Assertions.fail();
                }
            }
        }

    }

    @Test
    public void test3WALObjects_2PC_duplicate() throws IOException, ExecutionException, InterruptedException {
        ControllerClient controllerClient = Mockito.mock(ControllerClient.class);
        long objectId, streamId = 1;
        int nodeId = 2;

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
                PrepareS3ObjectsRequest request = PrepareS3ObjectsRequest.newBuilder()
                    .setPreparedCount(5)
                    .setTimeToLiveMinutes(5)
                    .build();

                PrepareS3ObjectsReply reply = client.prepareS3Objects(String.format("localhost:%d", port), request).get();
                objectId = reply.getFirstObjectId();
            }


            try (SqlSession session = this.getSessionFactory().openSession()) {
                S3WalObjectMapper s3WALObjectMapper = session.getMapper(S3WalObjectMapper.class);
                buildS3WalObjs(objectId + 2, 1).stream()
                    .map(s3WalObject -> {
                        Map<Long, SubStream> subStreams = buildWalSubStreams(1, 0, 10);
                        s3WalObject.setSubStreams(gson.toJson(subStreams));
                        return s3WalObject;
                    }).forEach(s3WALObjectMapper::create);

                buildS3WalObjs(objectId + 3, 1).stream()
                    .map(s3WalObject -> {
                        Map<Long, SubStream> subStreams = buildWalSubStreams(1, 10, 10);
                        s3WalObject.setSubStreams(gson.toJson(subStreams));
                        return s3WalObject;
                    }).forEach(s3WALObjectMapper::create);

                session.commit();
            }

            List<apache.rocketmq.controller.v1.S3StreamObject> s3StreamObjects = buildS3StreamObjs(objectId, 2, 20, 10)
                .stream().map(s3StreamObject -> apache.rocketmq.controller.v1.S3StreamObject.newBuilder()
                    .setObjectId(s3StreamObject.getObjectId())
                    .setStreamId(s3StreamObject.getStreamId())
                    .setObjectSize(s3StreamObject.getObjectSize())
                    .setBaseDataTimestamp(s3StreamObject.getBaseDataTimestamp())
                    .setStartOffset(s3StreamObject.getStartOffset())
                    .setEndOffset(s3StreamObject.getEndOffset())
                    .build())
                .toList();

            apache.rocketmq.controller.v1.S3WALObject walObject = apache.rocketmq.controller.v1.S3WALObject.newBuilder()
                .setObjectId(objectId + 4)
                .setObjectSize(222L)
                .setBrokerId(nodeId)
                .build();

            long time = System.currentTimeMillis();
            List<Long> compactedObjects = new ArrayList<>();
            compactedObjects.add(objectId + 2);
            compactedObjects.add(objectId + 3);

            try (ControllerTestServer testServer = new ControllerTestServer(0, new ControllerServiceImpl(metadataStore));
                 ControllerClient client = new GrpcControllerClient(config)
            ) {
                testServer.start();
                int port = testServer.getPort();

                CommitWALObjectRequest request = CommitWALObjectRequest.newBuilder()
                    .setS3WalObject(walObject)
                    .addAllS3StreamObjects(s3StreamObjects)
                    .addAllCompactedObjectIds(compactedObjects)
                    .build();

                client.commitWALObject(String.format("localhost:%d", port), request).get();
                client.commitWALObject(String.format("localhost:%d", port), request).get();
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

                S3WalObjectMapper s3WALObjectMapper = session.getMapper(S3WalObjectMapper.class);
                S3WalObject object = s3WALObjectMapper.getByObjectId(objectId + 4);
                Assertions.assertEquals(objectId + 2, object.getSequenceId());
                Assertions.assertTrue(object.getBaseDataTimestamp() > 0);
                if (object.getCommittedTimestamp() - time > 5 * 60) {
                    Assertions.fail();
                }
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