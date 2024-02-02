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

package com.automq.rocketmq.controller;

import apache.rocketmq.controller.v1.AcceptTypes;
import apache.rocketmq.common.v1.Code;
import apache.rocketmq.controller.v1.CreateTopicRequest;
import apache.rocketmq.controller.v1.MessageType;
import apache.rocketmq.controller.v1.Topic;
import com.automq.rocketmq.common.config.GrpcClientConfig;
import com.automq.rocketmq.common.exception.ControllerException;
import com.automq.rocketmq.controller.client.GrpcControllerClient;
import com.automq.rocketmq.controller.server.ControllerServiceImpl;
import com.automq.rocketmq.metadata.dao.Node;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

class GrpcControllerClientTest {

    private final GrpcClientConfig config;

    public GrpcControllerClientTest() {
        config = Mockito.mock(GrpcClientConfig.class);
        Mockito.when(config.rpcTimeout()).thenCallRealMethod();
    }

    @Test
    public void testRegisterBroker() throws IOException, ExecutionException, InterruptedException {
        String name = "broker-name";
        String address = "localhost:1234";
        String instanceId = "i-ctrl";
        MetadataStore metadataStore = Mockito.mock(MetadataStore.class);
        Node node = new Node();
        node.setId(1);
        node.setEpoch(1);
        Mockito.when(metadataStore.registerBrokerNode(ArgumentMatchers.anyString(), ArgumentMatchers.anyString(),
            ArgumentMatchers.anyString())).thenReturn(CompletableFuture.completedFuture(node));
        ControllerServiceImpl svc = new ControllerServiceImpl(metadataStore);
        try (ControllerTestServer testServer = new ControllerTestServer(0, svc);
             ControllerClient client = new GrpcControllerClient(config)
        ) {
            testServer.start();
            int port = testServer.getPort();
            Node result = client.registerBroker(String.format("localhost:%d", port), name, address, instanceId).get();
            Assertions.assertEquals(1, result.getId());
            Assertions.assertEquals(1, result.getEpoch());
            Assertions.assertEquals(name, result.getName());
            Assertions.assertEquals(address, result.getAddress());
            Assertions.assertEquals(instanceId, result.getInstanceId());
        }
    }

    @Test
    public void testRegisterBroker_badTarget() throws IOException {
        String name = "broker-name";
        String address = "localhost:1234";
        String instanceId = "i-ctrl";
        Node node = new Node();
        node.setId(1);
        node.setEpoch(1);
        try (ControllerClient client = new GrpcControllerClient(config);
             MetadataStore metadataStore = Mockito.mock(MetadataStore.class)) {
            Mockito.when(metadataStore.registerBrokerNode(ArgumentMatchers.anyString(), ArgumentMatchers.anyString(),
                ArgumentMatchers.anyString())).thenReturn(CompletableFuture.completedFuture(node));
            Assertions.assertThrows(ExecutionException.class,
                () -> client.registerBroker(null, name, address, instanceId).get());

        }
    }

    @Test
    public void testRegisterBroker_leaderFailure() throws IOException {
        String name = "broker-name";
        String address = "localhost:1234";
        String instanceId = "i-ctrl";
        MetadataStore metadataStore = Mockito.mock(MetadataStore.class);
        Mockito.when(metadataStore.registerBrokerNode(ArgumentMatchers.anyString(), ArgumentMatchers.anyString(),
                ArgumentMatchers.anyString()))
            .thenReturn(CompletableFuture.failedFuture(new ControllerException(Code.MOCK_FAILURE_VALUE, "Mock error message")));
        ControllerServiceImpl svc = new ControllerServiceImpl(metadataStore);
        try (ControllerTestServer testServer = new ControllerTestServer(0, svc);
             ControllerClient client = new GrpcControllerClient(config)
        ) {
            testServer.start();
            int port = testServer.getPort();
            Assertions.assertThrows(ExecutionException.class,
                () -> client.registerBroker(String.format("localhost:%d", port), name, address, instanceId).get());

        }
    }

    @Test
    public void testHeartbeat() throws IOException {
        MetadataStore metadataStore = Mockito.mock(MetadataStore.class);
        ControllerServiceImpl svc = new ControllerServiceImpl(metadataStore);
        try (ControllerTestServer testServer = new ControllerTestServer(0, svc);
             ControllerClient client = new GrpcControllerClient(config)
        ) {
            testServer.start();
            int port = testServer.getPort();
            Assertions.assertDoesNotThrow(() -> client.heartbeat(String.format("localhost:%d", port), 1, 1, false).get());
        }
    }

    @Test
    public void testCreateTopic() throws ControllerException, IOException, ExecutionException, InterruptedException {
        String topicName = "t1";
        int queueNum = 4;
        MetadataStore metadataStore = Mockito.mock(MetadataStore.class);
        Mockito.when(metadataStore.createTopic(ArgumentMatchers.any())).thenReturn(CompletableFuture.completedFuture(1L));
        ControllerServiceImpl svc = new ControllerServiceImpl(metadataStore);
        try (ControllerTestServer testServer = new ControllerTestServer(0, svc);
             ControllerClient client = new GrpcControllerClient(config)
        ) {
            testServer.start();
            int port = testServer.getPort();
            CreateTopicRequest request = CreateTopicRequest.newBuilder()
                .setTopic(topicName)
                .setCount(queueNum)
                .setRetentionHours(1)
                .setAcceptTypes(AcceptTypes.newBuilder()
                    .addTypes(MessageType.NORMAL)
                    .addTypes(MessageType.FIFO)
                    .addTypes(MessageType.DELAY)
                    .build())
                .build();
            long topicId = client.createTopic(String.format("localhost:%d", port), request).get();
            Assertions.assertEquals(1, topicId);
        }
    }

    @Test
    public void testCreateTopic_duplicate() throws IOException {
        String topicName = "t1";
        int queueNum = 4;
        MetadataStore metadataStore = Mockito.mock(MetadataStore.class);
        ControllerServiceImpl svc = new ControllerServiceImpl(metadataStore);
        ControllerException e = new ControllerException(Code.DUPLICATED_VALUE, "Topic is not available");
        Mockito.when(metadataStore.createTopic(ArgumentMatchers.any()))
            .thenReturn(CompletableFuture.failedFuture(e));
        try (ControllerTestServer testServer = new ControllerTestServer(0, svc);
             ControllerClient client = new GrpcControllerClient(config)
        ) {
            testServer.start();
            int port = testServer.getPort();
            CreateTopicRequest request = CreateTopicRequest.newBuilder()
                .setTopic(topicName)
                .setCount(queueNum)
                .setAcceptTypes(AcceptTypes.newBuilder()
                    .addTypes(MessageType.NORMAL)
                    .addTypes(MessageType.FIFO)
                    .addTypes(MessageType.DELAY)
                    .build())
                .build();
            Assertions.assertThrows(ExecutionException.class,
                () -> client.createTopic(String.format("localhost:%d", port), request).get());
        }
    }

    @Test
    public void testDeleteTopic() throws ControllerException, IOException, ExecutionException, InterruptedException {
        long topicId = 1;
        MetadataStore metadataStore = Mockito.mock(MetadataStore.class);
        Mockito.doAnswer(invocation -> {
            CompletableFuture<Void> future = new CompletableFuture<>();
            future.complete(null);
            return future;
        }).when(metadataStore).deleteTopic(ArgumentMatchers.anyLong());
        ControllerServiceImpl svc = new ControllerServiceImpl(metadataStore);
        try (ControllerTestServer testServer = new ControllerTestServer(0, svc);
             ControllerClient client = new GrpcControllerClient(config)
        ) {
            testServer.start();
            int port = testServer.getPort();
            client.deleteTopic(String.format("localhost:%d", port), topicId).get();
        }
    }

    @Test
    public void testDeleteTopic_NotFound() throws IOException {
        long topicId = 1;
        MetadataStore metadataStore = Mockito.mock(MetadataStore.class);
        Mockito.doAnswer(invocation -> {
            CompletableFuture<Void> future = new CompletableFuture<>();
            ControllerException e = new ControllerException(Code.NOT_FOUND_VALUE, "Not found");
            future.completeExceptionally(e);
            return future;
        }).when(metadataStore).deleteTopic(ArgumentMatchers.anyLong());
        ControllerServiceImpl svc = new ControllerServiceImpl(metadataStore);
        try (ControllerTestServer testServer = new ControllerTestServer(0, svc);
             ControllerClient client = new GrpcControllerClient(config)
        ) {
            testServer.start();
            int port = testServer.getPort();
            Assertions.assertThrows(ExecutionException.class, () -> client.deleteTopic(String.format("localhost:%d", port), topicId).get());
        }
    }

    @Test
    public void testDescribeTopic() throws IOException, ExecutionException, InterruptedException {
        long topicId = 1L;
        String topicName = "T-abc";
        MetadataStore metadataStore = Mockito.mock(MetadataStore.class);
        Topic topic = Topic.newBuilder()
            .setName(topicName)
            .setTopicId(topicId)
            .setCount(1)
            .build();
        CompletableFuture<Topic> future = new CompletableFuture<>();
        future.complete(topic);
        Mockito.when(metadataStore.describeTopic(ArgumentMatchers.anyLong(), ArgumentMatchers.anyString()))
            .thenReturn(future);
        ControllerServiceImpl svc = new ControllerServiceImpl(metadataStore);
        try (ControllerTestServer testServer = new ControllerTestServer(0, svc);
             ControllerClient client = new GrpcControllerClient(config)
        ) {
            testServer.start();
            int port = testServer.getPort();
            Topic got = client.describeTopic(String.format("localhost:%d", port), topicId, topicName).get();
            Assertions.assertEquals(topicId, got.getTopicId());
            Assertions.assertEquals(topicName, got.getName());
        }
    }

    @Test
    public void testDescribeTopic_NotFound() throws IOException {
        long topicId = 1L;
        String topicName = "T-abc";
        MetadataStore metadataStore = Mockito.mock(MetadataStore.class);
        CompletableFuture<Topic> future = new CompletableFuture<>();
        future.completeExceptionally(new ControllerException(Code.NOT_FOUND_VALUE, "Not found"));
        Mockito.when(metadataStore.describeTopic(ArgumentMatchers.anyLong(), ArgumentMatchers.anyString()))
            .thenReturn(future);
        ControllerServiceImpl svc = new ControllerServiceImpl(metadataStore);
        try (ControllerTestServer testServer = new ControllerTestServer(0, svc);
             ControllerClient client = new GrpcControllerClient(config)
        ) {
            testServer.start();
            int port = testServer.getPort();
            Assertions.assertThrows(ExecutionException.class,
                () -> client.describeTopic(String.format("localhost:%d", port), topicId, topicName).get());
        }
    }

    @Test
    public void testNotifyMessageQueueAssignable() throws IOException {
        long topicId = 1L;
        int queueId = 2;
        MetadataStore metadataStore = Mockito.mock(MetadataStore.class);
        Mockito.doAnswer(invocation -> {
            CompletableFuture<Void> future = new CompletableFuture<>();
            future.complete(null);
            return future;
        }).when(metadataStore).markMessageQueueAssignable(ArgumentMatchers.anyLong(), ArgumentMatchers.anyInt());
        ControllerServiceImpl svc = new ControllerServiceImpl(metadataStore);
        try (ControllerTestServer testServer = new ControllerTestServer(0, svc);
             ControllerClient client = new GrpcControllerClient(config)
        ) {
            testServer.start();
            int port = testServer.getPort();
            Assertions.assertDoesNotThrow(() -> {
                client.notifyQueueClose(String.format("localhost:%d", port), topicId, queueId).get();
            });
        }
    }
}