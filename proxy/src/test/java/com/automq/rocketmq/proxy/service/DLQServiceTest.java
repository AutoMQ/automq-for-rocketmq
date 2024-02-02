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

package com.automq.rocketmq.proxy.service;

import apache.rocketmq.controller.v1.AcceptTypes;
import apache.rocketmq.controller.v1.ConsumerGroup;
import apache.rocketmq.controller.v1.GroupType;
import apache.rocketmq.controller.v1.MessageQueue;
import apache.rocketmq.controller.v1.MessageQueueAssignment;
import apache.rocketmq.controller.v1.MessageType;
import apache.rocketmq.controller.v1.Topic;
import com.automq.rocketmq.common.config.BrokerConfig;
import com.automq.rocketmq.common.model.FlatMessageExt;
import com.automq.rocketmq.common.model.generated.FlatMessage;
import com.automq.rocketmq.metadata.DefaultProxyMetadataService;
import com.automq.rocketmq.metadata.api.ProxyMetadataService;
import com.automq.rocketmq.proxy.grpc.client.GrpcProxyClient;
import com.automq.rocketmq.proxy.mock.MockMessageUtil;
import com.automq.rocketmq.store.api.MessageStore;
import com.automq.rocketmq.store.model.StoreContext;
import com.automq.rocketmq.store.model.message.PutResult;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

public class DLQServiceTest {

    private BrokerConfig config;
    private ProxyMetadataService metadataService;
    private MessageStore messageStore;
    private DeadLetterService dlqService;

    private static final long TOPIC_ID = 6;
    private static final int QUEUE_ID = 0;
    private static final long DLQ_TOPIC_ID = 13;
    private static final String DLQ_TOPIC_NAME = "DLQ_13";
    private static final long CONSUMER_GROUP_ID = 1313;
    private static final String CONSUMER_GROUP_NAME = "CONSUMER_GROUP_1313";

    @BeforeEach
    public void setUp() {
        config = new BrokerConfig();
        config.setInnerAccessKey("accessKey");
        config.setInnerSecretKey("secretKey");
        config.setAdvertiseAddress("localhost:8081");
        metadataService = Mockito.mock(DefaultProxyMetadataService.class);
        messageStore = Mockito.mock(MessageStore.class);
        dlqService = Mockito.spy(new DeadLetterService(config, metadataService, new GrpcProxyClient(config)));
        dlqService.init(messageStore);
    }

    @Test
    public void send_normal() {
        MessageQueue messageQueue = MessageQueue.newBuilder().setQueueId(QUEUE_ID).setTopicId(TOPIC_ID).build();
        MessageQueueAssignment assignment = MessageQueueAssignment.newBuilder().setQueue(messageQueue).setNodeId(config.nodeId()).build();
        Topic dlqTopic = Topic.newBuilder()
            .setTopicId(DLQ_TOPIC_ID)
            .setName(DLQ_TOPIC_NAME)
            .setAcceptTypes(AcceptTypes.newBuilder().addTypes(MessageType.NORMAL).build())
            .addAssignments(assignment)
            .build();
        ConsumerGroup consumerGroup = ConsumerGroup.newBuilder()
            .setGroupId(CONSUMER_GROUP_ID)
            .setName(CONSUMER_GROUP_NAME)
            .setDeadLetterTopicId(DLQ_TOPIC_ID)
            .setGroupType(GroupType.GROUP_TYPE_STANDARD)
            .build();

        Mockito.doReturn(CompletableFuture.completedFuture(dlqTopic))
            .when(metadataService).topicOf(DLQ_TOPIC_ID);
        Mockito.doReturn(CompletableFuture.completedFuture(consumerGroup))
            .when(metadataService).consumerGroupOf(CONSUMER_GROUP_ID);
        FlatMessageExt msg = MockMessageUtil.buildMessage(TOPIC_ID, QUEUE_ID, "TAG_DLQ");
        ByteBuffer payload = msg.message().payloadAsByteBuffer();
        Mockito.doAnswer(ink -> {
            FlatMessage message = ink.getArgument(1);

            assertNotEquals(TOPIC_ID, message.topicId());
            assertEquals(DLQ_TOPIC_ID, message.topicId());

            assertEquals(QUEUE_ID, message.queueId());

            assertEquals("TAG_DLQ", message.tag());
            assertEquals(MockMessageUtil.DEFAULT_MESSAGE_ID, message.systemProperties().messageId());
            assertEquals(MockMessageUtil.DEFAULT_KEYS, message.keys());
            assertEquals(MockMessageUtil.DEFAULT_MESSAGE_GROUP, message.messageGroup());
            assertEquals(payload, message.payloadAsByteBuffer());
            return CompletableFuture.completedFuture(new PutResult(PutResult.Status.PUT_OK, 0));
        }).when(messageStore).put(Mockito.any(), Mockito.any(FlatMessage.class));

        dlqService.send(StoreContext.EMPTY, CONSUMER_GROUP_ID, msg.message()).join();
        Mockito.verify(messageStore, Mockito.times(1)).put(Mockito.any(), Mockito.any(FlatMessage.class));
    }

    @Test
    public void send_group_not_allowed_dlq() {
        // 1. DLQ topic isn't configured
        Topic dlqTopic = Topic.newBuilder()
            .setTopicId(DLQ_TOPIC_ID)
            .setName(DLQ_TOPIC_NAME)
            .setAcceptTypes(AcceptTypes.newBuilder().addTypes(MessageType.NORMAL).build())
            .build();
        ConsumerGroup consumerGroup = ConsumerGroup.newBuilder()
            .setGroupId(CONSUMER_GROUP_ID)
            .setName(CONSUMER_GROUP_NAME)
            .setGroupType(GroupType.GROUP_TYPE_STANDARD)
            .build();
        Mockito.doReturn(CompletableFuture.completedFuture(dlqTopic))
            .when(metadataService).topicOf(DLQ_TOPIC_ID);
        Mockito.doReturn(CompletableFuture.completedFuture(consumerGroup))
            .when(metadataService).consumerGroupOf(CONSUMER_GROUP_ID);

        FlatMessageExt msg = MockMessageUtil.buildMessage(TOPIC_ID, QUEUE_ID, "TAG_DLQ");

        dlqService.send(StoreContext.EMPTY, CONSUMER_GROUP_ID, msg.message()).join();
        Mockito.verify(messageStore, Mockito.times(0)).put(Mockito.any(), Mockito.any(FlatMessage.class));

        // 2. DLQ topic is configured but has no assignment
        consumerGroup = ConsumerGroup.newBuilder()
            .setGroupId(CONSUMER_GROUP_ID)
            .setName(CONSUMER_GROUP_NAME)
            .setDeadLetterTopicId(DLQ_TOPIC_ID)
            .setGroupType(GroupType.GROUP_TYPE_STANDARD)
            .build();
        Mockito.doReturn(CompletableFuture.completedFuture(dlqTopic))
            .when(metadataService).topicOf(DLQ_TOPIC_ID);
        Mockito.doReturn(CompletableFuture.completedFuture(consumerGroup))
            .when(metadataService).consumerGroupOf(CONSUMER_GROUP_ID);

        dlqService.send(StoreContext.EMPTY, CONSUMER_GROUP_ID, msg.message()).join();
        Mockito.verify(messageStore, Mockito.times(0)).put(Mockito.any(), Mockito.any(FlatMessage.class));

        // 3. DLQ topic is the same as original topic
        consumerGroup = ConsumerGroup.newBuilder()
            .setGroupId(CONSUMER_GROUP_ID)
            .setName(CONSUMER_GROUP_NAME)
            .setDeadLetterTopicId(TOPIC_ID)
            .setGroupType(GroupType.GROUP_TYPE_STANDARD)
            .build();
        msg = MockMessageUtil.buildMessage(TOPIC_ID, QUEUE_ID, "TAG_DLQ");
        Mockito.doReturn(CompletableFuture.completedFuture(dlqTopic))
            .when(metadataService).topicOf(DLQ_TOPIC_ID);
        Mockito.doReturn(CompletableFuture.completedFuture(consumerGroup))
            .when(metadataService).consumerGroupOf(CONSUMER_GROUP_ID);

        dlqService.send(StoreContext.EMPTY, CONSUMER_GROUP_ID, msg.message()).join();
        Mockito.verify(messageStore, Mockito.times(0)).put(Mockito.any(), Mockito.any(FlatMessage.class));

        // 4. DLQ topic doesn't accept DLQ message
        MessageQueue messageQueue = MessageQueue.newBuilder().setQueueId(QUEUE_ID).setTopicId(TOPIC_ID).build();
        MessageQueueAssignment assignment = MessageQueueAssignment.newBuilder().setQueue(messageQueue).setNodeId(config.nodeId()).build();
        dlqTopic = Topic.newBuilder()
            .setTopicId(DLQ_TOPIC_ID)
            .setName(DLQ_TOPIC_NAME)
            .setAcceptTypes(AcceptTypes.newBuilder().addTypes(MessageType.TRANSACTION).build())
            .addAssignments(assignment)
            .build();
        consumerGroup = ConsumerGroup.newBuilder()
            .setGroupId(CONSUMER_GROUP_ID)
            .setName(CONSUMER_GROUP_NAME)
            .setGroupType(GroupType.GROUP_TYPE_STANDARD)
            .setDeadLetterTopicId(DLQ_TOPIC_ID)
            .build();
        msg = MockMessageUtil.buildMessage(TOPIC_ID, QUEUE_ID, "TAG_DLQ");
        Mockito.doReturn(CompletableFuture.completedFuture(dlqTopic))
            .when(metadataService).topicOf(DLQ_TOPIC_ID);
        Mockito.doReturn(CompletableFuture.completedFuture(consumerGroup))
            .when(metadataService).consumerGroupOf(CONSUMER_GROUP_ID);

        dlqService.send(StoreContext.EMPTY, CONSUMER_GROUP_ID, msg.message()).join();
        Mockito.verify(messageStore, Mockito.times(0)).put(Mockito.any(), Mockito.any(FlatMessage.class));

        // 5. DLQ topic not exist
        Mockito.doReturn(CompletableFuture.completedFuture(null))
            .when(metadataService).topicOf(DLQ_TOPIC_ID);
        consumerGroup = ConsumerGroup.newBuilder()
            .setGroupId(CONSUMER_GROUP_ID)
            .setName(CONSUMER_GROUP_NAME)
            .setDeadLetterTopicId(DLQ_TOPIC_ID)
            .setGroupType(GroupType.GROUP_TYPE_STANDARD)
            .build();
        msg = MockMessageUtil.buildMessage(TOPIC_ID, QUEUE_ID, "TAG_DLQ");
        Mockito.doReturn(CompletableFuture.completedFuture(consumerGroup))
            .when(metadataService).consumerGroupOf(CONSUMER_GROUP_ID);

        dlqService.send(StoreContext.EMPTY, CONSUMER_GROUP_ID, msg.message()).join();
        Mockito.verify(messageStore, Mockito.times(0)).put(Mockito.any(), Mockito.any(FlatMessage.class));
    }
}
