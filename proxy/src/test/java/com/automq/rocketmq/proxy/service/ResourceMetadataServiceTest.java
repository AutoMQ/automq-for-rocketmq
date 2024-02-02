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
import apache.rocketmq.controller.v1.MessageType;
import apache.rocketmq.controller.v1.Topic;
import com.automq.rocketmq.metadata.api.ProxyMetadataService;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import org.apache.rocketmq.common.attribute.TopicMessageType;
import org.apache.rocketmq.remoting.protocol.subscription.SubscriptionGroupConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ResourceMetadataServiceTest {
    @Mock
    private ProxyMetadataService proxyMetadataService;
    private ResourceMetadataService resourceMetadataService;

    @BeforeEach
    void setUp() {
        resourceMetadataService = new ResourceMetadataService(proxyMetadataService);
    }

    @Test
    void getTopicMessageType() {
        String topicName = "TopicA";
        when(proxyMetadataService.topicOf(topicName)).thenReturn(CompletableFuture.completedFuture(createTopic(topicName, MessageType.NORMAL)));
        TopicMessageType messageType = resourceMetadataService.getTopicMessageType(null, topicName);
        assertEquals(TopicMessageType.NORMAL, messageType);

        when(proxyMetadataService.topicOf(topicName)).thenReturn(CompletableFuture.completedFuture(createTopic(topicName, MessageType.FIFO)));
        messageType = resourceMetadataService.getTopicMessageType(null, topicName);
        assertEquals(TopicMessageType.FIFO, messageType);

        when(proxyMetadataService.topicOf(topicName)).thenReturn(CompletableFuture.completedFuture(createTopic(topicName, MessageType.DELAY)));
        messageType = resourceMetadataService.getTopicMessageType(null, topicName);
        assertEquals(TopicMessageType.DELAY, messageType);

        when(proxyMetadataService.topicOf(topicName)).thenReturn(CompletableFuture.completedFuture(createTopic(topicName, MessageType.TRANSACTION)));
        messageType = resourceMetadataService.getTopicMessageType(null, topicName);
        assertEquals(TopicMessageType.TRANSACTION, messageType);

        when(proxyMetadataService.topicOf(topicName)).thenReturn(CompletableFuture.completedFuture(createTopic(topicName, MessageType.MESSAGE_TYPE_UNSPECIFIED)));
        messageType = resourceMetadataService.getTopicMessageType(null, topicName);
        assertEquals(TopicMessageType.UNSPECIFIED, messageType);

        when(proxyMetadataService.topicOf(topicName)).thenReturn(CompletableFuture.completedFuture(createTopic(topicName, MessageType.NORMAL, MessageType.FIFO)));
        messageType = resourceMetadataService.getTopicMessageType(null, topicName);
        assertEquals(TopicMessageType.UNSPECIFIED, messageType);

        when(proxyMetadataService.topicOf(topicName)).thenReturn(CompletableFuture.failedFuture(new RuntimeException("Topic not found")));
        messageType = resourceMetadataService.getTopicMessageType(null, topicName);
        assertEquals(TopicMessageType.UNSPECIFIED, messageType);
    }

    @Test
    void getSubscriptionGroupConfig() {
        String groupName = "GroupA";
        when(proxyMetadataService.consumerGroupOf(groupName)).thenReturn(CompletableFuture.completedFuture(createConsumerGroup(groupName, GroupType.GROUP_TYPE_FIFO, 1)));
        SubscriptionGroupConfig config = resourceMetadataService.getSubscriptionGroupConfig(null, groupName);
        assertTrue(config.isConsumeMessageOrderly());
        assertEquals(config.getRetryMaxTimes(), 1);

        when(proxyMetadataService.consumerGroupOf(groupName)).thenReturn(CompletableFuture.completedFuture(createConsumerGroup(groupName, GroupType.GROUP_TYPE_STANDARD, 2)));
        config = resourceMetadataService.getSubscriptionGroupConfig(null, groupName);
        assertFalse(config.isConsumeMessageOrderly());
        assertEquals(config.getRetryMaxTimes(), 2);

        when(proxyMetadataService.consumerGroupOf(groupName)).thenReturn(CompletableFuture.completedFuture(createConsumerGroup(groupName, GroupType.GROUP_TYPE_UNSPECIFIED, 2)));
        config = resourceMetadataService.getSubscriptionGroupConfig(null, groupName);
        assertFalse(config.isConsumeMessageOrderly());
        assertEquals(config.getRetryMaxTimes(), 2);

        when(proxyMetadataService.consumerGroupOf(groupName)).thenReturn(CompletableFuture.failedFuture(new RuntimeException("Group not found")));
        config = resourceMetadataService.getSubscriptionGroupConfig(null, groupName);
        assertNull(config);
    }

    private Topic createTopic(String topicName, MessageType... messageTypes) {
        Topic.Builder builder = Topic.newBuilder();
        builder.setName(topicName);
        AcceptTypes acceptTypes = AcceptTypes.newBuilder()
            .addAllTypes(Arrays.asList(messageTypes))
            .build();
        builder.setAcceptTypes(acceptTypes);
        return builder.build();
    }

    private ConsumerGroup createConsumerGroup(String groupName, GroupType groupType, int maxDeliveryAttempt) {
        ConsumerGroup.Builder builder = ConsumerGroup.newBuilder();
        builder.setName(groupName);
        builder.setGroupType(groupType);
        builder.setMaxDeliveryAttempt(maxDeliveryAttempt);
        return builder.build();
    }
}