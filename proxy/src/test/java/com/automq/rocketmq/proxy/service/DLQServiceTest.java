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

package com.automq.rocketmq.proxy.service;

import apache.rocketmq.controller.v1.AcceptTypes;
import apache.rocketmq.controller.v1.ConsumerGroup;
import apache.rocketmq.controller.v1.GroupType;
import apache.rocketmq.controller.v1.MessageType;
import apache.rocketmq.controller.v1.Topic;
import com.automq.rocketmq.common.config.BrokerConfig;
import com.automq.rocketmq.common.model.FlatMessageExt;
import com.automq.rocketmq.common.system.MessageConstants;
import com.automq.rocketmq.metadata.DefaultProxyMetadataService;
import com.automq.rocketmq.metadata.api.ProxyMetadataService;
import com.automq.rocketmq.proxy.mock.MockMessageUtil;
import com.automq.rocketmq.store.model.StoreContext;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import org.apache.rocketmq.client.apis.message.Message;
import org.apache.rocketmq.client.apis.producer.Producer;
import org.apache.rocketmq.common.message.MessageConst;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class DLQServiceTest {

    private BrokerConfig config;
    private ProxyMetadataService metadataService;
    private Producer producer;
    private DeadLetterService dlqService;


    private static final long TOPIC_ID = 6;
    private static final int QUEUE_ID = 0;
    private static final String TOPIC_NAME = "TOPIC_6";
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
        producer = Mockito.mock(Producer.class);
        dlqService = Mockito.spy(new DeadLetterService(config, metadataService));
        Mockito.doReturn(producer)
            .when(dlqService).getProducer();
    }

    @Test
    public void send_normal() {
        Topic dlqTopic = Topic.newBuilder()
            .setTopicId(DLQ_TOPIC_ID)
            .setName(DLQ_TOPIC_NAME)
            .setAcceptTypes(AcceptTypes.newBuilder().addTypes(MessageType.NORMAL).build())
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
        byte[] payload = msg.message().payloadAsByteBuffer().array();
        Mockito.doAnswer(ink -> {
            Message message = ink.getArgument(0);
            assertEquals(DLQ_TOPIC_NAME, message.getTopic());
            assertEquals("TAG_DLQ", message.getTag().get());
            assertEquals(TOPIC_ID, Long.parseLong(message.getProperties().get(MessageConstants.PROPERTY_DLQ_ORIGIN_TOPIC_ID)));
            assertEquals(MockMessageUtil.DEFAULT_MESSAGE_ID, message.getProperties().get(MessageConst.PROPERTY_DLQ_ORIGIN_MESSAGE_ID));
            assertEquals(MockMessageUtil.DEFAULT_KEYS, message.getKeys().stream().reduce((a, b) -> a + " " + b).get());
            assertEquals(MockMessageUtil.DEFAULT_MESSAGE_GROUP, message.getMessageGroup().get());
            assertEquals(ByteBuffer.wrap(payload), message.getBody());
            return CompletableFuture.completedFuture(null);
        }).when(producer).sendAsync(Mockito.any(Message.class));

        dlqService.send(StoreContext.EMPTY, CONSUMER_GROUP_ID, msg).join();
        Mockito.verify(producer, Mockito.times(1)).sendAsync(Mockito.any(Message.class));
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

        dlqService.send(StoreContext.EMPTY, CONSUMER_GROUP_ID, msg).join();
        Mockito.verify(producer, Mockito.times(0)).sendAsync(Mockito.any(Message.class));

        // 2. DLQ topic is the same as original topic
        consumerGroup = ConsumerGroup.newBuilder()
            .setGroupId(CONSUMER_GROUP_ID)
            .setName(CONSUMER_GROUP_NAME)
            .setDeadLetterTopicId(TOPIC_ID)
            .setGroupType(GroupType.GROUP_TYPE_STANDARD)
            .build();
        Mockito.doReturn(CompletableFuture.completedFuture(dlqTopic))
            .when(metadataService).topicOf(DLQ_TOPIC_ID);
        Mockito.doReturn(CompletableFuture.completedFuture(consumerGroup))
            .when(metadataService).consumerGroupOf(CONSUMER_GROUP_ID);

        dlqService.send(StoreContext.EMPTY, CONSUMER_GROUP_ID, msg).join();
        Mockito.verify(producer, Mockito.times(0)).sendAsync(Mockito.any(Message.class));

        // 3. DLQ topic doesn't accept DLQ message
        dlqTopic = Topic.newBuilder()
            .setTopicId(DLQ_TOPIC_ID)
            .setName(DLQ_TOPIC_NAME)
            .setAcceptTypes(AcceptTypes.newBuilder().addTypes(MessageType.TRANSACTION).build())
            .build();
        consumerGroup = ConsumerGroup.newBuilder()
            .setGroupId(CONSUMER_GROUP_ID)
            .setName(CONSUMER_GROUP_NAME)
            .setGroupType(GroupType.GROUP_TYPE_STANDARD)
            .setDeadLetterTopicId(DLQ_TOPIC_ID)
            .build();
        Mockito.doReturn(CompletableFuture.completedFuture(dlqTopic))
            .when(metadataService).topicOf(DLQ_TOPIC_ID);
        Mockito.doReturn(CompletableFuture.completedFuture(consumerGroup))
            .when(metadataService).consumerGroupOf(CONSUMER_GROUP_ID);

        dlqService.send(StoreContext.EMPTY, CONSUMER_GROUP_ID, msg).join();
        Mockito.verify(producer, Mockito.times(0)).sendAsync(Mockito.any(Message.class));

        // 4. DLQ topic not exist
        Mockito.doReturn(CompletableFuture.completedFuture(null))
            .when(metadataService).topicOf(DLQ_TOPIC_ID);
        consumerGroup = ConsumerGroup.newBuilder()
            .setGroupId(CONSUMER_GROUP_ID)
            .setName(CONSUMER_GROUP_NAME)
            .setDeadLetterTopicId(DLQ_TOPIC_ID)
            .setGroupType(GroupType.GROUP_TYPE_STANDARD)
            .build();
        Mockito.doReturn(CompletableFuture.completedFuture(consumerGroup))
            .when(metadataService).consumerGroupOf(CONSUMER_GROUP_ID);

        dlqService.send(StoreContext.EMPTY, CONSUMER_GROUP_ID, msg).join();
        Mockito.verify(producer, Mockito.times(0)).sendAsync(Mockito.any(Message.class));
    }



}
