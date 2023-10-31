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

import com.automq.rocketmq.common.config.ProxyConfig;
import com.automq.rocketmq.metadata.api.ProxyMetadataService;
import com.automq.rocketmq.proxy.mock.MockMessageStore;
import com.automq.rocketmq.proxy.mock.MockProxyMetadataService;
import com.automq.rocketmq.proxy.model.ProxyContextExt;
import com.automq.rocketmq.proxy.model.VirtualQueue;
import com.automq.rocketmq.proxy.util.FlatMessageUtil;
import com.automq.rocketmq.proxy.util.ReceiptHandleUtil;
import com.automq.rocketmq.store.api.DeadLetterSender;
import com.automq.rocketmq.store.api.MessageStore;
import com.automq.rocketmq.store.model.message.TagFilter;
import java.lang.reflect.Field;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.client.consumer.AckResult;
import org.apache.rocketmq.client.consumer.AckStatus;
import org.apache.rocketmq.client.consumer.PopResult;
import org.apache.rocketmq.client.consumer.PopStatus;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.common.filter.ExpressionType;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.config.Configuration;
import org.apache.rocketmq.proxy.config.ConfigurationManager;
import org.apache.rocketmq.proxy.service.message.MessageService;
import org.apache.rocketmq.proxy.service.route.AddressableMessageQueue;
import org.apache.rocketmq.remoting.protocol.header.AckMessageRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.ChangeInvisibleTimeRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.PopMessageRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.QueryConsumerOffsetRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.SendMessageRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.UpdateConsumerOffsetRequestHeader;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;

class MessageServiceImplTest {
    public static final String RECEIPT_HANDLE = "FAAAAAAAAAAMABwABAAAAAwAFAAMAAAAAgAAAAAAAAACAAAAAAAAAAMAAAAAAAAA";

    private ProxyMetadataService metadataService;
    private MessageStore messageStore;
    private MessageService messageService;

    @BeforeAll
    public static void setUpAll() throws Exception {
        Field field = ConfigurationManager.class.getDeclaredField("configuration");
        field.setAccessible(true);
        Configuration configuration = new Configuration();
        configuration.setProxyConfig(new org.apache.rocketmq.proxy.config.ProxyConfig());
        field.set(null, configuration);
    }

    @BeforeEach
    public void setUp() {
        metadataService = new MockProxyMetadataService();
        messageStore = new MockMessageStore();
        ProxyConfig config = new ProxyConfig();
        messageService = new MessageServiceImpl(config, messageStore, metadataService, new LockService(config), Mockito.mock(DeadLetterSender.class));
    }

    @Test
    void sendMessage() {
        String topicName = "topic";
        VirtualQueue virtualQueue = new VirtualQueue(2, 0);

        Message message = new Message(topicName, "tag", new byte[] {});
        SendMessageRequestHeader header = new SendMessageRequestHeader();
        header.setBname(virtualQueue.brokerName());
        header.setTopic(topicName);
        header.setQueueId(0);

        AddressableMessageQueue messageQueue = new AddressableMessageQueue(new MessageQueue(topicName, virtualQueue.brokerName(), 0), null);

        List<SendResult> resultList = messageService.sendMessage(ProxyContextExt.create(), messageQueue, List.of(message), header, 0).join();
        assertEquals(1, resultList.size());

        SendResult result = resultList.get(0);
        assertEquals(SendStatus.SEND_OK, result.getSendStatus());
        assertEquals(0, result.getQueueOffset());

        MessageQueue queue = result.getMessageQueue();
        assertEquals(header.getBname(), queue.getBrokerName());
        assertEquals(header.getTopic(), queue.getTopic());
        assertEquals(header.getQueueId(), queue.getQueueId());
    }

    @RepeatedTest(100)
    void popMessage() {
        // Pop queue 0.
        PopMessageRequestHeader header = new PopMessageRequestHeader();
        String groupName = "group";
        String topicName = "topic";
        header.setConsumerGroup(groupName);
        header.setTopic(topicName);
        header.setQueueId(0);
        header.setMaxMsgNums(32);

        VirtualQueue virtualQueue = new VirtualQueue(2, 0);
        AddressableMessageQueue messageQueue = new AddressableMessageQueue(new MessageQueue(topicName, virtualQueue.brokerName(), 0), null);

        PopResult result = messageService.popMessage(ProxyContextExt.create(), messageQueue, header, 0L).join();
        assertEquals(PopStatus.POLLING_NOT_FOUND, result.getPopStatus());

        header.setExpType(ExpressionType.TAG);
        header.setExp(TagFilter.SUB_ALL);
        long consumerGroupId = metadataService.consumerGroupOf(groupName).join().getGroupId();
        long topicId = metadataService.topicOf(topicName).join().getTopicId();
        messageStore.put(FlatMessageUtil.convertTo(topicId, 0, "", new Message(topicName, "", new byte[] {})));
        messageStore.put(FlatMessageUtil.convertTo(topicId, 0, "", new Message(topicName, "", new byte[] {})));

        result = messageService.popMessage(ProxyContextExt.create(), messageQueue, header, 0L).join();
        assertEquals(PopStatus.FOUND, result.getPopStatus());
        assertEquals(2, result.getMsgFoundList().size());
        // All messages in queue 0 has been consumed
        assertEquals(2, messageStore.getConsumeOffset(consumerGroupId, topicId, 0).join());

        // Pop again.
        result = messageService.popMessage(ProxyContextExt.create(), messageQueue, header, 0L).join();
        assertEquals(PopStatus.POLLING_NOT_FOUND, result.getPopStatus());
        assertEquals(0, result.getMsgFoundList().size());
    }

    @Test
    void pop_withFifo() {
        PopMessageRequestHeader header = new PopMessageRequestHeader();
        header.setConsumerGroup("group");
        String topicName = "topic";
        header.setTopic(topicName);
        header.setQueueId(0);
        header.setMaxMsgNums(1);
        header.setOrder(true);

        long topicId = metadataService.topicOf(topicName).join().getTopicId();
        messageStore.put(FlatMessageUtil.convertTo(topicId, 0, "", new Message(topicName, "", new byte[] {})));
        messageStore.put(FlatMessageUtil.convertTo(topicId, 0, "", new Message(topicName, "", new byte[] {})));
        messageStore.put(FlatMessageUtil.convertTo(topicId, 0, "", new Message(topicName, "", new byte[] {})));

        // Pop message with client id "client1".
        ProxyContext context = ProxyContextExt.create();
        context.setClientID("client1");

        VirtualQueue virtualQueue = new VirtualQueue(2, 0);
        AddressableMessageQueue messageQueue = new AddressableMessageQueue(new MessageQueue(topicName, virtualQueue.brokerName(), 0), null);

        PopResult result = messageService.popMessage(context, messageQueue, header, 0L).join();
        assertEquals(PopStatus.FOUND, result.getPopStatus());
        assertEquals(1, result.getMsgFoundList().size());

        // Pop again with the same client id.
        result = messageService.popMessage(context, messageQueue, header, 0L).join();
        assertEquals(PopStatus.FOUND, result.getPopStatus());
        assertEquals(1, result.getMsgFoundList().size());

        // Pop with client id "client2".
        context.setClientID("client2");
        result = messageService.popMessage(context, messageQueue, header, 0L).join();
        assertEquals(PopStatus.POLLING_NOT_FOUND, result.getPopStatus());
        assertEquals(0, result.getMsgFoundList().size());

        AckMessageRequestHeader ackHeader = new AckMessageRequestHeader();
        ackHeader.setExtraInfo(ReceiptHandleUtil.encodeReceiptHandle(RECEIPT_HANDLE, 0L));
        ackHeader.setTopic(topicName);
        ackHeader.setQueueId(0);
        ackHeader.setConsumerGroup("group");
        messageService.ackMessage(context, null, "", ackHeader, 0L);
        messageService.ackMessage(context, null, "", ackHeader, 0L);

        // Pop with client id "client2" after all message acked.
        context.setClientID("client2");
        await().atMost(3, TimeUnit.SECONDS)
            .until(() -> {
                PopResult client2Result = messageService.popMessage(context, messageQueue, header, 0L).join();
                return client2Result.getPopStatus() == PopStatus.FOUND && client2Result.getMsgFoundList().size() == 1;
            });
    }

    @Test
    void changeInvisibleTime() {
        ChangeInvisibleTimeRequestHeader header = new ChangeInvisibleTimeRequestHeader();
        header.setExtraInfo(ReceiptHandleUtil.encodeReceiptHandle(RECEIPT_HANDLE, 0L));
        header.setInvisibleTime(100L);
        AckResult ackResult = messageService.changeInvisibleTime(ProxyContextExt.create(), null, null, header, 0).join();
        assertEquals(AckStatus.OK, ackResult.getStatus());

        header.setExtraInfo("");
        ackResult = messageService.changeInvisibleTime(ProxyContextExt.create(), null, null, header, 0).join();
        assertEquals(AckStatus.NO_EXIST, ackResult.getStatus());
    }

    @Test
    void ackMessage() {
        AckMessageRequestHeader header = new AckMessageRequestHeader();
        header.setExtraInfo(ReceiptHandleUtil.encodeReceiptHandle(RECEIPT_HANDLE, 0L));
        header.setTopic("topic");
        header.setQueueId(0);
        header.setConsumerGroup("group");
        AckResult ackResult = messageService.ackMessage(ProxyContextExt.create(), null, null, header, 0).join();
        assertEquals(AckStatus.OK, ackResult.getStatus());

        header.setExtraInfo("");
        ackResult = messageService.ackMessage(ProxyContextExt.create(), null, null, header, 0).join();
        assertEquals(AckStatus.NO_EXIST, ackResult.getStatus());
    }

    @Test
    void offset() {
        UpdateConsumerOffsetRequestHeader updateConsumerOffsetRequestHeader = new UpdateConsumerOffsetRequestHeader();
        updateConsumerOffsetRequestHeader.setConsumerGroup("group");
        updateConsumerOffsetRequestHeader.setTopic("topic");
        updateConsumerOffsetRequestHeader.setQueueId(0);
        updateConsumerOffsetRequestHeader.setCommitOffset(100L);
        VirtualQueue virtualQueue = new VirtualQueue(1, 0);
        AddressableMessageQueue messageQueue = new AddressableMessageQueue(new MessageQueue("topic", virtualQueue.brokerName(), 0), null);
        messageService.updateConsumerOffset(ProxyContextExt.create(), messageQueue, updateConsumerOffsetRequestHeader, 0).join();

        QueryConsumerOffsetRequestHeader queryConsumerOffsetRequestHeader = new QueryConsumerOffsetRequestHeader();
        queryConsumerOffsetRequestHeader.setConsumerGroup("group");
        queryConsumerOffsetRequestHeader.setTopic("topic");
        queryConsumerOffsetRequestHeader.setQueueId(0);
        Long offset = messageService.queryConsumerOffset(ProxyContextExt.create(), null, queryConsumerOffsetRequestHeader, 0).join();
        assertEquals(100L, offset);
    }
}