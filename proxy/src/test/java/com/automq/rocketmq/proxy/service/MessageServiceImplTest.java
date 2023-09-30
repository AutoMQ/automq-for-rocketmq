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
import com.automq.rocketmq.metadata.ProxyMetadataService;
import com.automq.rocketmq.proxy.mock.MockMessageStore;
import com.automq.rocketmq.proxy.mock.MockProxyMetadataService;
import com.automq.rocketmq.proxy.model.VirtualQueue;
import com.automq.rocketmq.store.api.MessageStore;
import com.automq.rocketmq.store.model.message.TagFilter;
import com.automq.rocketmq.proxy.util.FlatMessageUtil;
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
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.service.message.MessageService;
import org.apache.rocketmq.proxy.service.route.AddressableMessageQueue;
import org.apache.rocketmq.remoting.protocol.header.AckMessageRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.ChangeInvisibleTimeRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.PopMessageRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.QueryConsumerOffsetRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.SendMessageRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.UpdateConsumerOffsetRequestHeader;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

class MessageServiceImplTest {
    public static final String RECEIPT_HANDLE = "FAAAAAAAAAAMABwABAAAAAwAFAAMAAAAAgAAAAAAAAACAAAAAAAAAAMAAAAAAAAA";

    private ProxyMetadataService metadataService;
    private MessageStore messageStore;
    private MessageService messageService;

    @BeforeEach
    public void setUp() {
        metadataService = new MockProxyMetadataService();
        messageStore = new MockMessageStore();
        ProxyConfig config = new ProxyConfig();
        messageService = new MessageServiceImpl(config, messageStore, metadataService, new LockService(config));
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

        List<SendResult> resultList = messageService.sendMessage(ProxyContext.create(), messageQueue, List.of(message), header, 0).join();
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
        PopResult result = messageService.popMessage(ProxyContext.create(), null, header, 0L).join();
        assertEquals(PopStatus.NO_NEW_MSG, result.getPopStatus());

        header.setExpType(ExpressionType.TAG);
        header.setExp(TagFilter.SUB_ALL);
        long consumerGroupId = metadataService.queryConsumerGroupId(groupName);
        long topicId = metadataService.queryTopicId(topicName);
        messageStore.put(FlatMessageUtil.transferFrom(topicId, 0, new Message(topicName, "", new byte[] {})));
        messageStore.put(FlatMessageUtil.transferFrom(topicId, 0, new Message(topicName, "", new byte[] {})));


        result = messageService.popMessage(ProxyContext.create(), null, header, 0L).join();
        assertEquals(PopStatus.FOUND, result.getPopStatus());
        assertEquals(2, result.getMsgFoundList().size());
        // All messages in queue 0 has been consumed
        assertEquals(2, metadataService.queryConsumerOffset(consumerGroupId, topicId, 0));

        // Pop all queues.
        header.setQueueId(-1);
        header.setMaxMsgNums(4);
        messageStore.put(FlatMessageUtil.transferFrom(topicId, 1, new Message(topicName, "", new byte[] {})));
        messageStore.put(FlatMessageUtil.transferFrom(topicId, 2, new Message(topicName, "", new byte[] {})));
        messageStore.put(FlatMessageUtil.transferFrom(topicId, 2, new Message(topicName, "", new byte[] {})));
        messageStore.put(FlatMessageUtil.transferFrom(topicId, 4, new Message(topicName, "", new byte[] {})));
        messageStore.put(FlatMessageUtil.transferFrom(topicId, 4, new Message(topicName, "", new byte[] {})));
        messageStore.put(FlatMessageUtil.transferFrom(topicId, 4, new Message(topicName, "", new byte[] {})));


        result = messageService.popMessage(ProxyContext.create(), null, header, 0L).join();
        assertEquals(PopStatus.FOUND, result.getPopStatus());
        assertEquals(4, result.getMsgFoundList().size());
        // Queue 1 should not be touched because it is not assigned.
        assertEquals(0, metadataService.queryConsumerOffset(consumerGroupId, topicId, 1));

        // The priorities of queues 2 and 4 are not fixed, so there are the following two results:
        // 1. pop one message from queue 2 and three messages from queue 4
        // 2. pop two messages each from queue 2 and 4
        int messageFromQueue2 = 0;
        int messageFromQueue4 = 0;
        for (MessageExt messageExt : result.getMsgFoundList()) {
            switch (messageExt.getQueueId()) {
                case 2 -> messageFromQueue2++;
                case 4 -> messageFromQueue4++;
                default -> fail("All messages should be popped from queue 2 or 4.");
            }
        }
        assertEquals(messageFromQueue2, metadataService.queryConsumerOffset(consumerGroupId, topicId, 2));
        assertEquals(messageFromQueue4, metadataService.queryConsumerOffset(consumerGroupId, topicId, 4));

        // Pop remaining messages.
        header.setMaxMsgNums(1);
        result = messageService.popMessage(ProxyContext.create(), null, header, 0L).join();
        assertEquals(PopStatus.FOUND, result.getPopStatus());
        assertEquals(1, result.getMsgFoundList().size());

        int remainingQueue;
        if (messageFromQueue4 == 3) {
            remainingQueue = 2;
        } else {
            remainingQueue = 4;
        }

        MessageExt messageExt = result.getMsgFoundList().get(0);
        assertEquals(remainingQueue, messageExt.getQueueId());
        assertEquals(remainingQueue == 2 ? 1 : 2, messageExt.getQueueOffset());
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

        long topicId = metadataService.queryTopicId(topicName);
        messageStore.put(FlatMessageUtil.transferFrom(topicId, 0, new Message(topicName, "", new byte[] {})));
        messageStore.put(FlatMessageUtil.transferFrom(topicId, 0, new Message(topicName, "", new byte[] {})));
        messageStore.put(FlatMessageUtil.transferFrom(topicId, 0, new Message(topicName, "", new byte[] {})));

        // Pop message with client id "client1".
        ProxyContext context = ProxyContext.create();
        context.setClientID("client1");
        PopResult result = messageService.popMessage(context, null, header, 0L).join();
        assertEquals(PopStatus.FOUND, result.getPopStatus());
        assertEquals(1, result.getMsgFoundList().size());

        // Pop again with the same client id.
        result = messageService.popMessage(context, null, header, 0L).join();
        assertEquals(PopStatus.FOUND, result.getPopStatus());
        assertEquals(1, result.getMsgFoundList().size());

        // Pop with client id "client2".
        context.setClientID("client2");
        result = messageService.popMessage(context, null, header, 0L).join();
        assertEquals(PopStatus.NO_NEW_MSG, result.getPopStatus());
        assertEquals(0, result.getMsgFoundList().size());

        AckMessageRequestHeader ackHeader = new AckMessageRequestHeader();
        ackHeader.setExtraInfo(RECEIPT_HANDLE);
        ackHeader.setTopic(topicName);
        ackHeader.setQueueId(0);
        messageService.ackMessage(context, null, "", ackHeader, 0L);
        messageService.ackMessage(context, null, "", ackHeader, 0L);

        // Pop with client id "client2" after all message acked.
        context.setClientID("client2");
        await().atMost(3, TimeUnit.SECONDS)
            .until(() -> {
                PopResult client2Result = messageService.popMessage(context, null, header, 0L).join();
                return client2Result.getPopStatus() == PopStatus.FOUND && client2Result.getMsgFoundList().size() == 1;
            });
    }

    @Test
    void changeInvisibleTime() {
        ChangeInvisibleTimeRequestHeader header = new ChangeInvisibleTimeRequestHeader();
        header.setExtraInfo(RECEIPT_HANDLE);
        header.setInvisibleTime(100L);
        AckResult ackResult = messageService.changeInvisibleTime(ProxyContext.create(), null, null, header, 0).join();
        assertEquals(AckStatus.OK, ackResult.getStatus());

        header.setExtraInfo("");
        ackResult = messageService.changeInvisibleTime(ProxyContext.create(), null, null, header, 0).join();
        assertEquals(AckStatus.NO_EXIST, ackResult.getStatus());
    }

    @Test
    void ackMessage() {
        AckMessageRequestHeader header = new AckMessageRequestHeader();
        header.setExtraInfo(RECEIPT_HANDLE);
        header.setTopic("topic");
        header.setQueueId(0);
        AckResult ackResult = messageService.ackMessage(ProxyContext.create(), null, null, header, 0).join();
        assertEquals(AckStatus.OK, ackResult.getStatus());

        header.setExtraInfo("");
        ackResult = messageService.ackMessage(ProxyContext.create(), null, null, header, 0).join();
        assertEquals(AckStatus.NO_EXIST, ackResult.getStatus());
    }

    @Test
    void offset() {
        UpdateConsumerOffsetRequestHeader updateConsumerOffsetRequestHeader = new UpdateConsumerOffsetRequestHeader();
        updateConsumerOffsetRequestHeader.setConsumerGroup("group");
        updateConsumerOffsetRequestHeader.setTopic("topic");
        updateConsumerOffsetRequestHeader.setQueueId(0);
        updateConsumerOffsetRequestHeader.setCommitOffset(100L);
        messageService.updateConsumerOffset(ProxyContext.create(), null, updateConsumerOffsetRequestHeader, 0).join();

        QueryConsumerOffsetRequestHeader queryConsumerOffsetRequestHeader = new QueryConsumerOffsetRequestHeader();
        queryConsumerOffsetRequestHeader.setConsumerGroup("group");
        queryConsumerOffsetRequestHeader.setTopic("topic");
        queryConsumerOffsetRequestHeader.setQueueId(0);
        Long offset = messageService.queryConsumerOffset(ProxyContext.create(), null, queryConsumerOffsetRequestHeader, 0).join();
        assertEquals(100L, offset);
    }
}