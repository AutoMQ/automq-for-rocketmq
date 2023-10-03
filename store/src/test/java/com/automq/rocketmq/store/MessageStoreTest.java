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

package com.automq.rocketmq.store;

import com.automq.rocketmq.common.config.StoreConfig;
import com.automq.rocketmq.common.model.FlatMessageExt;
import com.automq.rocketmq.common.model.generated.FlatMessage;
import com.automq.rocketmq.metadata.api.StoreMetadataService;
import com.automq.rocketmq.store.api.MessageStateMachine;
import com.automq.rocketmq.store.api.MessageStore;
import com.automq.rocketmq.store.api.StreamStore;
import com.automq.rocketmq.store.api.TopicQueue;
import com.automq.rocketmq.store.api.TopicQueueManager;
import com.automq.rocketmq.store.mock.MockStoreMetadataService;
import com.automq.rocketmq.store.mock.MockStreamStore;
import com.automq.rocketmq.store.model.message.Filter;
import com.automq.rocketmq.store.model.message.PopResult;
import com.automq.rocketmq.store.service.InflightService;
import com.automq.rocketmq.store.service.RocksDBKVService;
import com.automq.rocketmq.store.service.api.KVService;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import static com.automq.rocketmq.store.mock.MockMessageUtil.buildMessage;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class MessageStoreTest {
    private static final String PATH = "/tmp/ros/topic_queue_test/";
    private static final long TOPIC_ID = 1313;
    private static final int QUEUE_ID = 13;
    private static final long CONSUMER_GROUP_ID = 131313;

    private static KVService kvService;
    private static StoreMetadataService metadataService;
    private static StreamStore streamStore;
    private static MessageStateMachine stateMachine;
    private static InflightService inflightService;
    private static TopicQueue topicQueue;
    private static MessageStore messageStore;

    @BeforeEach
    public void setUp() throws Exception {
        kvService = new RocksDBKVService(PATH);
        metadataService = new MockStoreMetadataService();
        streamStore = new MockStreamStore();
        stateMachine = new DefaultMessageStateMachine(TOPIC_ID, QUEUE_ID, kvService);
        inflightService = new InflightService();
        topicQueue = new StreamTopicQueue(new StoreConfig(), TOPIC_ID, QUEUE_ID, metadataService, stateMachine,
            streamStore, inflightService);
        topicQueue.open().join();
        TopicQueueManager manager = Mockito.mock(TopicQueueManager.class);
        Mockito.when(manager.get(TOPIC_ID, QUEUE_ID)).thenReturn(topicQueue);
        messageStore = new MessageStoreImpl(new StoreConfig(), streamStore, metadataService, kvService, manager);
        messageStore.start();
    }

    @AfterEach
    public void tearDown() throws Exception {
        messageStore.shutdown();
        kvService.destroy();
    }

    @Test
    public void pop_normal() throws Exception {
        // 1. append 5 message
        for (int i = 0; i < 5; i++) {
            FlatMessage message = FlatMessage.getRootAsFlatMessage(buildMessage(TOPIC_ID, QUEUE_ID, "TagA"));
            messageStore.put(message).join();
        }
        List<String> receiptHandles = new ArrayList<>();
        // 2. pop 3 message
        PopResult popResult = messageStore.pop(CONSUMER_GROUP_ID, TOPIC_ID, QUEUE_ID, Filter.DEFAULT_FILTER, 3, false, false, 800 * 1000 * 1000).join();
        assertEquals(3, popResult.messageList().size());
        for (FlatMessageExt message : popResult.messageList()) {
            receiptHandles.add(message.receiptHandle().get());
        }
        // 3. pop 3 message
        popResult = messageStore.pop(CONSUMER_GROUP_ID, TOPIC_ID, QUEUE_ID, Filter.DEFAULT_FILTER, 3, false, false, 800 * 1000 * 1000).join();
        assertEquals(2, popResult.messageList().size());
        for (FlatMessageExt message : popResult.messageList()) {
            receiptHandles.add(message.receiptHandle().get());
        }

        // 4. ack msg_2, msg_3
        messageStore.ack(receiptHandles.get(2)).join();
        messageStore.ack(receiptHandles.get(3)).join();

        // 5. pop again
        popResult = messageStore.pop(CONSUMER_GROUP_ID, TOPIC_ID, QUEUE_ID, Filter.DEFAULT_FILTER, 3, false, false, 800 * 1000 * 1000).join();
        assertEquals(PopResult.Status.NOT_FOUND, popResult.status());
        popResult = messageStore.pop(CONSUMER_GROUP_ID, TOPIC_ID, QUEUE_ID, Filter.DEFAULT_FILTER, 3, false, true, 800 * 1000 * 1000).join();
        assertEquals(PopResult.Status.NOT_FOUND, popResult.status());

        // 6. after 1100ms, pop again
        Thread.sleep(1100);
        popResult = messageStore.pop(CONSUMER_GROUP_ID, TOPIC_ID, QUEUE_ID, Filter.DEFAULT_FILTER, 3, false, false, 800 * 1000 * 1000).join();
        assertEquals(PopResult.Status.NOT_FOUND, popResult.status());
        popResult = messageStore.pop(CONSUMER_GROUP_ID, TOPIC_ID, QUEUE_ID, Filter.DEFAULT_FILTER, 3, false, true, 800 * 1000 * 1000).join();
        assertEquals(PopResult.Status.FOUND, popResult.status());
        assertEquals(3, popResult.messageList().size());
        assertEquals(0, popResult.messageList().get(0).offset());
        assertEquals(0, popResult.messageList().get(0).originalOffset());
        assertEquals(1, popResult.messageList().get(1).offset());
        assertEquals(1, popResult.messageList().get(1).originalOffset());
        assertEquals(2, popResult.messageList().get(2).offset());
        assertEquals(4, popResult.messageList().get(2).originalOffset());

    }

    @Test
    public void pop_order() throws Exception {
        // 1. append 5 message
        for (int i = 0; i < 5; i++) {
            FlatMessage message = FlatMessage.getRootAsFlatMessage(buildMessage(TOPIC_ID, QUEUE_ID, "TagA"));
            messageStore.put(message).join();
        }
        List<String> receiptHandles = new ArrayList<>();
        // 2. pop 3 message
        PopResult popResult = messageStore.pop(CONSUMER_GROUP_ID, TOPIC_ID, QUEUE_ID, Filter.DEFAULT_FILTER, 3, true, false, 800 * 1000 * 1000).join();
        assertEquals(3, popResult.messageList().size());
        for (FlatMessageExt message : popResult.messageList()) {
            receiptHandles.add(message.receiptHandle().get());
        }
        // 3. pop 3 message
        popResult = messageStore.pop(CONSUMER_GROUP_ID, TOPIC_ID, QUEUE_ID, Filter.DEFAULT_FILTER, 3, true, false, 800 * 1000 * 1000).join();
        assertEquals(PopResult.Status.LOCKED, popResult.status());

        // 4. ack msg_1
        messageStore.ack(receiptHandles.get(1)).join();

        // 5. pop again
        popResult = messageStore.pop(CONSUMER_GROUP_ID, TOPIC_ID, QUEUE_ID, Filter.DEFAULT_FILTER, 3, true, false, 800 * 1000 * 1000).join();
        assertEquals(PopResult.Status.LOCKED, popResult.status());

        // 6. ack msg_0
        messageStore.ack(receiptHandles.get(0)).join();

        // 7. pop again
        popResult = messageStore.pop(CONSUMER_GROUP_ID, TOPIC_ID, QUEUE_ID, Filter.DEFAULT_FILTER, 3, true, false, 800 * 1000 * 1000).join();
        assertEquals(PopResult.Status.LOCKED, popResult.status());

        // 8. ack msg_2
        messageStore.ack(receiptHandles.get(2)).join();

        // 9. pop again
        popResult = messageStore.pop(CONSUMER_GROUP_ID, TOPIC_ID, QUEUE_ID, Filter.DEFAULT_FILTER, 3, true, false, 800 * 1000 * 1000).join();
        assertEquals(PopResult.Status.FOUND, popResult.status());
        assertEquals(2, popResult.messageList().size());
        assertEquals(3, popResult.messageList().get(0).offset());
        assertEquals(4, popResult.messageList().get(1).offset());
        for (int i = 0; i < 2; i++) {
            receiptHandles.add(popResult.messageList().get(i).receiptHandle().get());
        }

        // 10. pop again
        popResult = messageStore.pop(CONSUMER_GROUP_ID, TOPIC_ID, QUEUE_ID, Filter.DEFAULT_FILTER, 3, true, false, 800 * 1000 * 1000).join();
        assertEquals(PopResult.Status.LOCKED, popResult.status());

        // 11. after 1100ms, pop again
        Thread.sleep(1100);
        popResult = messageStore.pop(CONSUMER_GROUP_ID, TOPIC_ID, QUEUE_ID, Filter.DEFAULT_FILTER, 3, true, false, 800 * 1000 * 1000).join();
        assertEquals(PopResult.Status.FOUND, popResult.status());
        assertEquals(2, popResult.messageList().size());
        assertEquals(3, popResult.messageList().get(0).offset());
        assertEquals(4, popResult.messageList().get(1).offset());

    }
}
