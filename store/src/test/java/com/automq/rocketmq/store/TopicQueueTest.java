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
import com.automq.rocketmq.metadata.StoreMetadataService;
import com.automq.rocketmq.store.api.MessageStateMachine;
import com.automq.rocketmq.store.api.StreamStore;
import com.automq.rocketmq.store.api.TopicQueue;
import com.automq.rocketmq.store.exception.StoreException;
import com.automq.rocketmq.store.mock.MockStoreMetadataService;
import com.automq.rocketmq.store.mock.MockStreamStore;
import com.automq.rocketmq.store.model.generated.CheckPoint;
import com.automq.rocketmq.store.model.message.AckResult;
import com.automq.rocketmq.store.model.message.Filter;
import com.automq.rocketmq.store.model.message.PopResult;
import com.automq.rocketmq.store.model.message.TagFilter;
import com.automq.rocketmq.store.service.InflightService;
import com.automq.rocketmq.store.service.RocksDBKVService;
import com.automq.rocketmq.store.service.api.KVService;
import com.automq.rocketmq.store.util.SerializeUtil;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static com.automq.rocketmq.store.mock.MockMessageUtil.buildMessage;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TopicQueueTest {

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

    @BeforeEach
    public void setUp() throws StoreException {
        kvService = new RocksDBKVService(PATH);
        metadataService = new MockStoreMetadataService();
        streamStore = new MockStreamStore();
        stateMachine = new DefaultMessageStateMachine(TOPIC_ID, QUEUE_ID, kvService);
        inflightService = new InflightService();
        topicQueue = new StreamTopicQueue(new StoreConfig(), TOPIC_ID, QUEUE_ID, metadataService, stateMachine,
            streamStore, inflightService);
        topicQueue.open().join();
    }

    @AfterEach
    public void tearDown() throws StoreException {
        kvService.destroy();
    }

    @Test
    void putWithPop() {
        FlatMessage message = FlatMessage.getRootAsFlatMessage(buildMessage(TOPIC_ID, QUEUE_ID, "TagA"));
        topicQueue.put(message);

        PopResult popResult = topicQueue.popNormal(CONSUMER_GROUP_ID, Filter.DEFAULT_FILTER, 1, 100).join();
        assertEquals(PopResult.Status.FOUND, popResult.status());
        assertFalse(popResult.messageList().isEmpty());
        assertEquals(popResult.messageList().size(), topicQueue.getInflightStats(CONSUMER_GROUP_ID).join());

        FlatMessageExt messageExt = popResult.messageList().get(0);
        assertEquals(message.topicId(), messageExt.message().topicId());
        assertEquals(message.queueId(), messageExt.message().queueId());
        assertEquals(message.payloadAsByteBuffer(), messageExt.message().payloadAsByteBuffer());
    }

    @Test
    void pop_normal() throws StoreException {

        // 1. append 5 messages
        for (int i = 0; i < 5; i++) {
            FlatMessage message = FlatMessage.getRootAsFlatMessage(buildMessage(TOPIC_ID, QUEUE_ID, "TagA"));
            topicQueue.put(message);
        }

        // 2. pop 2 messages
        PopResult popResult = topicQueue.popNormal(CONSUMER_GROUP_ID, Filter.DEFAULT_FILTER, 2, 100).join();
        assertEquals(PopResult.Status.FOUND, popResult.status());
        assertEquals(2, popResult.messageList().size());
        assertEquals(2, topicQueue.getInflightStats(CONSUMER_GROUP_ID).join());

        assertEquals(2, stateMachine.consumeOffset(CONSUMER_GROUP_ID).join());

        byte[] bytes = kvService.get(MessageStoreImpl.KV_NAMESPACE_CHECK_POINT, SerializeUtil.buildCheckPointKey(TOPIC_ID, QUEUE_ID, 0, popResult.operationId()));
        CheckPoint checkPoint = SerializeUtil.decodeCheckPoint(ByteBuffer.wrap(bytes));
        assertEquals(popResult.deliveryTimestamp() + 100, checkPoint.nextVisibleTimestamp());
        assertEquals(TOPIC_ID, checkPoint.topicId());
        assertEquals(QUEUE_ID, checkPoint.queueId());

        List<CheckPoint> allCheckPointList = new ArrayList<>();
        kvService.iterate(MessageStoreImpl.KV_NAMESPACE_CHECK_POINT, (key, value) ->
            allCheckPointList.add(CheckPoint.getRootAsCheckPoint(ByteBuffer.wrap(value))));

        assertEquals(2, allCheckPointList.size());
        for (CheckPoint point : allCheckPointList) {
            assertEquals(CONSUMER_GROUP_ID, point.consumerGroupId());
            assertEquals(TOPIC_ID, point.topicId());
            assertEquals(QUEUE_ID, point.queueId());
        }

        // pop 4 messages
        popResult = topicQueue.popNormal(CONSUMER_GROUP_ID, Filter.DEFAULT_FILTER, 4, 100).join();
        assertEquals(PopResult.Status.FOUND, popResult.status());
        assertEquals(3, popResult.messageList().size());
        assertEquals(5, topicQueue.getInflightStats(CONSUMER_GROUP_ID).join());
        allCheckPointList.clear();
        kvService.iterate(MessageStoreImpl.KV_NAMESPACE_CHECK_POINT, (key, value) ->
            allCheckPointList.add(CheckPoint.getRootAsCheckPoint(ByteBuffer.wrap(value))));

        assertEquals(5, allCheckPointList.size());
        for (CheckPoint point : allCheckPointList) {
            assertEquals(CONSUMER_GROUP_ID, point.consumerGroupId());
            assertEquals(TOPIC_ID, point.topicId());
            assertEquals(QUEUE_ID, point.queueId());
        }

        assertEquals(5, topicQueue.getInflightStats(CONSUMER_GROUP_ID).join());
        assertEquals(5, stateMachine.consumeOffset(CONSUMER_GROUP_ID).join());
    }

    @Test
    void pop_order() throws StoreException {
        // 1. append 5 messages
        for (int i = 0; i < 5; i++) {
            FlatMessage message = FlatMessage.getRootAsFlatMessage(buildMessage(TOPIC_ID, QUEUE_ID, "TagA"));
            topicQueue.put(message);
        }

        // 2. pop 2 messages
        PopResult popResult = topicQueue.popFifo(CONSUMER_GROUP_ID, Filter.DEFAULT_FILTER, 2, 100).join();
        assertEquals(PopResult.Status.FOUND, popResult.status());
        assertEquals(2, popResult.messageList().size());
        assertEquals(2, topicQueue.getInflightStats(CONSUMER_GROUP_ID).join());

        assertEquals(2, stateMachine.consumeOffset(CONSUMER_GROUP_ID).join());

        byte[] bytes = kvService.get(MessageStoreImpl.KV_NAMESPACE_CHECK_POINT, SerializeUtil.buildCheckPointKey(TOPIC_ID, QUEUE_ID, 0, popResult.operationId()));
        CheckPoint checkPoint = SerializeUtil.decodeCheckPoint(ByteBuffer.wrap(bytes));
        assertEquals(popResult.deliveryTimestamp() + 100, checkPoint.nextVisibleTimestamp());
        assertEquals(TOPIC_ID, checkPoint.topicId());
        assertEquals(QUEUE_ID, checkPoint.queueId());

        List<CheckPoint> allCheckPointList = new ArrayList<>();
        kvService.iterate(MessageStoreImpl.KV_NAMESPACE_CHECK_POINT, (key, value) ->
            allCheckPointList.add(CheckPoint.getRootAsCheckPoint(ByteBuffer.wrap(value))));

        assertEquals(2, allCheckPointList.size());
        for (CheckPoint point : allCheckPointList) {
            assertEquals(CONSUMER_GROUP_ID, point.consumerGroupId());
            assertEquals(TOPIC_ID, point.topicId());
            assertEquals(QUEUE_ID, point.queueId());
        }

        // pop 4 messages
        popResult = topicQueue.popFifo(CONSUMER_GROUP_ID, Filter.DEFAULT_FILTER, 4, 100).join();
        assertEquals(PopResult.Status.LOCKED, popResult.status());
        assertEquals(2, topicQueue.getInflightStats(CONSUMER_GROUP_ID).join());
        assertEquals(2, stateMachine.consumeOffset(CONSUMER_GROUP_ID).join());
    }

    @Test
    void pop_normal_multi_group() throws StoreException {
        long group0 = 0;
        long group1 = 1;

        // 1. append 5 messages
        for (int i = 0; i < 5; i++) {
            FlatMessage message = FlatMessage.getRootAsFlatMessage(buildMessage(TOPIC_ID, QUEUE_ID, "TagA"));
            topicQueue.put(message);
        }

        // 2. group0 pop 2 messages
        PopResult popResult = topicQueue.popNormal(group0, Filter.DEFAULT_FILTER, 2, 100).join();
        assertEquals(PopResult.Status.FOUND, popResult.status());
        assertEquals(2, popResult.messageList().size());
        assertEquals(2, topicQueue.getInflightStats(group0).join());
        assertEquals(2, stateMachine.consumeOffset(group0).join());

        // 3. group1 pop 4 messages
        popResult = topicQueue.popNormal(group1, Filter.DEFAULT_FILTER, 4, 100).join();
        assertEquals(PopResult.Status.FOUND, popResult.status());
        assertEquals(4, popResult.messageList().size());
        assertEquals(4, topicQueue.getInflightStats(group1).join());
        assertEquals(4, stateMachine.consumeOffset(group1).join());

        // 4. group0 pop 4 messages
        popResult = topicQueue.popNormal(group0, Filter.DEFAULT_FILTER, 4, 100).join();
        assertEquals(PopResult.Status.FOUND, popResult.status());
        assertEquals(3, popResult.messageList().size());
        assertEquals(5, topicQueue.getInflightStats(group0).join());
        assertEquals(5, stateMachine.consumeOffset(group0).join());
    }

    @Test
    void pop_order_multi_group() throws StoreException {
        long group0 = 0;
        long group1 = 1;

        // 1. append 5 messages
        for (int i = 0; i < 5; i++) {
            FlatMessage message = FlatMessage.getRootAsFlatMessage(buildMessage(TOPIC_ID, QUEUE_ID, "TagA"));
            topicQueue.put(message);
        }

        // 2. group0 pop 2 messages
        PopResult popResult = topicQueue.popFifo(group0, Filter.DEFAULT_FILTER, 2, 100).join();
        assertEquals(PopResult.Status.FOUND, popResult.status());
        assertEquals(2, popResult.messageList().size());
        assertEquals(2, topicQueue.getInflightStats(group0).join());
        assertEquals(2, stateMachine.consumeOffset(group0).join());

        // 3. group1 pop 4 messages
        popResult = topicQueue.popFifo(group1, Filter.DEFAULT_FILTER, 4, 100).join();
        assertEquals(PopResult.Status.FOUND, popResult.status());
        assertEquals(4, popResult.messageList().size());
        assertEquals(4, topicQueue.getInflightStats(group1).join());
        assertEquals(4, stateMachine.consumeOffset(group1).join());

        // 4. group0 pop 4 messages
        popResult = topicQueue.popFifo(group0, Filter.DEFAULT_FILTER, 4, 100).join();
        assertEquals(PopResult.Status.LOCKED, popResult.status());
    }

    @Test
    void pop_ack() throws StoreException {

        // 1. append 5 messages
        for (int i = 0; i < 5; i++) {
            FlatMessage message = FlatMessage.getRootAsFlatMessage(buildMessage(TOPIC_ID, QUEUE_ID, "TagA"));
            topicQueue.put(message);
        }

        // 2. pop 2 messages
        PopResult popResult = topicQueue.popFifo(CONSUMER_GROUP_ID, Filter.DEFAULT_FILTER, 2, 100).join();
        assertEquals(PopResult.Status.FOUND, popResult.status());
        assertEquals(2, popResult.messageList().size());
        assertEquals(2, topicQueue.getInflightStats(CONSUMER_GROUP_ID).join());
        assertEquals(2, stateMachine.consumeOffset(CONSUMER_GROUP_ID).join());
        String receiptHandle0 = popResult.messageList().get(0).receiptHandle().get();
        String receiptHandle1 = popResult.messageList().get(1).receiptHandle().get();

        // 3. pop 1 message
        popResult = topicQueue.popFifo(CONSUMER_GROUP_ID, Filter.DEFAULT_FILTER, 1, 100).join();
        assertEquals(PopResult.Status.LOCKED, popResult.status());

        // 3. ack 1 message
        AckResult ackResult = topicQueue.ack(receiptHandle0).join();
        assertEquals(AckResult.Status.SUCCESS, ackResult.status());
        assertEquals(1, topicQueue.getInflightStats(CONSUMER_GROUP_ID).join());
        assertEquals(2, stateMachine.consumeOffset(CONSUMER_GROUP_ID).join());
        assertEquals(1, stateMachine.ackOffset(CONSUMER_GROUP_ID).join());

        // 4. check ck
        byte[] bytes = kvService.get(MessageStoreImpl.KV_NAMESPACE_CHECK_POINT, SerializeUtil.buildCheckPointKey(TOPIC_ID, QUEUE_ID, 0, popResult.operationId()));
        assertNull(bytes);

        // 5. pop 1 message
        popResult = topicQueue.popFifo(CONSUMER_GROUP_ID, Filter.DEFAULT_FILTER, 1, 100).join();
        assertEquals(PopResult.Status.LOCKED, popResult.status());

        // 6. ack 1 message
        ackResult = topicQueue.ack(receiptHandle1).join();
        assertEquals(AckResult.Status.SUCCESS, ackResult.status());
        assertEquals(0, topicQueue.getInflightStats(CONSUMER_GROUP_ID).join());
        assertEquals(2, stateMachine.consumeOffset(CONSUMER_GROUP_ID).join());
        assertEquals(2, stateMachine.ackOffset(CONSUMER_GROUP_ID).join());

        // 7. pop 1 message
        popResult = topicQueue.popFifo(CONSUMER_GROUP_ID, Filter.DEFAULT_FILTER, 1, 100).join();
        assertEquals(PopResult.Status.FOUND, popResult.status());
        assertEquals(1, popResult.messageList().size());
        assertEquals(1, topicQueue.getInflightStats(CONSUMER_GROUP_ID).join());
        assertEquals(3, stateMachine.consumeOffset(CONSUMER_GROUP_ID).join());
        assertEquals(2, stateMachine.ackOffset(CONSUMER_GROUP_ID).join());

    }

    @Test
    void pop_ack_timeout() throws StoreException {
        // 1. append 5 messages
        for (int i = 0; i < 5; i++) {
            FlatMessage message = FlatMessage.getRootAsFlatMessage(buildMessage(TOPIC_ID, QUEUE_ID, "TagA"));
            topicQueue.put(message);
        }

        // 2. pop 2 messages
        PopResult popResult = topicQueue.popFifo(CONSUMER_GROUP_ID, Filter.DEFAULT_FILTER, 2, 100).join();
        assertEquals(PopResult.Status.FOUND, popResult.status());
        assertEquals(2, popResult.messageList().size());
        assertEquals(2, topicQueue.getInflightStats(CONSUMER_GROUP_ID).join());
        assertEquals(2, stateMachine.consumeOffset(CONSUMER_GROUP_ID).join());
        String receiptHandle0 = popResult.messageList().get(0).receiptHandle().get();
        String receiptHandle1 = popResult.messageList().get(1).receiptHandle().get();

        // 3. pop 1 message
        popResult = topicQueue.popFifo(CONSUMER_GROUP_ID, Filter.DEFAULT_FILTER, 1, 100).join();
        assertEquals(PopResult.Status.LOCKED, popResult.status());

        // 3. ack 1 message
        AckResult ackResult = topicQueue.ack(receiptHandle0).join();
        assertEquals(AckResult.Status.SUCCESS, ackResult.status());
        assertEquals(1, topicQueue.getInflightStats(CONSUMER_GROUP_ID).join());
        assertEquals(2, stateMachine.consumeOffset(CONSUMER_GROUP_ID).join());
        assertEquals(1, stateMachine.ackOffset(CONSUMER_GROUP_ID).join());

        // 4. check ck
        byte[] bytes = kvService.get(MessageStoreImpl.KV_NAMESPACE_CHECK_POINT, SerializeUtil.buildCheckPointKey(TOPIC_ID, QUEUE_ID, 0, popResult.operationId()));
        assertNull(bytes);

        // 5. pop 1 message
        popResult = topicQueue.popFifo(CONSUMER_GROUP_ID, Filter.DEFAULT_FILTER, 1, 100).join();
        assertEquals(PopResult.Status.LOCKED, popResult.status());

        // 6. ack 1 message with timeout
        ackResult = topicQueue.ackTimeout(receiptHandle1).join();
        assertEquals(AckResult.Status.SUCCESS, ackResult.status());
        assertEquals(0, topicQueue.getInflightStats(CONSUMER_GROUP_ID).join());
        assertEquals(2, stateMachine.consumeOffset(CONSUMER_GROUP_ID).join());
        assertEquals(1, stateMachine.ackOffset(CONSUMER_GROUP_ID).join());

        // 7. pop 1 message
        popResult = topicQueue.popFifo(CONSUMER_GROUP_ID, Filter.DEFAULT_FILTER, 1, 100).join();
        assertEquals(PopResult.Status.FOUND, popResult.status());
        assertEquals(1, popResult.messageList().size());
        assertEquals(1, topicQueue.getInflightStats(CONSUMER_GROUP_ID).join());
        assertEquals(2, stateMachine.consumeOffset(CONSUMER_GROUP_ID).join());
        assertEquals(1, stateMachine.ackOffset(CONSUMER_GROUP_ID).join());
    }

    @Test
    void pop_with_out_of_order_ack() throws StoreException {
        // 1. append 5 messages
        for (int i = 0; i < 5; i++) {
            FlatMessage message = FlatMessage.getRootAsFlatMessage(buildMessage(TOPIC_ID, QUEUE_ID, "TagA"));
            topicQueue.put(message);
        }

        // 2. pop 2 messages
        PopResult popResult = topicQueue.popFifo(CONSUMER_GROUP_ID, Filter.DEFAULT_FILTER, 2, 100).join();
        assertEquals(PopResult.Status.FOUND, popResult.status());
        assertEquals(2, popResult.messageList().size());
        assertEquals(2, topicQueue.getInflightStats(CONSUMER_GROUP_ID).join());
        assertEquals(2, stateMachine.consumeOffset(CONSUMER_GROUP_ID).join());
        String receiptHandle0 = popResult.messageList().get(0).receiptHandle().get();
        String receiptHandle1 = popResult.messageList().get(1).receiptHandle().get();

        // 3. pop 1 message
        popResult = topicQueue.popFifo(CONSUMER_GROUP_ID, Filter.DEFAULT_FILTER, 1, 100).join();
        assertEquals(PopResult.Status.LOCKED, popResult.status());

        // 3. ack offset_1
        AckResult ackResult = topicQueue.ack(receiptHandle1).join();
        assertEquals(AckResult.Status.SUCCESS, ackResult.status());
        assertEquals(1, topicQueue.getInflightStats(CONSUMER_GROUP_ID).join());
        assertEquals(2, stateMachine.consumeOffset(CONSUMER_GROUP_ID).join());
        assertEquals(0, stateMachine.ackOffset(CONSUMER_GROUP_ID).join());

        // 4. check ck
        byte[] bytes = kvService.get(MessageStoreImpl.KV_NAMESPACE_CHECK_POINT, SerializeUtil.buildCheckPointKey(TOPIC_ID, QUEUE_ID, 0, popResult.operationId()));
        assertNull(bytes);

        // 5. pop 1 message
        popResult = topicQueue.popFifo(CONSUMER_GROUP_ID, Filter.DEFAULT_FILTER, 1, 100).join();
        assertEquals(PopResult.Status.LOCKED, popResult.status());

        // 6. ack offset_0
        ackResult = topicQueue.ack(receiptHandle0).join();
        assertEquals(AckResult.Status.SUCCESS, ackResult.status());
        assertEquals(0, topicQueue.getInflightStats(CONSUMER_GROUP_ID).join());
        assertEquals(2, stateMachine.consumeOffset(CONSUMER_GROUP_ID).join());
        assertEquals(2, stateMachine.ackOffset(CONSUMER_GROUP_ID).join());

        // 7. pop 1 message
        popResult = topicQueue.popFifo(CONSUMER_GROUP_ID, Filter.DEFAULT_FILTER, 1, 100).join();
        assertEquals(PopResult.Status.FOUND, popResult.status());
        assertEquals(1, popResult.messageList().size());
        assertEquals(1, topicQueue.getInflightStats(CONSUMER_GROUP_ID).join());
        assertEquals(3, stateMachine.consumeOffset(CONSUMER_GROUP_ID).join());
        assertEquals(2, stateMachine.ackOffset(CONSUMER_GROUP_ID).join());
    }

    @Test
    void pop_with_filter() throws StoreException {
        // 1. append 5 messages with tagA
        for (int i = 0; i < 5; i++) {
            FlatMessage message = FlatMessage.getRootAsFlatMessage(buildMessage(TOPIC_ID, QUEUE_ID, "TagA"));
            topicQueue.put(message);
        }

        // 2. append 2 messages with tagB
        for (int i = 0; i < 2; i++) {
            FlatMessage message = FlatMessage.getRootAsFlatMessage(buildMessage(TOPIC_ID, QUEUE_ID, "TagB"));
            topicQueue.put(message);
        }
        // 3. append 3 messages with tagA
        for (int i = 0; i < 3; i++) {
            FlatMessage message = FlatMessage.getRootAsFlatMessage(buildMessage(TOPIC_ID, QUEUE_ID, "TagA"));
            topicQueue.put(message);
        }

        // 4. append 2 messages with tagB
        for (int i = 0; i < 2; i++) {
            FlatMessage message = FlatMessage.getRootAsFlatMessage(buildMessage(TOPIC_ID, QUEUE_ID, "TagB"));
            topicQueue.put(message);
        }

        // 4. pop 6 messages with tagA
        PopResult popResult = topicQueue.popNormal(CONSUMER_GROUP_ID, new TagFilter("TagA"), 6, 100).join();
        assertEquals(PopResult.Status.FOUND, popResult.status());
        assertEquals(6, popResult.messageList().size());
        popResult.messageList().forEach(messageExt -> assertEquals("TagA", messageExt.message().tag()));
        assertEquals(6, topicQueue.getInflightStats(CONSUMER_GROUP_ID).join());
        assertEquals(8, stateMachine.consumeOffset(CONSUMER_GROUP_ID).join());

        // 5. pop 3 messages with tagB
        popResult = topicQueue.popNormal(CONSUMER_GROUP_ID, new TagFilter("TagB"), 3, 100).join();
        assertEquals(PopResult.Status.FOUND, popResult.status());
        assertEquals(2, popResult.messageList().size());
        popResult.messageList().forEach(messageExt -> assertEquals("TagB", messageExt.message().tag()));
        assertEquals(8, topicQueue.getInflightStats(CONSUMER_GROUP_ID).join());
        assertEquals(12, stateMachine.consumeOffset(CONSUMER_GROUP_ID).join());
    }

    @Test
    void changeInvisibleDuration() throws StoreException {
        // 1. append message
        FlatMessage message = FlatMessage.getRootAsFlatMessage(buildMessage(TOPIC_ID, QUEUE_ID, "TagA"));
        topicQueue.put(message);

        // 2. pop message
        long popStartTimestamp = System.nanoTime();
        PopResult popResult = topicQueue.popNormal(CONSUMER_GROUP_ID, Filter.DEFAULT_FILTER, 1, 100).join();
        long popEndTimestamp = System.nanoTime();
        assertEquals(PopResult.Status.FOUND, popResult.status());
        assertFalse(popResult.messageList().isEmpty());
        assertEquals(1, topicQueue.getInflightStats(CONSUMER_GROUP_ID).join());

        byte[] checkPointKey = SerializeUtil.buildCheckPointKey(TOPIC_ID, QUEUE_ID, 0, popResult.operationId());
        byte[] bytes = kvService.get(MessageStoreImpl.KV_NAMESPACE_CHECK_POINT, checkPointKey);
        assertNotNull(bytes);

        CheckPoint checkPoint = CheckPoint.getRootAsCheckPoint(ByteBuffer.wrap(bytes));

        long lastVisibleTime = checkPoint.nextVisibleTimestamp();
        assertTrue(popStartTimestamp + 100 <= checkPoint.nextVisibleTimestamp());
        assertTrue(popEndTimestamp + 100 >= checkPoint.nextVisibleTimestamp());

        // 3. change invisible duration.
        FlatMessageExt messageExt = popResult.messageList().get(0);
        String receiptHandle = SerializeUtil.encodeReceiptHandle(CONSUMER_GROUP_ID, TOPIC_ID, QUEUE_ID, messageExt.offset(), popResult.operationId());
        long changeStartTimestamp = System.nanoTime();
        topicQueue.changeInvisibleDuration(receiptHandle, 1000L).join();
        long changeEndTimestamp = System.nanoTime();
        assertEquals(1, topicQueue.getInflightStats(CONSUMER_GROUP_ID).join());

        bytes = kvService.get(MessageStoreImpl.KV_NAMESPACE_CHECK_POINT, checkPointKey);
        assertNotNull(bytes);

        checkPoint = CheckPoint.getRootAsCheckPoint(ByteBuffer.wrap(bytes));

        assertTrue(changeStartTimestamp + 1000 <= checkPoint.nextVisibleTimestamp());
        assertTrue(changeEndTimestamp + 1000 >= checkPoint.nextVisibleTimestamp());
    }

}
