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
import com.automq.rocketmq.store.api.LogicQueue;
import com.automq.rocketmq.store.api.MessageStateMachine;
import com.automq.rocketmq.store.api.StreamStore;
import com.automq.rocketmq.store.exception.StoreException;
import com.automq.rocketmq.store.mock.MockStoreMetadataService;
import com.automq.rocketmq.store.mock.MockStreamStore;
import com.automq.rocketmq.store.model.generated.CheckPoint;
import com.automq.rocketmq.store.model.generated.ReceiptHandle;
import com.automq.rocketmq.store.model.message.AckResult;
import com.automq.rocketmq.store.model.message.Filter;
import com.automq.rocketmq.store.model.message.PopResult;
import com.automq.rocketmq.store.model.message.TagFilter;
import com.automq.rocketmq.store.queue.DefaultLogicQueueStateMachine;
import com.automq.rocketmq.store.queue.StreamLogicQueue;
import com.automq.rocketmq.store.service.InflightService;
import com.automq.rocketmq.store.service.RocksDBKVService;
import com.automq.rocketmq.store.service.SnapshotService;
import com.automq.rocketmq.store.service.StreamOperationLogService;
import com.automq.rocketmq.store.service.api.KVService;
import com.automq.rocketmq.store.service.api.OperationLogService;
import com.automq.rocketmq.store.util.SerializeUtil;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import static com.automq.rocketmq.store.mock.MockMessageUtil.buildMessage;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class LogicQueueTest {
    private static final String PATH = "/tmp/ros/topic_queue_test/";
    private static final long TOPIC_ID = 1313;
    private static final int QUEUE_ID = 13;
    private static final long CONSUMER_GROUP_ID = 131313;
    private static final long DATA_STREAM_ID = 13131313;
    private static final long OP_STREAM_ID = 1313131313;
    private static final long SNAPSHOT_STREAM_ID = 131313131313L;
    private static final long EPOCH = 13131313131313L;

    private KVService kvService;
    private StoreMetadataService metadataService;
    private StreamStore streamStore;
    private MessageStateMachine stateMachine;
    private InflightService inflightService;
    private OperationLogService operationLogService;
    private SnapshotService snapshotService;
    private LogicQueue logicQueue;

    @BeforeEach
    public void setUp() throws StoreException {
        kvService = new RocksDBKVService(PATH);
        metadataService = new MockStoreMetadataService();
        streamStore = new MockStreamStore();
        stateMachine = Mockito.spy(new DefaultLogicQueueStateMachine(TOPIC_ID, QUEUE_ID, kvService));
        inflightService = new InflightService();
        snapshotService = new SnapshotService(streamStore, kvService);
        operationLogService = new StreamOperationLogService(streamStore, snapshotService, new StoreConfig());
        logicQueue = new StreamLogicQueue(new StoreConfig(), TOPIC_ID, QUEUE_ID,
            metadataService, stateMachine, streamStore, operationLogService, inflightService);
        logicQueue.open().join();
    }

    @AfterEach
    public void tearDown() throws StoreException {
        kvService.destroy();
    }

    @Test
    void putWithPop() {
        FlatMessage message = FlatMessage.getRootAsFlatMessage(buildMessage(TOPIC_ID, QUEUE_ID, "TagA"));
        logicQueue.put(message);

        PopResult popResult = logicQueue.popNormal(CONSUMER_GROUP_ID, Filter.DEFAULT_FILTER, 1, 100).join();
        assertEquals(PopResult.Status.FOUND, popResult.status());
        assertFalse(popResult.messageList().isEmpty());
        assertEquals(popResult.messageList().size(), logicQueue.getInflightStats(CONSUMER_GROUP_ID).join());

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
            logicQueue.put(message);
        }

        // 2. pop 2 messages
        PopResult popResult = logicQueue.popNormal(CONSUMER_GROUP_ID, Filter.DEFAULT_FILTER, 2, 100).join();
        assertEquals(PopResult.Status.FOUND, popResult.status());
        assertEquals(2, popResult.messageList().size());
        assertEquals(2, logicQueue.getInflightStats(CONSUMER_GROUP_ID).join());

        assertEquals(2, stateMachine.consumeOffset(CONSUMER_GROUP_ID).join());

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
        popResult = logicQueue.popNormal(CONSUMER_GROUP_ID, Filter.DEFAULT_FILTER, 4, 100).join();
        assertEquals(PopResult.Status.FOUND, popResult.status());
        assertEquals(3, popResult.messageList().size());
        assertEquals(5, logicQueue.getInflightStats(CONSUMER_GROUP_ID).join());
        allCheckPointList.clear();
        kvService.iterate(MessageStoreImpl.KV_NAMESPACE_CHECK_POINT, (key, value) ->
            allCheckPointList.add(CheckPoint.getRootAsCheckPoint(ByteBuffer.wrap(value))));

        assertEquals(5, allCheckPointList.size());
        for (CheckPoint point : allCheckPointList) {
            assertEquals(CONSUMER_GROUP_ID, point.consumerGroupId());
            assertEquals(TOPIC_ID, point.topicId());
            assertEquals(QUEUE_ID, point.queueId());
        }

        assertEquals(5, logicQueue.getInflightStats(CONSUMER_GROUP_ID).join());
        assertEquals(5, stateMachine.consumeOffset(CONSUMER_GROUP_ID).join());
    }

    @Test
    void pop_order() throws StoreException {
        // 1. append 5 messages
        for (int i = 0; i < 5; i++) {
            FlatMessage message = FlatMessage.getRootAsFlatMessage(buildMessage(TOPIC_ID, QUEUE_ID, "TagA"));
            logicQueue.put(message);
        }

        // 2. pop 2 messages
        PopResult popResult = logicQueue.popFifo(CONSUMER_GROUP_ID, Filter.DEFAULT_FILTER, 2, 100).join();
        assertEquals(PopResult.Status.FOUND, popResult.status());
        assertEquals(2, popResult.messageList().size());
        assertEquals(2, logicQueue.getInflightStats(CONSUMER_GROUP_ID).join());

        assertEquals(2, stateMachine.consumeOffset(CONSUMER_GROUP_ID).join());

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
        popResult = logicQueue.popFifo(CONSUMER_GROUP_ID, Filter.DEFAULT_FILTER, 4, 100).join();
        assertEquals(PopResult.Status.LOCKED, popResult.status());
        assertEquals(2, logicQueue.getInflightStats(CONSUMER_GROUP_ID).join());
        assertEquals(2, stateMachine.consumeOffset(CONSUMER_GROUP_ID).join());
    }

    @Test
    void pop_normal_multi_group() throws StoreException {
        long group0 = 0;
        long group1 = 1;

        // 1. append 5 messages
        for (int i = 0; i < 5; i++) {
            FlatMessage message = FlatMessage.getRootAsFlatMessage(buildMessage(TOPIC_ID, QUEUE_ID, "TagA"));
            logicQueue.put(message);
        }

        // 2. group0 pop 2 messages
        PopResult popResult = logicQueue.popNormal(group0, Filter.DEFAULT_FILTER, 2, 100).join();
        assertEquals(PopResult.Status.FOUND, popResult.status());
        assertEquals(2, popResult.messageList().size());
        assertEquals(2, logicQueue.getInflightStats(group0).join());
        assertEquals(2, stateMachine.consumeOffset(group0).join());

        // 3. group1 pop 4 messages
        popResult = logicQueue.popNormal(group1, Filter.DEFAULT_FILTER, 4, 100).join();
        assertEquals(PopResult.Status.FOUND, popResult.status());
        assertEquals(4, popResult.messageList().size());
        assertEquals(4, logicQueue.getInflightStats(group1).join());
        assertEquals(4, stateMachine.consumeOffset(group1).join());

        // 4. group0 pop 4 messages
        popResult = logicQueue.popNormal(group0, Filter.DEFAULT_FILTER, 4, 100).join();
        assertEquals(PopResult.Status.FOUND, popResult.status());
        assertEquals(3, popResult.messageList().size());
        assertEquals(5, logicQueue.getInflightStats(group0).join());
        assertEquals(5, stateMachine.consumeOffset(group0).join());
    }

    @Test
    void pop_order_multi_group() throws StoreException {
        long group0 = 0;
        long group1 = 1;

        // 1. append 5 messages
        for (int i = 0; i < 5; i++) {
            FlatMessage message = FlatMessage.getRootAsFlatMessage(buildMessage(TOPIC_ID, QUEUE_ID, "TagA"));
            logicQueue.put(message);
        }

        // 2. group0 pop 2 messages
        PopResult popResult = logicQueue.popFifo(group0, Filter.DEFAULT_FILTER, 2, 100).join();
        assertEquals(PopResult.Status.FOUND, popResult.status());
        assertEquals(2, popResult.messageList().size());
        assertEquals(2, logicQueue.getInflightStats(group0).join());
        assertEquals(2, stateMachine.consumeOffset(group0).join());

        // 3. group1 pop 4 messages
        popResult = logicQueue.popFifo(group1, Filter.DEFAULT_FILTER, 4, 100).join();
        assertEquals(PopResult.Status.FOUND, popResult.status());
        assertEquals(4, popResult.messageList().size());
        assertEquals(4, logicQueue.getInflightStats(group1).join());
        assertEquals(4, stateMachine.consumeOffset(group1).join());

        // 4. group0 pop 4 messages
        popResult = logicQueue.popFifo(group0, Filter.DEFAULT_FILTER, 4, 100).join();
        assertEquals(PopResult.Status.LOCKED, popResult.status());
    }

    @Test
    void pop_ack() throws StoreException {

        // 1. append 5 messages
        for (int i = 0; i < 5; i++) {
            FlatMessage message = FlatMessage.getRootAsFlatMessage(buildMessage(TOPIC_ID, QUEUE_ID, "TagA"));
            logicQueue.put(message);
        }

        // 2. pop 2 messages
        PopResult popResult = logicQueue.popFifo(CONSUMER_GROUP_ID, Filter.DEFAULT_FILTER, 2, 100).join();
        assertEquals(PopResult.Status.FOUND, popResult.status());
        assertEquals(2, popResult.messageList().size());
        assertEquals(2, logicQueue.getInflightStats(CONSUMER_GROUP_ID).join());
        assertEquals(2, stateMachine.consumeOffset(CONSUMER_GROUP_ID).join());
        String receiptHandle0 = popResult.messageList().get(0).receiptHandle().get();
        String receiptHandle1 = popResult.messageList().get(1).receiptHandle().get();

        // 3. pop 1 message
        popResult = logicQueue.popFifo(CONSUMER_GROUP_ID, Filter.DEFAULT_FILTER, 1, 100).join();
        assertEquals(PopResult.Status.LOCKED, popResult.status());

        // 3. ack 1 message
        AckResult ackResult = logicQueue.ack(receiptHandle0).join();
        assertEquals(AckResult.Status.SUCCESS, ackResult.status());
        assertEquals(1, logicQueue.getInflightStats(CONSUMER_GROUP_ID).join());
        assertEquals(2, stateMachine.consumeOffset(CONSUMER_GROUP_ID).join());
        assertEquals(1, stateMachine.ackOffset(CONSUMER_GROUP_ID).join());

        // 4. check ck
        ReceiptHandle handle0 = SerializeUtil.decodeReceiptHandle(receiptHandle0);
        byte[] bytes = kvService.get(MessageStoreImpl.KV_NAMESPACE_CHECK_POINT, SerializeUtil.buildCheckPointKey(TOPIC_ID, QUEUE_ID, handle0.operationId()));
        assertNull(bytes);

        // 5. pop 1 message
        popResult = logicQueue.popFifo(CONSUMER_GROUP_ID, Filter.DEFAULT_FILTER, 1, 100).join();
        assertEquals(PopResult.Status.LOCKED, popResult.status());

        // 6. ack 1 message
        ackResult = logicQueue.ack(receiptHandle1).join();
        assertEquals(AckResult.Status.SUCCESS, ackResult.status());
        assertEquals(0, logicQueue.getInflightStats(CONSUMER_GROUP_ID).join());
        assertEquals(2, stateMachine.consumeOffset(CONSUMER_GROUP_ID).join());
        assertEquals(2, stateMachine.ackOffset(CONSUMER_GROUP_ID).join());

        // 7. pop 1 message
        popResult = logicQueue.popFifo(CONSUMER_GROUP_ID, Filter.DEFAULT_FILTER, 1, 100).join();
        assertEquals(PopResult.Status.FOUND, popResult.status());
        assertEquals(1, popResult.messageList().size());
        assertEquals(1, logicQueue.getInflightStats(CONSUMER_GROUP_ID).join());
        assertEquals(3, stateMachine.consumeOffset(CONSUMER_GROUP_ID).join());
        assertEquals(2, stateMachine.ackOffset(CONSUMER_GROUP_ID).join());

    }

    @Test
    void pop_ack_timeout() throws StoreException {
        // 1. append 5 messages
        for (int i = 0; i < 5; i++) {
            FlatMessage message = FlatMessage.getRootAsFlatMessage(buildMessage(TOPIC_ID, QUEUE_ID, "TagA"));
            logicQueue.put(message);
        }

        // 2. pop 2 messages
        PopResult popResult = logicQueue.popFifo(CONSUMER_GROUP_ID, Filter.DEFAULT_FILTER, 2, 100).join();
        assertEquals(PopResult.Status.FOUND, popResult.status());
        assertEquals(2, popResult.messageList().size());
        assertEquals(2, logicQueue.getInflightStats(CONSUMER_GROUP_ID).join());
        assertEquals(2, stateMachine.consumeOffset(CONSUMER_GROUP_ID).join());
        String receiptHandle0 = popResult.messageList().get(0).receiptHandle().get();
        String receiptHandle1 = popResult.messageList().get(1).receiptHandle().get();

        // 3. pop 1 message
        popResult = logicQueue.popFifo(CONSUMER_GROUP_ID, Filter.DEFAULT_FILTER, 1, 100).join();
        assertEquals(PopResult.Status.LOCKED, popResult.status());

        // 3. ack 1 message
        AckResult ackResult = logicQueue.ack(receiptHandle0).join();
        assertEquals(AckResult.Status.SUCCESS, ackResult.status());
        assertEquals(1, logicQueue.getInflightStats(CONSUMER_GROUP_ID).join());
        assertEquals(2, stateMachine.consumeOffset(CONSUMER_GROUP_ID).join());
        assertEquals(1, stateMachine.ackOffset(CONSUMER_GROUP_ID).join());

        // 4. check ck
        ReceiptHandle handle0 = SerializeUtil.decodeReceiptHandle(receiptHandle0);
        byte[] bytes = kvService.get(MessageStoreImpl.KV_NAMESPACE_CHECK_POINT, SerializeUtil.buildCheckPointKey(TOPIC_ID, QUEUE_ID, handle0.operationId()));
        assertNull(bytes);

        // 5. pop 1 message
        popResult = logicQueue.popFifo(CONSUMER_GROUP_ID, Filter.DEFAULT_FILTER, 1, 100).join();
        assertEquals(PopResult.Status.LOCKED, popResult.status());

        // 6. ack 1 message with timeout
        ackResult = logicQueue.ackTimeout(receiptHandle1).join();
        assertEquals(AckResult.Status.SUCCESS, ackResult.status());
        assertEquals(0, logicQueue.getInflightStats(CONSUMER_GROUP_ID).join());
        assertEquals(2, stateMachine.consumeOffset(CONSUMER_GROUP_ID).join());
        assertEquals(1, stateMachine.ackOffset(CONSUMER_GROUP_ID).join());

        // 7. pop 1 message
        popResult = logicQueue.popFifo(CONSUMER_GROUP_ID, Filter.DEFAULT_FILTER, 1, 100).join();
        assertEquals(PopResult.Status.FOUND, popResult.status());
        assertEquals(1, popResult.messageList().size());
        assertEquals(1, logicQueue.getInflightStats(CONSUMER_GROUP_ID).join());
        assertEquals(2, stateMachine.consumeOffset(CONSUMER_GROUP_ID).join());
        assertEquals(1, stateMachine.ackOffset(CONSUMER_GROUP_ID).join());
    }

    @Test
    void pop_with_out_of_order_ack() throws StoreException {
        // 1. append 5 messages
        for (int i = 0; i < 5; i++) {
            FlatMessage message = FlatMessage.getRootAsFlatMessage(buildMessage(TOPIC_ID, QUEUE_ID, "TagA"));
            logicQueue.put(message);
        }

        // 2. pop 2 messages
        PopResult popResult = logicQueue.popFifo(CONSUMER_GROUP_ID, Filter.DEFAULT_FILTER, 2, 100).join();
        assertEquals(PopResult.Status.FOUND, popResult.status());
        assertEquals(2, popResult.messageList().size());
        assertEquals(2, logicQueue.getInflightStats(CONSUMER_GROUP_ID).join());
        assertEquals(2, stateMachine.consumeOffset(CONSUMER_GROUP_ID).join());
        String receiptHandle0 = popResult.messageList().get(0).receiptHandle().get();
        String receiptHandle1 = popResult.messageList().get(1).receiptHandle().get();

        // 3. pop 1 message
        popResult = logicQueue.popFifo(CONSUMER_GROUP_ID, Filter.DEFAULT_FILTER, 1, 100).join();
        assertEquals(PopResult.Status.LOCKED, popResult.status());

        // 3. ack offset_1
        AckResult ackResult = logicQueue.ack(receiptHandle1).join();
        assertEquals(AckResult.Status.SUCCESS, ackResult.status());
        assertEquals(1, logicQueue.getInflightStats(CONSUMER_GROUP_ID).join());
        assertEquals(2, stateMachine.consumeOffset(CONSUMER_GROUP_ID).join());
        assertEquals(0, stateMachine.ackOffset(CONSUMER_GROUP_ID).join());

        // 4. check ck
        ReceiptHandle handle1 = SerializeUtil.decodeReceiptHandle(receiptHandle1);
        byte[] bytes = kvService.get(MessageStoreImpl.KV_NAMESPACE_CHECK_POINT, SerializeUtil.buildCheckPointKey(TOPIC_ID, QUEUE_ID, handle1.operationId()));
        assertNull(bytes);

        // 5. pop 1 message
        popResult = logicQueue.popFifo(CONSUMER_GROUP_ID, Filter.DEFAULT_FILTER, 1, 100).join();
        assertEquals(PopResult.Status.LOCKED, popResult.status());

        // 6. ack offset_0
        ackResult = logicQueue.ack(receiptHandle0).join();
        assertEquals(AckResult.Status.SUCCESS, ackResult.status());
        assertEquals(0, logicQueue.getInflightStats(CONSUMER_GROUP_ID).join());
        assertEquals(2, stateMachine.consumeOffset(CONSUMER_GROUP_ID).join());
        assertEquals(2, stateMachine.ackOffset(CONSUMER_GROUP_ID).join());

        // 7. pop 1 message
        popResult = logicQueue.popFifo(CONSUMER_GROUP_ID, Filter.DEFAULT_FILTER, 1, 100).join();
        assertEquals(PopResult.Status.FOUND, popResult.status());
        assertEquals(1, popResult.messageList().size());
        assertEquals(1, logicQueue.getInflightStats(CONSUMER_GROUP_ID).join());
        assertEquals(3, stateMachine.consumeOffset(CONSUMER_GROUP_ID).join());
        assertEquals(2, stateMachine.ackOffset(CONSUMER_GROUP_ID).join());
    }

    @Test
    void pop_with_filter() throws StoreException {
        // 1. append 5 messages with tagA
        for (int i = 0; i < 5; i++) {
            FlatMessage message = FlatMessage.getRootAsFlatMessage(buildMessage(TOPIC_ID, QUEUE_ID, "TagA"));
            logicQueue.put(message);
        }

        // 2. append 2 messages with tagB
        for (int i = 0; i < 2; i++) {
            FlatMessage message = FlatMessage.getRootAsFlatMessage(buildMessage(TOPIC_ID, QUEUE_ID, "TagB"));
            logicQueue.put(message);
        }
        // 3. append 3 messages with tagA
        for (int i = 0; i < 3; i++) {
            FlatMessage message = FlatMessage.getRootAsFlatMessage(buildMessage(TOPIC_ID, QUEUE_ID, "TagA"));
            logicQueue.put(message);
        }

        // 4. append 2 messages with tagB
        for (int i = 0; i < 2; i++) {
            FlatMessage message = FlatMessage.getRootAsFlatMessage(buildMessage(TOPIC_ID, QUEUE_ID, "TagB"));
            logicQueue.put(message);
        }

        // 4. pop 6 messages with tagA
        PopResult popResult = logicQueue.popNormal(CONSUMER_GROUP_ID, new TagFilter("TagA"), 6, 100).join();
        assertEquals(PopResult.Status.FOUND, popResult.status());
        assertEquals(6, popResult.messageList().size());
        popResult.messageList().forEach(messageExt -> assertEquals("TagA", messageExt.message().tag()));
        assertEquals(6, logicQueue.getInflightStats(CONSUMER_GROUP_ID).join());
        assertEquals(8, stateMachine.consumeOffset(CONSUMER_GROUP_ID).join());

        // 5. pop 3 messages with tagB
        popResult = logicQueue.popNormal(CONSUMER_GROUP_ID, new TagFilter("TagB"), 3, 100).join();
        assertEquals(PopResult.Status.FOUND, popResult.status());
        assertEquals(2, popResult.messageList().size());
        popResult.messageList().forEach(messageExt -> assertEquals("TagB", messageExt.message().tag()));
        assertEquals(8, logicQueue.getInflightStats(CONSUMER_GROUP_ID).join());
        assertEquals(12, stateMachine.consumeOffset(CONSUMER_GROUP_ID).join());
    }

    @Test
    void pop_fifo_filter_ack() {
        // build 9 messages like this: A, A, B, A, A, A, B, A, A
        for (int i = 0; i < 2; i++) {
            FlatMessage message = FlatMessage.getRootAsFlatMessage(buildMessage(TOPIC_ID, QUEUE_ID, "TagA"));
            logicQueue.put(message);
        }
        for (int i = 0; i < 1; i++) {
            FlatMessage message = FlatMessage.getRootAsFlatMessage(buildMessage(TOPIC_ID, QUEUE_ID, "TagB"));
            logicQueue.put(message);
        }
        for (int i = 0; i < 3; i++) {
            FlatMessage message = FlatMessage.getRootAsFlatMessage(buildMessage(TOPIC_ID, QUEUE_ID, "TagA"));
            logicQueue.put(message);
        }
        for (int i = 0; i < 1; i++) {
            FlatMessage message = FlatMessage.getRootAsFlatMessage(buildMessage(TOPIC_ID, QUEUE_ID, "TagB"));
            logicQueue.put(message);
        }
        for (int i = 0; i < 2; i++) {
            FlatMessage message = FlatMessage.getRootAsFlatMessage(buildMessage(TOPIC_ID, QUEUE_ID, "TagA"));
            logicQueue.put(message);
        }

        // 1. pop fifo with TagB
        PopResult popResult = logicQueue.popFifo(CONSUMER_GROUP_ID, new TagFilter("TagB"), 7, 100).join();
        assertEquals(PopResult.Status.FOUND, popResult.status());
        List<FlatMessageExt> popMessageList = popResult.messageList();
        assertEquals(2, popMessageList.size());
        popResult.messageList().forEach(messageExt -> assertEquals("TagB", messageExt.message().tag()));
        assertEquals(2, logicQueue.getInflightStats(CONSUMER_GROUP_ID).join());
        assertEquals(9, stateMachine.consumeOffset(CONSUMER_GROUP_ID).join());

        // 2. append 1 message with TagB
        FlatMessage message = FlatMessage.getRootAsFlatMessage(buildMessage(TOPIC_ID, QUEUE_ID, "TagB"));
        logicQueue.put(message);

        // 2. pop fifo again
        popResult = logicQueue.popFifo(CONSUMER_GROUP_ID, new TagFilter("TagB"), 1, 100).join();
        assertEquals(PopResult.Status.LOCKED, popResult.status());

        // 3. ack second message in result
        AckResult ackResult = logicQueue.ack(popMessageList.get(1).receiptHandle().get()).join();
        assertEquals(AckResult.Status.SUCCESS, ackResult.status());
        assertEquals(0, stateMachine.ackOffset(CONSUMER_GROUP_ID).join());

        // 4. pop fifo again
        popResult = logicQueue.popFifo(CONSUMER_GROUP_ID, new TagFilter("TagB"), 1, 100).join();
        assertEquals(PopResult.Status.LOCKED, popResult.status());

        // 5. ack first message in result
        ackResult = logicQueue.ack(popMessageList.get(0).receiptHandle().get()).join();
        assertEquals(AckResult.Status.SUCCESS, ackResult.status());
        assertEquals(9, stateMachine.ackOffset(CONSUMER_GROUP_ID).join());

        // 6. pop fifo again
        popResult = logicQueue.popFifo(CONSUMER_GROUP_ID, new TagFilter("TagB"), 1, 100).join();
        assertEquals(PopResult.Status.FOUND, popResult.status());
        assertEquals(1, popResult.messageList().size());
        assertEquals(1, logicQueue.getInflightStats(CONSUMER_GROUP_ID).join());
        assertEquals(10, stateMachine.consumeOffset(CONSUMER_GROUP_ID).join());
    }

    @Test
    void changeInvisibleDuration() throws StoreException {
        // 1. append message
        FlatMessage message = FlatMessage.getRootAsFlatMessage(buildMessage(TOPIC_ID, QUEUE_ID, "TagA"));
        logicQueue.put(message);

        // 2. pop message
        long popStartTimestamp = System.currentTimeMillis();
        PopResult popResult = logicQueue.popNormal(CONSUMER_GROUP_ID, Filter.DEFAULT_FILTER, 1, 100).join();
        long popEndTimestamp = System.currentTimeMillis();
        assertEquals(PopResult.Status.FOUND, popResult.status());
        assertFalse(popResult.messageList().isEmpty());
        assertEquals(1, logicQueue.getInflightStats(CONSUMER_GROUP_ID).join());
        ReceiptHandle handle = SerializeUtil.decodeReceiptHandle(popResult.messageList().get(0).receiptHandle().get());
        byte[] checkPointKey = SerializeUtil.buildCheckPointKey(TOPIC_ID, QUEUE_ID, handle.operationId());
        byte[] bytes = kvService.get(MessageStoreImpl.KV_NAMESPACE_CHECK_POINT, checkPointKey);
        assertNotNull(bytes);

        CheckPoint checkPoint = CheckPoint.getRootAsCheckPoint(ByteBuffer.wrap(bytes));

        long lastVisibleTime = checkPoint.nextVisibleTimestamp();
        assertTrue(popStartTimestamp + 100 <= checkPoint.nextVisibleTimestamp());
        assertTrue(popEndTimestamp + 100 >= checkPoint.nextVisibleTimestamp());

        // 3. change invisible duration.
        FlatMessageExt messageExt = popResult.messageList().get(0);
        String receiptHandle = SerializeUtil.encodeReceiptHandle(CONSUMER_GROUP_ID, TOPIC_ID, QUEUE_ID, handle.operationId());
        long changeStartTimestamp = System.currentTimeMillis();
        logicQueue.changeInvisibleDuration(receiptHandle, 1000L).join();
        long changeEndTimestamp = System.currentTimeMillis();
        assertEquals(1, logicQueue.getInflightStats(CONSUMER_GROUP_ID).join());

        bytes = kvService.get(MessageStoreImpl.KV_NAMESPACE_CHECK_POINT, checkPointKey);
        assertNotNull(bytes);

        checkPoint = CheckPoint.getRootAsCheckPoint(ByteBuffer.wrap(bytes));

        assertTrue(changeStartTimestamp + 1000 <= checkPoint.nextVisibleTimestamp());
        assertTrue(changeEndTimestamp + 1000 >= checkPoint.nextVisibleTimestamp());
    }

    @Test
    void pop_retry() throws StoreException {
        // 1. append 5 messages to retry queue
        for (int i = 0; i < 5; i++) {
            FlatMessage message = FlatMessage.getRootAsFlatMessage(buildMessage(TOPIC_ID, QUEUE_ID, "TagA"));
            logicQueue.putRetry(CONSUMER_GROUP_ID, message);
        }

        // 2. pop 2 messages
        PopResult popResult = logicQueue.popRetry(CONSUMER_GROUP_ID, Filter.DEFAULT_FILTER, 2, 100).join();
        assertEquals(PopResult.Status.FOUND, popResult.status());
        assertEquals(2, popResult.messageList().size());
        assertEquals(2, logicQueue.getInflightStats(CONSUMER_GROUP_ID).join());
        assertEquals(0, stateMachine.consumeOffset(CONSUMER_GROUP_ID).join());
        assertEquals(0, stateMachine.ackOffset(CONSUMER_GROUP_ID).join());
        assertEquals(2, stateMachine.retryConsumeOffset(CONSUMER_GROUP_ID).join());
        assertEquals(0, stateMachine.retryAckOffset(CONSUMER_GROUP_ID).join());
        String receiptHandle0 = popResult.messageList().get(0).receiptHandle().get();
        String receiptHandle1 = popResult.messageList().get(1).receiptHandle().get();

        // 3. pop 1 message
        popResult = logicQueue.popRetry(CONSUMER_GROUP_ID, Filter.DEFAULT_FILTER, 1, 100).join();
        assertEquals(PopResult.Status.FOUND, popResult.status());
        assertEquals(1, popResult.messageList().size());
        assertEquals(3, logicQueue.getInflightStats(CONSUMER_GROUP_ID).join());
        assertEquals(0, stateMachine.consumeOffset(CONSUMER_GROUP_ID).join());
        assertEquals(0, stateMachine.ackOffset(CONSUMER_GROUP_ID).join());
        assertEquals(3, stateMachine.retryConsumeOffset(CONSUMER_GROUP_ID).join());
        assertEquals(0, stateMachine.retryAckOffset(CONSUMER_GROUP_ID).join());
        String receiptHandle2 = popResult.messageList().get(0).receiptHandle().get();

        // 3. ack msg_0
        AckResult ackResult = logicQueue.ack(receiptHandle0).join();
        assertEquals(AckResult.Status.SUCCESS, ackResult.status());
        assertEquals(2, logicQueue.getInflightStats(CONSUMER_GROUP_ID).join());
        assertEquals(0, stateMachine.consumeOffset(CONSUMER_GROUP_ID).join());
        assertEquals(0, stateMachine.ackOffset(CONSUMER_GROUP_ID).join());
        assertEquals(3, stateMachine.retryConsumeOffset(CONSUMER_GROUP_ID).join());
        assertEquals(1, stateMachine.retryAckOffset(CONSUMER_GROUP_ID).join());

        // 4. check ck

        // 5. ack msg_2 timeout
        ackResult = logicQueue.ackTimeout(receiptHandle2).join();
        assertEquals(AckResult.Status.SUCCESS, ackResult.status());
        assertEquals(1, logicQueue.getInflightStats(CONSUMER_GROUP_ID).join());
        assertEquals(0, stateMachine.consumeOffset(CONSUMER_GROUP_ID).join());
        assertEquals(0, stateMachine.ackOffset(CONSUMER_GROUP_ID).join());
        assertEquals(3, stateMachine.retryConsumeOffset(CONSUMER_GROUP_ID).join());
        assertEquals(1, stateMachine.retryAckOffset(CONSUMER_GROUP_ID).join());

        // 6. ack msg_1
        ackResult = logicQueue.ack(receiptHandle1).join();
        assertEquals(AckResult.Status.SUCCESS, ackResult.status());
        assertEquals(0, logicQueue.getInflightStats(CONSUMER_GROUP_ID).join());
        assertEquals(0, stateMachine.consumeOffset(CONSUMER_GROUP_ID).join());
        assertEquals(0, stateMachine.ackOffset(CONSUMER_GROUP_ID).join());
        assertEquals(3, stateMachine.retryConsumeOffset(CONSUMER_GROUP_ID).join());
        assertEquals(3, stateMachine.retryAckOffset(CONSUMER_GROUP_ID).join());
    }

    @Test
    void open_close() {
        // 1. append 5 messages
        for (int i = 0; i < 5; i++) {
            FlatMessage message = FlatMessage.getRootAsFlatMessage(buildMessage(TOPIC_ID, QUEUE_ID, "TagA"));
            logicQueue.put(message);
        }

        // 2. pop 2 messages
        PopResult popResult = logicQueue.popFifo(CONSUMER_GROUP_ID, Filter.DEFAULT_FILTER, 2, 100).join();
        assertEquals(PopResult.Status.FOUND, popResult.status());
        assertEquals(2, popResult.messageList().size());
        assertEquals(2, logicQueue.getInflightStats(CONSUMER_GROUP_ID).join());
        assertEquals(2, stateMachine.consumeOffset(CONSUMER_GROUP_ID).join());
        String receiptHandle0 = popResult.messageList().get(0).receiptHandle().get();
        String receiptHandle1 = popResult.messageList().get(1).receiptHandle().get();

        // 3. check ck exist
        checkCkExist(receiptHandle0, true);
        checkCkExist(receiptHandle1, true);
        assertEquals(2, scanAllTimerTag().size());

        // 4. close normally
        logicQueue.close().join();

        // 5. check ck not exist
        checkCkExist(receiptHandle0, false);
        checkCkExist(receiptHandle1, false);
        assertEquals(0, scanAllTimerTag().size());

        // 6. open again
        logicQueue = new StreamLogicQueue(new StoreConfig(), TOPIC_ID, QUEUE_ID,
            metadataService, stateMachine, streamStore, operationLogService, inflightService);
        logicQueue.open().join();

        // 7. check ck exist
        checkCkExist(receiptHandle0, true);
        checkCkExist(receiptHandle1, true);
        assertEquals(2, scanAllTimerTag().size());
        assertEquals(2, stateMachine.consumeOffset(CONSUMER_GROUP_ID).join());
        assertEquals(0, stateMachine.ackOffset(CONSUMER_GROUP_ID).join());
    }

    @Test
    void open_close_ungracefully() {
        // 1. append 5 messages
        for (int i = 0; i < 5; i++) {
            FlatMessage message = FlatMessage.getRootAsFlatMessage(buildMessage(TOPIC_ID, QUEUE_ID, "TagA"));
            logicQueue.put(message);
        }

        // 2. pop 2 messages
        PopResult popResult = logicQueue.popFifo(CONSUMER_GROUP_ID, Filter.DEFAULT_FILTER, 2, 100).join();
        assertEquals(PopResult.Status.FOUND, popResult.status());
        assertEquals(2, popResult.messageList().size());
        assertEquals(2, logicQueue.getInflightStats(CONSUMER_GROUP_ID).join());
        assertEquals(2, stateMachine.consumeOffset(CONSUMER_GROUP_ID).join());
        assertEquals(0, stateMachine.ackOffset(CONSUMER_GROUP_ID).join());
        String receiptHandle0 = popResult.messageList().get(0).receiptHandle().get();
        String receiptHandle1 = popResult.messageList().get(1).receiptHandle().get();

        // 3. check ck exist
        checkCkExist(receiptHandle0, true);
        checkCkExist(receiptHandle1, true);
        assertEquals(2, scanAllTimerTag().size());

        // 4. mock that close ungracefully, stream closed but states is not cleaned
        Mockito.doReturn(CompletableFuture.completedFuture(null)).when(stateMachine).clear();
        logicQueue.close().join();

        // 5. check ck exist
        checkCkExist(receiptHandle0, true);
        checkCkExist(receiptHandle1, true);
        assertEquals(2, scanAllTimerTag().size());

        // 5. open again
        Mockito.doAnswer(ink -> {
            ink.callRealMethod();
            // 6. check ck not exist
            checkCkExist(receiptHandle0, false);
            checkCkExist(receiptHandle1, false);
            assertEquals(0, scanAllTimerTag().size());
            return CompletableFuture.completedFuture(null);
        }).when(stateMachine).clear();
        logicQueue = new StreamLogicQueue(new StoreConfig(), TOPIC_ID, QUEUE_ID,
            metadataService, stateMachine, streamStore, operationLogService, inflightService);
        logicQueue.open().join();

        // 5. check ck exist
        checkCkExist(receiptHandle0, true);
        checkCkExist(receiptHandle1, true);
        assertEquals(2, scanAllTimerTag().size());
        assertEquals(2, stateMachine.consumeOffset(CONSUMER_GROUP_ID).join());
        assertEquals(0, stateMachine.ackOffset(CONSUMER_GROUP_ID).join());
    }

    private void checkCkExist(String receiptHandle, boolean expectExist) {
        try {
            ReceiptHandle handle0 = SerializeUtil.decodeReceiptHandle(receiptHandle);
            byte[] bytes = kvService.get(MessageStoreImpl.KV_NAMESPACE_CHECK_POINT, SerializeUtil.buildCheckPointKey(TOPIC_ID, QUEUE_ID, handle0.operationId()));
            if (expectExist) {
                assertNotNull(bytes);
            } else {
                assertNull(bytes);
            }
        } catch (Exception e) {
            Assertions.fail(e);
        }
    }

    private List<ReceiptHandle> scanAllTimerTag() {
        List<ReceiptHandle> receiptHandleList = new ArrayList<>();
        try {
            // Iterate timer tag until now to find messages need to reconsume.
            kvService.iterate(MessageStoreImpl.KV_NAMESPACE_TIMER_TAG, (key, value) -> {
                ReceiptHandle receiptHandle = ReceiptHandle.getRootAsReceiptHandle(ByteBuffer.wrap(value));
                receiptHandleList.add(receiptHandle);
            });
        } catch (Exception e) {
            Assertions.fail(e);
        }
        return receiptHandleList;
    }
}
