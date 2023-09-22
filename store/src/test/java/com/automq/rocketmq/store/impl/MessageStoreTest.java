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

package com.automq.rocketmq.store.impl;

import com.automq.rocketmq.common.config.StoreConfig;
import com.automq.rocketmq.common.model.MessageExt;
import com.automq.rocketmq.common.model.generated.Message;
import com.automq.rocketmq.metadata.StoreMetadataService;
import com.automq.rocketmq.store.MessageStore;
import com.automq.rocketmq.store.StreamStore;
import com.automq.rocketmq.store.exception.StoreException;
import com.automq.rocketmq.store.mock.MockOperationLogService;
import com.automq.rocketmq.store.mock.MockStoreMetadataService;
import com.automq.rocketmq.store.model.generated.CheckPoint;
import com.automq.rocketmq.store.model.message.Filter;
import com.automq.rocketmq.store.model.message.PopResult;
import com.automq.rocketmq.store.model.message.TagFilter;
import com.automq.rocketmq.store.model.stream.SingleRecord;
import com.automq.rocketmq.store.service.KVService;
import com.automq.rocketmq.store.service.impl.RocksDBKVService;
import com.automq.rocketmq.store.util.SerializeUtil;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static com.automq.rocketmq.store.mock.MockMessageUtil.buildMessage;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MessageStoreTest {
    private static final String PATH = "/tmp/test_message_store/";

    private static KVService kvService;
    private static StoreMetadataService metadataService;
    private static StreamStore streamStore;
    private static MessageStore messageStore;

    @BeforeEach
    public void setUp() throws StoreException {
        kvService = new RocksDBKVService(PATH);
        metadataService = new MockStoreMetadataService();
        streamStore = new StreamStoreImpl();
        messageStore = new MessageStoreImpl(new StoreConfig(), streamStore, new MockOperationLogService(), metadataService, kvService);
    }

    @AfterEach
    public void tearDown() throws StoreException {
        kvService.destroy();
    }

    @Test
    void putWithPop() {
        Message message = Message.getRootAsMessage(buildMessage(1, 1, "TagA"));
        messageStore.put(message, new HashMap<>());

        PopResult popResult = messageStore.pop(1, 1, 1, 0, Filter.DEFAULT_FILTER, 1, false, false, 100).join();
        assertEquals(PopResult.Status.FOUND, popResult.status());
        assertFalse(popResult.messageList().isEmpty());
        assertEquals(popResult.messageList().size(), messageStore.getInflightStats(1, 1, 1));

        MessageExt messageExt = popResult.messageList().get(0);
        assertEquals(message.topicId(), messageExt.message().topicId());
        assertEquals(message.queueId(), messageExt.message().queueId());
        assertEquals(message.payloadAsByteBuffer(), messageExt.message().payloadAsByteBuffer());
    }

    @Test
    void pop() throws StoreException {
        long testStartTime = System.nanoTime();

        // Append mock message.
        long streamId = metadataService.getStreamId(1, 1);
        streamStore.append(streamId, new SingleRecord(new HashMap<>(), buildMessage(1, 1, ""))).join();
        streamId = metadataService.getStreamId(1, 2);
        streamStore.append(streamId, new SingleRecord(new HashMap<>(), buildMessage(1, 2, ""))).join();

        PopResult popResult = messageStore.pop(1, 1, 1, 0, Filter.DEFAULT_FILTER, 1, false, false, 100).join();
        assertEquals(PopResult.Status.FOUND, popResult.status());
        assertFalse(popResult.messageList().isEmpty());
        assertEquals(popResult.messageList().size(), messageStore.getInflightStats(1, 1, 1));

        MessageExt messageExt = popResult.messageList().get(0);
        byte[] bytes = kvService.get(MessageStoreImpl.KV_NAMESPACE_CHECK_POINT, SerializeUtil.buildCheckPointKey(1, 1, 0, popResult.operationId()));
        assertNotNull(bytes);

        CheckPoint checkPoint = CheckPoint.getRootAsCheckPoint(ByteBuffer.wrap(bytes));
        assertTrue(testStartTime < popResult.deliveryTimestamp());
        assertEquals(popResult.deliveryTimestamp(), checkPoint.deliveryTimestamp());
        assertEquals(popResult.deliveryTimestamp() + 100, checkPoint.nextVisibleTimestamp());
        assertEquals(1, checkPoint.consumerGroupId());
        assertEquals(1, checkPoint.topicId());
        assertEquals(1, checkPoint.queueId());
        assertEquals(messageExt.offset(), checkPoint.messageOffset());

        messageStore.pop(1, 1, 2, 0, Filter.DEFAULT_FILTER, 1, false, false, 100).join();
        assertEquals(1, messageStore.getInflightStats(1, 1, 2));

        List<CheckPoint> allCheckPointList = new ArrayList<>();
        kvService.iterate(MessageStoreImpl.KV_NAMESPACE_CHECK_POINT, (key, value) ->
            allCheckPointList.add(CheckPoint.getRootAsCheckPoint(ByteBuffer.wrap(value))));

        assertEquals(2, allCheckPointList.size());

        assertEquals(1, allCheckPointList.get(0).consumerGroupId());
        assertEquals(1, allCheckPointList.get(0).topicId());
        assertEquals(1, allCheckPointList.get(0).queueId());

        assertEquals(1, allCheckPointList.get(1).consumerGroupId());
        assertEquals(1, allCheckPointList.get(1).topicId());
        assertEquals(2, allCheckPointList.get(1).queueId());
    }

    @Test
    void popMultiple() throws StoreException {
        // Append mock message.
        long streamId = metadataService.getStreamId(1, 1);
        streamStore.append(streamId, new SingleRecord(new HashMap<>(), buildMessage(1, 1, ""))).join();
        streamStore.append(streamId, new SingleRecord(new HashMap<>(), buildMessage(1, 1, ""))).join();
        streamStore.append(streamId, new SingleRecord(new HashMap<>(), buildMessage(1, 1, ""))).join();
        streamStore.append(streamId, new SingleRecord(new HashMap<>(), buildMessage(1, 1, ""))).join();
        streamStore.append(streamId, new SingleRecord(new HashMap<>(), buildMessage(1, 1, ""))).join();

        PopResult popResult = messageStore.pop(1, 1, 1, 0, Filter.DEFAULT_FILTER, 32, true, false, 100).join();
        assertEquals(PopResult.Status.FOUND, popResult.status());
        assertEquals(5, popResult.messageList().size());
        assertEquals(5, messageStore.getInflightStats(1, 1, 1));

        for (int i = 0; i < 5; i++) {
            assertEquals(i, popResult.messageList().get(i).offset());
        }

        AtomicInteger checkPointCount = new AtomicInteger();
        kvService.iterate(MessageStoreImpl.KV_NAMESPACE_CHECK_POINT, (key, value) -> checkPointCount.getAndIncrement());
        assertEquals(5, checkPointCount.get());

        AtomicInteger orderIndexCount = new AtomicInteger();
        kvService.iterate(MessageStoreImpl.KV_NAMESPACE_FIFO_INDEX, (key, value) -> orderIndexCount.getAndIncrement());
        assertEquals(5, orderIndexCount.get());

        AtomicInteger timerTagCount = new AtomicInteger();
        kvService.iterate(MessageStoreImpl.KV_NAMESPACE_TIMER_TAG, (key, value) -> timerTagCount.getAndIncrement());
        assertEquals(5, timerTagCount.get());

        // Pop again.
        popResult = messageStore.pop(1, 1, 1, 0, Filter.DEFAULT_FILTER, 32, true, false, 100).join();
        assertEquals(PopResult.Status.FOUND, popResult.status());
        assertEquals(5, popResult.messageList().size());
        assertEquals(5, messageStore.getInflightStats(1, 1, 1));

        for (int i = 0; i < 5; i++) {
            assertEquals(i, popResult.messageList().get(i).offset());
        }

        checkPointCount.set(0);
        kvService.iterate(MessageStoreImpl.KV_NAMESPACE_CHECK_POINT, (key, value) -> {
            int index = checkPointCount.getAndIncrement();
            CheckPoint checkPoint = CheckPoint.getRootAsCheckPoint(ByteBuffer.wrap(value));
            assertEquals(index, checkPoint.messageOffset());
            assertEquals(1, checkPoint.reconsumeCount());
        });
        assertEquals(5, checkPointCount.get());

        orderIndexCount.set(0);
        kvService.iterate(MessageStoreImpl.KV_NAMESPACE_FIFO_INDEX, (key, value) -> orderIndexCount.getAndIncrement());
        assertEquals(5, orderIndexCount.get());

        timerTagCount.set(0);
        kvService.iterate(MessageStoreImpl.KV_NAMESPACE_TIMER_TAG, (key, value) -> timerTagCount.getAndIncrement());
        assertEquals(5, timerTagCount.get());

        // Change invisible duration and ack all messages.
        for (MessageExt messageExt : popResult.messageList()) {
            assertTrue(messageExt.receiptHandle().isPresent());
            messageStore.changeInvisibleDuration(messageExt.receiptHandle().get(), 1000).join();
            messageStore.ack(messageExt.receiptHandle().get()).join();
        }

        assertEquals(0, messageStore.getInflightStats(1, 1, 1));

        checkPointCount.set(0);
        kvService.iterate(MessageStoreImpl.KV_NAMESPACE_CHECK_POINT, (key, value) -> checkPointCount.getAndIncrement());
        assertEquals(0, checkPointCount.get());

        orderIndexCount.set(0);
        kvService.iterate(MessageStoreImpl.KV_NAMESPACE_FIFO_INDEX, (key, value) -> orderIndexCount.getAndIncrement());
        assertEquals(0, orderIndexCount.get());

        timerTagCount.set(0);
        kvService.iterate(MessageStoreImpl.KV_NAMESPACE_TIMER_TAG, (key, value) -> timerTagCount.getAndIncrement());
        assertEquals(0, timerTagCount.get());
    }

    @Test
    void popWithFilter() throws StoreException {
        // Append mock message.
        long streamId = metadataService.getStreamId(1, 1);
        streamStore.append(streamId, new SingleRecord(new HashMap<>(), buildMessage(1, 1, "tagA"))).join();
        streamStore.append(streamId, new SingleRecord(new HashMap<>(), buildMessage(1, 1, "tagA"))).join();
        streamStore.append(streamId, new SingleRecord(new HashMap<>(), buildMessage(1, 1, "tagA"))).join();
        streamStore.append(streamId, new SingleRecord(new HashMap<>(), buildMessage(1, 1, "tagA"))).join();

        streamStore.append(streamId, new SingleRecord(new HashMap<>(), buildMessage(1, 1, "tagB"))).join();
        streamStore.append(streamId, new SingleRecord(new HashMap<>(), buildMessage(1, 1, "tagA"))).join();
        streamStore.append(streamId, new SingleRecord(new HashMap<>(), buildMessage(1, 1, "tagA"))).join();
        streamStore.append(streamId, new SingleRecord(new HashMap<>(), buildMessage(1, 1, "tagA"))).join();

        streamStore.append(streamId, new SingleRecord(new HashMap<>(), buildMessage(1, 1, "tagB"))).join();
        streamStore.append(streamId, new SingleRecord(new HashMap<>(), buildMessage(1, 1, "tagB"))).join();

        PopResult popResult = messageStore.pop(1, 1, 1, 0, new TagFilter("tagB || tagC"), 2, true, false, 100).join();
        assertEquals(PopResult.Status.FOUND, popResult.status());
        assertEquals(2, popResult.messageList().size());
        assertEquals(2, messageStore.getInflightStats(1, 1, 1));

        MessageExt firstMessageExt = popResult.messageList().get(0);
        assertEquals(4, firstMessageExt.offset());
        assertEquals("tagB", firstMessageExt.message().tag());

        MessageExt secondMessageExt = popResult.messageList().get(1);
        assertEquals(8, secondMessageExt.offset());
        assertEquals("tagB", secondMessageExt.message().tag());

        AtomicInteger checkPointCount = new AtomicInteger();
        kvService.iterate(MessageStoreImpl.KV_NAMESPACE_CHECK_POINT, (key, value) -> checkPointCount.getAndIncrement());
        assertEquals(2, checkPointCount.get());

        AtomicInteger timerTagCount = new AtomicInteger();
        kvService.iterate(MessageStoreImpl.KV_NAMESPACE_TIMER_TAG, (key, value) -> timerTagCount.getAndIncrement());
        assertEquals(2, timerTagCount.get());

        // Ack all messages.
        for (MessageExt messageExt : popResult.messageList()) {
            assertTrue(messageExt.receiptHandle().isPresent());
            messageStore.changeInvisibleDuration(messageExt.receiptHandle().get(), 1000).join();
            messageStore.ack(messageExt.receiptHandle().get()).join();
        }

        assertEquals(0, messageStore.getInflightStats(1, 1, 1));

        checkPointCount.set(0);
        kvService.iterate(MessageStoreImpl.KV_NAMESPACE_CHECK_POINT, (key, value) -> checkPointCount.getAndIncrement());
        assertEquals(0, checkPointCount.get());

        timerTagCount.set(0);
        kvService.iterate(MessageStoreImpl.KV_NAMESPACE_TIMER_TAG, (key, value) -> timerTagCount.getAndIncrement());
        assertEquals(0, timerTagCount.get());
    }

    @Test
    void popOrderly() throws StoreException {
        // Append mock message.
        long streamId = metadataService.getStreamId(1, 1);
        streamStore.append(streamId, new SingleRecord(new HashMap<>(), buildMessage())).join();

        PopResult popResult = messageStore.pop(1, 1, 1, 0, Filter.DEFAULT_FILTER, 1, true, false, 100).join();
        assertEquals(PopResult.Status.FOUND, popResult.status());
        assertFalse(popResult.messageList().isEmpty());
        assertEquals(1, messageStore.getInflightStats(1, 1, 1));

        MessageExt messageExt = popResult.messageList().get(0);
        byte[] bytes = kvService.get(MessageStoreImpl.KV_NAMESPACE_CHECK_POINT, SerializeUtil.buildCheckPointKey(1, 1, messageExt.offset(), popResult.operationId()));
        assertNotNull(bytes);

        CheckPoint checkPoint = CheckPoint.getRootAsCheckPoint(ByteBuffer.wrap(bytes));
        assertTrue(checkPoint.fifo());
        assertEquals(popResult.operationId(), checkPoint.operationId());
        assertEquals(popResult.deliveryTimestamp(), checkPoint.deliveryTimestamp());
        assertEquals(popResult.deliveryTimestamp() + 100, checkPoint.nextVisibleTimestamp());
        assertEquals(0, checkPoint.reconsumeCount());

        bytes = kvService.get(MessageStoreImpl.KV_NAMESPACE_FIFO_INDEX, SerializeUtil.buildOrderIndexKey(1, 1, 1, messageExt.offset()));
        assertNotNull(bytes);

        assertEquals(popResult.operationId(), ByteBuffer.wrap(bytes).getLong());

        // Pop the same message again.
        popResult = messageStore.pop(1, 1, 1, 0, Filter.DEFAULT_FILTER, 1, true, false, 100).join();

        assertEquals(1, messageStore.getInflightStats(1, 1, 1));

        messageExt = popResult.messageList().get(0);
        bytes = kvService.get(MessageStoreImpl.KV_NAMESPACE_CHECK_POINT, SerializeUtil.buildCheckPointKey(1, 1, messageExt.offset(), popResult.operationId()));
        assertNotNull(bytes);

        checkPoint = CheckPoint.getRootAsCheckPoint(ByteBuffer.wrap(bytes));
        assertTrue(checkPoint.fifo());
        assertEquals(popResult.operationId(), checkPoint.operationId());
        assertEquals(popResult.deliveryTimestamp(), checkPoint.deliveryTimestamp());
        assertEquals(popResult.deliveryTimestamp() + 100, checkPoint.nextVisibleTimestamp());
        assertEquals(1, checkPoint.reconsumeCount());

        bytes = kvService.get(MessageStoreImpl.KV_NAMESPACE_FIFO_INDEX, SerializeUtil.buildOrderIndexKey(1, 1, 1, messageExt.offset()));
        assertNotNull(bytes);

        assertEquals(popResult.operationId(), ByteBuffer.wrap(bytes).getLong());

        AtomicInteger checkPointCount = new AtomicInteger();
        kvService.iterate(MessageStoreImpl.KV_NAMESPACE_CHECK_POINT, (key, value) -> checkPointCount.getAndIncrement());
        assertEquals(1, checkPointCount.get());

        AtomicInteger orderIndexCount = new AtomicInteger();
        kvService.iterate(MessageStoreImpl.KV_NAMESPACE_FIFO_INDEX, (key, value) -> orderIndexCount.getAndIncrement());
        assertEquals(1, orderIndexCount.get());

        AtomicInteger timerTagCount = new AtomicInteger();
        kvService.iterate(MessageStoreImpl.KV_NAMESPACE_TIMER_TAG, (key, value) -> timerTagCount.getAndIncrement());
        assertEquals(1, timerTagCount.get());
    }

    @Test
    void ack() throws StoreException {
        // Append mock message.
        long streamId = metadataService.getStreamId(1, 1);
        streamStore.append(streamId, new SingleRecord(new HashMap<>(), buildMessage())).join();

        PopResult popResult = messageStore.pop(1, 1, 1, 0, Filter.DEFAULT_FILTER, 1, true, false, 100).join();
        assertEquals(PopResult.Status.FOUND, popResult.status());
        assertFalse(popResult.messageList().isEmpty());
        assertEquals(1, messageStore.getInflightStats(1, 1, 1));

        MessageExt messageExt = popResult.messageList().get(0);
        messageStore.ack(SerializeUtil.encodeReceiptHandle(1, 1, messageExt.offset(), popResult.operationId())).join();

        assertEquals(0, messageStore.getInflightStats(1, 1, 1));

        AtomicInteger checkPointCount = new AtomicInteger();
        kvService.iterate(MessageStoreImpl.KV_NAMESPACE_CHECK_POINT, (key, value) -> checkPointCount.getAndIncrement());
        assertEquals(0, checkPointCount.get());

        AtomicInteger orderIndexCount = new AtomicInteger();
        kvService.iterate(MessageStoreImpl.KV_NAMESPACE_FIFO_INDEX, (key, value) -> orderIndexCount.getAndIncrement());
        assertEquals(0, orderIndexCount.get());

        AtomicInteger timerTagCount = new AtomicInteger();
        kvService.iterate(MessageStoreImpl.KV_NAMESPACE_TIMER_TAG, (key, value) -> timerTagCount.getAndIncrement());
        assertEquals(0, timerTagCount.get());
    }

    @Test
    void changeInvisibleDuration() throws StoreException {
        // Append mock message.
        long streamId = metadataService.getStreamId(1, 1);
        streamStore.append(streamId, new SingleRecord(new HashMap<>(), buildMessage())).join();

        // Pop the message to generate the check point.
        PopResult popResult = messageStore.pop(1, 1, 1, 0, Filter.DEFAULT_FILTER, 1, false, false, 100).join();
        assertEquals(PopResult.Status.FOUND, popResult.status());
        assertFalse(popResult.messageList().isEmpty());
        assertEquals(1, messageStore.getInflightStats(1, 1, 1));

        byte[] checkPointKey = SerializeUtil.buildCheckPointKey(1, 1, 0, popResult.operationId());
        byte[] bytes = kvService.get(MessageStoreImpl.KV_NAMESPACE_CHECK_POINT, checkPointKey);
        assertNotNull(bytes);

        CheckPoint checkPoint = CheckPoint.getRootAsCheckPoint(ByteBuffer.wrap(bytes));

        long popEndTimestamp = System.nanoTime();
        long lastVisibleTime = checkPoint.nextVisibleTimestamp();
        assertTrue(popEndTimestamp + 100 > checkPoint.nextVisibleTimestamp());

        // Change the invisible duration.
        MessageExt messageExt = popResult.messageList().get(0);
        String receiptHandle = SerializeUtil.encodeReceiptHandle(1, 1, messageExt.offset(), popResult.operationId());
        messageStore.changeInvisibleDuration(receiptHandle, 100_000_000_000L).join();

        assertEquals(1, messageStore.getInflightStats(1, 1, 1));

        bytes = kvService.get(MessageStoreImpl.KV_NAMESPACE_CHECK_POINT, checkPointKey);
        assertNotNull(bytes);

        checkPoint = CheckPoint.getRootAsCheckPoint(ByteBuffer.wrap(bytes));

        assertTrue(lastVisibleTime < checkPoint.nextVisibleTimestamp());
        assertTrue(popEndTimestamp + 100 < checkPoint.nextVisibleTimestamp());

        AtomicInteger checkPointCount = new AtomicInteger();
        kvService.iterate(MessageStoreImpl.KV_NAMESPACE_CHECK_POINT, (key, value) -> checkPointCount.getAndIncrement());
        assertEquals(1, checkPointCount.get());

        AtomicInteger timerTagCount = new AtomicInteger();
        kvService.iterate(MessageStoreImpl.KV_NAMESPACE_TIMER_TAG, (key, value) -> timerTagCount.getAndIncrement());
        assertEquals(1, timerTagCount.get());

        // Change the invisible duration again.
        messageExt = popResult.messageList().get(0);
        receiptHandle = SerializeUtil.encodeReceiptHandle(1, 1, messageExt.offset(), popResult.operationId());
        messageStore.changeInvisibleDuration(receiptHandle, 0L).join();

        checkPointCount.set(0);
        kvService.iterate(MessageStoreImpl.KV_NAMESPACE_CHECK_POINT, (key, value) -> checkPointCount.getAndIncrement());
        assertEquals(1, checkPointCount.get());

        timerTagCount.set(0);
        kvService.iterate(MessageStoreImpl.KV_NAMESPACE_TIMER_TAG, (key, value) -> timerTagCount.getAndIncrement());
        assertEquals(1, timerTagCount.get());

        // Ack the message with the same receipt handle.
        messageStore.ack(receiptHandle).join();

        checkPointCount.set(0);
        kvService.iterate(MessageStoreImpl.KV_NAMESPACE_CHECK_POINT, (key, value) -> checkPointCount.getAndIncrement());
        assertEquals(0, checkPointCount.get());

        timerTagCount.set(0);
        kvService.iterate(MessageStoreImpl.KV_NAMESPACE_TIMER_TAG, (key, value) -> timerTagCount.getAndIncrement());
        assertEquals(0, timerTagCount.get());
    }
}