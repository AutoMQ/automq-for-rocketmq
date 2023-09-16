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

import com.automq.rocketmq.common.model.Message;
import com.automq.rocketmq.store.MessageStore;
import com.automq.rocketmq.store.mock.MockOperationLogService;
import com.automq.rocketmq.store.model.generated.CheckPoint;
import com.automq.rocketmq.store.model.message.PopResult;
import com.automq.rocketmq.store.service.KVService;
import com.automq.rocketmq.store.service.impl.RocksDBKVService;
import com.automq.rocketmq.store.util.SerializeUtil;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.rocksdb.RocksDBException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MessageStoreTest {
    private static final String PATH = "/tmp/test_message_store/";

    private static KVService kvService;
    private static MessageStore messageStore;

    @BeforeEach
    public void setUp() throws RocksDBException {
        kvService = new RocksDBKVService(PATH);
        messageStore = new MessageStoreImpl(null, new MockOperationLogService(), kvService);
    }

    @AfterEach
    public void tearDown() throws RocksDBException {
        kvService.destroy();
    }

    @Test
    void pop() throws RocksDBException, ExecutionException, InterruptedException {
        long testStartTime = System.nanoTime();
        PopResult popResult = messageStore.pop(1, 1, 1, 0, 1, false, 100).get();
        assertEquals(0, popResult.status());
        assertFalse(popResult.messageList().isEmpty());

        Message message = popResult.messageList().get(0);
        byte[] bytes = kvService.get(MessageStoreImpl.KV_PARTITION_CHECK_POINT, SerializeUtil.buildCheckPointKey(1, 1, 0, popResult.operationId()));
        assertNotNull(bytes);

        CheckPoint checkPoint = CheckPoint.getRootAsCheckPoint(ByteBuffer.wrap(bytes));
        assertTrue(testStartTime < popResult.deliveryTimestamp());
        assertEquals(popResult.deliveryTimestamp(), checkPoint.deliveryTimestamp());
        assertEquals(popResult.deliveryTimestamp() + 100, checkPoint.nextVisibleTimestamp());
        assertEquals(1, checkPoint.consumerGroupId());
        assertEquals(1, checkPoint.topicId());
        assertEquals(1, checkPoint.queueId());
        assertEquals(message.offset(), checkPoint.messgeOffset());

        messageStore.pop(1, 1, 2, 0, 1, false, 100).join();

        List<CheckPoint> allCheckPointList = new ArrayList<>();
        kvService.iterate(MessageStoreImpl.KV_PARTITION_CHECK_POINT, (key, value) ->
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
    void popOrderly() throws RocksDBException, ExecutionException, InterruptedException {
        PopResult popResult = messageStore.pop(1, 1, 1, 0, 1, true, 100).get();
        assertEquals(0, popResult.status());
        assertFalse(popResult.messageList().isEmpty());

        Message message = popResult.messageList().get(0);
        byte[] bytes = kvService.get(MessageStoreImpl.KV_PARTITION_CHECK_POINT, SerializeUtil.buildCheckPointKey(1, 1, message.offset(), popResult.operationId()));
        assertNotNull(bytes);

        CheckPoint checkPoint = CheckPoint.getRootAsCheckPoint(ByteBuffer.wrap(bytes));
        assertTrue(checkPoint.isOrder());
        assertEquals(popResult.operationId(), checkPoint.operationId());
        assertEquals(popResult.deliveryTimestamp(), checkPoint.deliveryTimestamp());
        assertEquals(popResult.deliveryTimestamp() + 100, checkPoint.nextVisibleTimestamp());
        assertEquals(0, checkPoint.reconsumeCount());

        bytes = kvService.get(MessageStoreImpl.KV_PARTITION_ORDER_INDEX, SerializeUtil.buildOrderIndexKey(1, 1, 1, message.offset()));
        assertNotNull(bytes);

        assertEquals(popResult.operationId(), ByteBuffer.wrap(bytes).getLong());

        // Pop the same message again.
        popResult = messageStore.pop(1, 1, 1, 0, 1, true, 100).join();

        message = popResult.messageList().get(0);
        bytes = kvService.get(MessageStoreImpl.KV_PARTITION_CHECK_POINT, SerializeUtil.buildCheckPointKey(1, 1, message.offset(), popResult.operationId()));
        assertNotNull(bytes);

        checkPoint = CheckPoint.getRootAsCheckPoint(ByteBuffer.wrap(bytes));
        assertTrue(checkPoint.isOrder());
        assertEquals(popResult.operationId(), checkPoint.operationId());
        assertEquals(popResult.deliveryTimestamp(), checkPoint.deliveryTimestamp());
        assertEquals(popResult.deliveryTimestamp() + 100, checkPoint.nextVisibleTimestamp());
        assertEquals(1, checkPoint.reconsumeCount());

        bytes = kvService.get(MessageStoreImpl.KV_PARTITION_ORDER_INDEX, SerializeUtil.buildOrderIndexKey(1, 1, 1, message.offset()));
        assertNotNull(bytes);

        assertEquals(popResult.operationId(), ByteBuffer.wrap(bytes).getLong());

        AtomicInteger checkPointCount = new AtomicInteger();
        kvService.iterate(MessageStoreImpl.KV_PARTITION_CHECK_POINT, (key, value) -> checkPointCount.getAndIncrement());
        assertEquals(1, checkPointCount.get());

        AtomicInteger orderIndexCount = new AtomicInteger();
        kvService.iterate(MessageStoreImpl.KV_PARTITION_ORDER_INDEX, (key, value) -> orderIndexCount.getAndIncrement());
        assertEquals(1, orderIndexCount.get());

        AtomicInteger timerTagCount = new AtomicInteger();
        kvService.iterate(MessageStoreImpl.KV_PARTITION_TIMER_TAG, (key, value) -> timerTagCount.getAndIncrement());
        assertEquals(1, timerTagCount.get());
    }

    @Test
    void ack() throws RocksDBException, ExecutionException, InterruptedException {
        PopResult popResult = messageStore.pop(1, 1, 1, 0, 1, true, 100).get();
        assertEquals(0, popResult.status());
        assertFalse(popResult.messageList().isEmpty());

        Message message = popResult.messageList().get(0);
        messageStore.ack(SerializeUtil.encodeReceiptHandle(1, 1, message.offset(), popResult.operationId())).join();

        AtomicInteger checkPointCount = new AtomicInteger();
        kvService.iterate(MessageStoreImpl.KV_PARTITION_CHECK_POINT, (key, value) -> checkPointCount.getAndIncrement());
        assertEquals(0, checkPointCount.get());

        AtomicInteger orderIndexCount = new AtomicInteger();
        kvService.iterate(MessageStoreImpl.KV_PARTITION_ORDER_INDEX, (key, value) -> orderIndexCount.getAndIncrement());
        assertEquals(0, orderIndexCount.get());

        AtomicInteger timerTagCount = new AtomicInteger();
        kvService.iterate(MessageStoreImpl.KV_PARTITION_TIMER_TAG, (key, value) -> timerTagCount.getAndIncrement());
        assertEquals(0, timerTagCount.get());
    }

    @Test
    void changeInvisibleDuration() throws RocksDBException, ExecutionException, InterruptedException {
        // Pop the message to generate the check point.
        PopResult popResult = messageStore.pop(1, 1, 1, 0, 1, false, 100).get();
        assertEquals(0, popResult.status());
        assertFalse(popResult.messageList().isEmpty());

        byte[] checkPointKey = SerializeUtil.buildCheckPointKey(1, 1, 0, popResult.operationId());
        byte[] bytes = kvService.get(MessageStoreImpl.KV_PARTITION_CHECK_POINT, checkPointKey);
        assertNotNull(bytes);

        CheckPoint checkPoint = CheckPoint.getRootAsCheckPoint(ByteBuffer.wrap(bytes));

        long popEndTimestamp = System.nanoTime();
        long lastVisibleTime = checkPoint.nextVisibleTimestamp();
        assertTrue(popEndTimestamp + 100 > checkPoint.nextVisibleTimestamp());

        // Change the invisible duration.
        Message message = popResult.messageList().get(0);
        String receiptHandle = SerializeUtil.encodeReceiptHandle(1, 1, message.offset(), popResult.operationId());
        messageStore.changeInvisibleDuration(receiptHandle, 100_000_000_000L).join();

        bytes = kvService.get(MessageStoreImpl.KV_PARTITION_CHECK_POINT, checkPointKey);
        assertNotNull(bytes);

        checkPoint = CheckPoint.getRootAsCheckPoint(ByteBuffer.wrap(bytes));

        assertTrue(lastVisibleTime < checkPoint.nextVisibleTimestamp());
        assertTrue(popEndTimestamp + 100 < checkPoint.nextVisibleTimestamp());

        AtomicInteger checkPointCount = new AtomicInteger();
        kvService.iterate(MessageStoreImpl.KV_PARTITION_CHECK_POINT, (key, value) -> checkPointCount.getAndIncrement());
        assertEquals(1, checkPointCount.get());

        AtomicInteger timerTagCount = new AtomicInteger();
        kvService.iterate(MessageStoreImpl.KV_PARTITION_TIMER_TAG, (key, value) -> timerTagCount.getAndIncrement());
        assertEquals(1, timerTagCount.get());

        // Change the invisible duration again.
        message = popResult.messageList().get(0);
        receiptHandle = SerializeUtil.encodeReceiptHandle(1, 1, message.offset(), popResult.operationId());
        messageStore.changeInvisibleDuration(receiptHandle, 0L).join();

        checkPointCount.set(0);
        kvService.iterate(MessageStoreImpl.KV_PARTITION_CHECK_POINT, (key, value) -> checkPointCount.getAndIncrement());
        assertEquals(1, checkPointCount.get());

        timerTagCount.set(0);
        kvService.iterate(MessageStoreImpl.KV_PARTITION_TIMER_TAG, (key, value) -> timerTagCount.getAndIncrement());
        assertEquals(1, timerTagCount.get());

        // Ack the message with the same receipt handle.
        messageStore.ack(receiptHandle).join();

        checkPointCount.set(0);
        kvService.iterate(MessageStoreImpl.KV_PARTITION_CHECK_POINT, (key, value) -> checkPointCount.getAndIncrement());
        assertEquals(0, checkPointCount.get());

        timerTagCount.set(0);
        kvService.iterate(MessageStoreImpl.KV_PARTITION_TIMER_TAG, (key, value) -> timerTagCount.getAndIncrement());
        assertEquals(0, timerTagCount.get());
    }
}