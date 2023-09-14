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
import com.automq.rocketmq.store.model.generated.CheckPoint;
import com.automq.rocketmq.store.model.message.PopResult;
import com.automq.rocketmq.store.service.KVService;
import com.automq.rocketmq.store.service.RocksDBKVService;
import java.nio.ByteBuffer;
import org.junit.jupiter.api.BeforeAll;
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

    @BeforeAll
    private static void setUp() throws RocksDBException {
        kvService = new RocksDBKVService(PATH);
        messageStore = new MessageStoreImpl(null, kvService);
    }

    @Test
    void pop() throws RocksDBException {
        long testStartTime = System.nanoTime();
        PopResult popResult = messageStore.pop(1, 1, 1, 1, 32, false, 100);
        assertEquals(0, popResult.status());
        assertFalse(popResult.messageList().isEmpty());

        for (Message message : popResult.messageList()) {
            byte[] bytes = kvService.get(MessageStoreImpl.KV_PARTITION_CHECK_POINT, MessageStoreImpl.buildCheckPointKey(1, 1, 1, message.offset()));
            assertNotNull(bytes);

            CheckPoint checkPoint = CheckPoint.getRootAsCheckPoint(ByteBuffer.wrap(bytes));
            assertTrue(testStartTime < popResult.deliveryTimestamp());
            assertEquals(popResult.deliveryTimestamp(), checkPoint.deliveryTimestamp());
            assertEquals(100, checkPoint.invisibleDuration());
            assertEquals(1, checkPoint.consumerGroupId());
            assertEquals(1, checkPoint.topicId());
            assertEquals(1, checkPoint.queueId());
            assertEquals(message.offset(), checkPoint.offset());
        }
    }
}