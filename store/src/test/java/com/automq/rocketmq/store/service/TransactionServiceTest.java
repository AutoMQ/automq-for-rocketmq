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

package com.automq.rocketmq.store.service;

import com.automq.rocketmq.common.config.StoreConfig;
import com.automq.rocketmq.common.model.generated.FlatMessage;
import com.automq.rocketmq.store.MessageStoreTest;
import com.automq.rocketmq.store.exception.StoreException;
import com.automq.rocketmq.store.mock.MockMessageUtil;
import com.automq.rocketmq.store.model.generated.TimerHandlerType;
import com.automq.rocketmq.store.service.api.KVService;
import com.google.common.testing.FakeTicker;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

class TransactionServiceTest {
    private static final String PATH = "/tmp/test_transaction_service/";
    private StoreConfig config;
    private FakeTicker ticker;
    private KVService kvService;
    private TimerService timerService;
    private TransactionService transactionService;

    @BeforeEach
    void setUp() throws StoreException {
        kvService = new RocksDBKVService(PATH);
        ticker = new FakeTicker();
        timerService = new TimerService(MessageStoreTest.KV_NAMESPACE_TIMER_TAG, kvService, ticker);
        config = new StoreConfig();
        transactionService = new TransactionService(new StoreConfig(), timerService);
    }

    @AfterEach
    public void tearDown() throws StoreException {
        kvService.destroy();
    }

    @Test
    void prepareTransaction() throws StoreException {
        ByteBuffer buffer = MockMessageUtil.buildMessage();
        FlatMessage message = FlatMessage.getRootAsFlatMessage(buffer);

        AtomicInteger counter = new AtomicInteger(0);
        timerService.registerHandler(TimerHandlerType.TRANSACTION_MESSAGE, (timerTag) -> {
            assertEquals(buffer.remaining(), timerTag.payloadLength());
            counter.incrementAndGet();
        });

        String transactionId = transactionService.prepareTransaction(message);
        assertEquals(message.systemProperties().messageId(), transactionId);

        long dequeueTimestamp = System.currentTimeMillis() + config.transactionTimeoutMillis();
        ticker.advance(dequeueTimestamp);
        timerService.dequeue();
        assertEquals(1, counter.get());
    }

    @Test
    void cancelCheck() throws StoreException {
        timerService.registerHandler(TimerHandlerType.TRANSACTION_MESSAGE, (timerTag) -> fail("The transaction check should be canceled"));

        ByteBuffer buffer = MockMessageUtil.buildMessage();
        FlatMessage message = FlatMessage.getRootAsFlatMessage(buffer);

        String transactionId = transactionService.prepareTransaction(message);
        transactionService.cancelCheck(transactionId);

        ticker.advance(Long.MAX_VALUE);
        timerService.dequeue();
    }
}