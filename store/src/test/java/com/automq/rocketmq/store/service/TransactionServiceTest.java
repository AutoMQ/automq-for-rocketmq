/*
 * Copyright 2024, AutoMQ CO.,LTD.
 *
 * Use of this software is governed by the Business Source License
 * included in the file BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
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

        String transactionId = transactionService.begin(message);
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

        String transactionId = transactionService.begin(message);
        transactionService.cancelCheck(transactionId);

        ticker.advance(Long.MAX_VALUE);
        timerService.dequeue();
    }
}