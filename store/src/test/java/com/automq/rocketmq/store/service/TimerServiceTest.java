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

import com.automq.rocketmq.store.MessageStoreTest;
import com.automq.rocketmq.store.exception.StoreException;
import com.automq.rocketmq.store.model.generated.TimerHandlerType;
import com.automq.rocketmq.store.service.api.KVService;
import com.google.common.testing.FakeTicker;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

class TimerServiceTest {
    private static final String PATH = "/tmp/test_timer_service";

    private KVService kvService;
    private TimerService timerService;
    private FakeTicker ticker;

    @BeforeEach
    void setUp() throws StoreException {
        kvService = new RocksDBKVService(PATH);
        ticker = new FakeTicker();
        timerService = new TimerService(MessageStoreTest.KV_NAMESPACE_TIMER_TAG, kvService, ticker);
    }

    @AfterEach
    public void tearDown() throws StoreException {
        kvService.destroy();
    }

    @Test
    void register() throws StoreException {
        short undefined = -1;
        assertThrowsExactly(StoreException.class, () -> timerService.registerHandler(undefined, (timerTag) -> {
        }));

        timerService.registerHandler(TimerHandlerType.TIMER_MESSAGE, (timerTag) -> {
        });
        assertTrue(timerService.hasHandler(TimerHandlerType.TIMER_MESSAGE));

        timerService.unregisterHandler(TimerHandlerType.TIMER_MESSAGE);
        assertFalse(timerService.hasHandler(TimerHandlerType.TIMER_MESSAGE));
    }

    @Test
    void enqueue_exception() throws StoreException {
        long deliveryTimestamp = System.currentTimeMillis() - 1;
        timerService.registerHandler(TimerHandlerType.TIMER_MESSAGE, (timerTag) -> assertTrue(timerTag.deliveryTimestamp() <= System.currentTimeMillis()));

        short undefined = -1;
        assertThrowsExactly(StoreException.class, () -> timerService.enqueue(deliveryTimestamp, "identity".getBytes(), undefined, "payload".getBytes()));
        assertThrowsExactly(StoreException.class, () -> timerService.enqueue(deliveryTimestamp, "identity".getBytes(), TimerHandlerType.POP_REVIVE, "payload".getBytes()));

        timerService.unregisterHandler(TimerHandlerType.TIMER_MESSAGE);
        assertThrowsExactly(StoreException.class, () -> timerService.enqueue(deliveryTimestamp, "identity".getBytes(), TimerHandlerType.TIMER_MESSAGE, "payload".getBytes()));
    }

    @Test
    void enqueue_dequeue() throws StoreException {
        long deliveryTimestamp = System.currentTimeMillis() + 1000;
        AtomicInteger counter = new AtomicInteger(0);
        timerService.registerHandler(TimerHandlerType.TIMER_MESSAGE, (timerTag) -> {
            assertTrue(timerTag.deliveryTimestamp() <= ticker.read());
            assertEquals(TimerHandlerType.TIMER_MESSAGE, timerTag.handlerType());

            ByteBuffer payload = timerTag.payloadAsByteBuffer();
            byte[] bytes = new byte[payload.remaining()];
            payload.get(bytes);
            assertArrayEquals("payload".getBytes(), bytes);
            counter.incrementAndGet();
        });

        timerService.enqueue(deliveryTimestamp, "identity".getBytes(), TimerHandlerType.TIMER_MESSAGE, "payload".getBytes());

        byte[] timerTag = kvService.get(MessageStoreTest.KV_NAMESPACE_TIMER_TAG + "_tag", timerService.buildTimerTagKey(deliveryTimestamp, "identity".getBytes()));
        assertNotNull(timerTag);

        byte[] timerIndex = kvService.get(MessageStoreTest.KV_NAMESPACE_TIMER_TAG + "_index", "identity".getBytes());
        assertNotNull(timerIndex);
        assertEquals(deliveryTimestamp, ByteBuffer.wrap(timerIndex).getLong());

        ticker.advance(deliveryTimestamp);
        timerService.dequeue();
        assertEquals(1, counter.get());
    }

    @Test
    void enqueue_cancel() throws StoreException {
        timerService.registerHandler(TimerHandlerType.TIMER_MESSAGE, (timerTag) -> fail("The timer tag should be canceled"));

        long deliveryTimestamp = System.currentTimeMillis() + 1000;
        timerService.enqueue(deliveryTimestamp, "identity".getBytes(), TimerHandlerType.TIMER_MESSAGE, "payload".getBytes());
        timerService.cancel("identity".getBytes());

        ticker.advance(Long.MAX_VALUE);
        timerService.dequeue();
    }
}