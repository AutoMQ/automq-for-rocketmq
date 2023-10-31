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

import com.automq.rocketmq.store.MessageStoreTest;
import com.automq.rocketmq.store.exception.StoreException;
import com.automq.rocketmq.store.model.generated.TimerHandlerType;
import com.automq.rocketmq.store.service.api.KVService;
import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TimerServiceTest {
    private static final String PATH = "/tmp/test_timer_service";

    private KVService kvService;
    private TimerService timerService;

    @BeforeEach
    void setUp() throws StoreException {
        kvService = new RocksDBKVService(PATH);
        timerService = new TimerService(MessageStoreTest.KV_NAMESPACE_TIMER_TAG, kvService);
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
        long deliveryTimestamp = System.currentTimeMillis() + 10;
        AtomicInteger counter = new AtomicInteger(0);
        timerService.registerHandler(TimerHandlerType.TIMER_MESSAGE, (timerTag) -> {
            assertTrue(timerTag.deliveryTimestamp() <= System.currentTimeMillis());
            assertEquals(TimerHandlerType.TIMER_MESSAGE, timerTag.handlerType());

            ByteBuffer payload = timerTag.payloadAsByteBuffer();
            byte[] bytes = new byte[payload.remaining()];
            payload.get(bytes);
            assertArrayEquals("payload".getBytes(), bytes);
            counter.incrementAndGet();
        });

        timerService.enqueue(deliveryTimestamp, "identity".getBytes(), TimerHandlerType.TIMER_MESSAGE, "payload".getBytes());

        await().atMost(1, TimeUnit.SECONDS)
            .until(() -> {
                timerService.dequeue();
                return counter.get() == 1;
            });
    }

    @Test
    void enqueue_cancel() throws StoreException, InterruptedException {
        long deliveryTimestamp = System.currentTimeMillis() + 50;
        AtomicInteger counter = new AtomicInteger(0);
        timerService.registerHandler(TimerHandlerType.TIMER_MESSAGE, (timerTag) -> {
            assertTrue(timerTag.deliveryTimestamp() <= System.currentTimeMillis());
            counter.incrementAndGet();
        });

        timerService.enqueue(deliveryTimestamp, "identity".getBytes(), TimerHandlerType.TIMER_MESSAGE, "payload".getBytes());
        timerService.cancel(deliveryTimestamp, "identity".getBytes());

        Thread.sleep(100);
        timerService.dequeue();
        assertEquals(0, counter.get());
    }
}