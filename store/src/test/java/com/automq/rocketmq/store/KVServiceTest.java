/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.automq.rocketmq.store;

import com.automq.rocketmq.store.exception.StoreException;
import com.automq.rocketmq.store.model.kv.BatchDeleteRequest;
import com.automq.rocketmq.store.model.kv.BatchWriteRequest;
import com.automq.rocketmq.store.service.KVService;
import com.automq.rocketmq.store.service.impl.RocksDBKVService;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class KVServiceTest {
    private static final String PATH = "/tmp/test_kv_service/";
    private static final String NAMESPACE = "rocketmq";

    @AfterAll
    public static void cleanUp() {
        cleanUp(PATH);
    }

    public static void cleanUp(String path) {
        File kvStore = new File(path);
        if (!kvStore.exists()) {
            return;
        }
        for (File dir : Objects.requireNonNull(kvStore.listFiles())) {
            for (File file : Objects.requireNonNull(dir.listFiles())) {
                assertTrue(file.isFile());
                assertTrue(file.delete());
            }
            assertTrue(dir.delete());
        }
        assertTrue(kvStore.delete());
    }

    @Test
    public void testMutation() throws IOException, StoreException {
        String key = "Hello world";
        String value = "Hello RocketMQ";

        String path = new File(PATH + UUID.randomUUID()).getCanonicalPath();
        cleanUp(path);
        KVService store = new RocksDBKVService(path);
        assertNotNull(store);
        store.put(NAMESPACE, key.getBytes(), value.getBytes());
        store.flush(true);

        final Map<String, String> queryResult = new HashMap<>();
        store.iterate(NAMESPACE, (key1, value1) ->
            queryResult.put(new String(key1), new String(value1)));

        assertEquals(1, queryResult.size());
        assertEquals(key, queryResult.keySet().iterator().next());
        assertEquals(value, queryResult.values().iterator().next());

        byte[] valueFound = store.get(NAMESPACE, key.getBytes());
        assertNotNull(valueFound);
        assertEquals(value, new String(valueFound));

        store.delete(NAMESPACE, key.getBytes());
        byte[] valueNotFound = store.get(NAMESPACE, key.getBytes());
        assertNull(valueNotFound);

        store.destroy();
        assertFalse(new File(path).exists());

        assertThrowsExactly(StoreException.class, () -> store.get(NAMESPACE, key.getBytes()));
        assertThrowsExactly(StoreException.class, () -> store.iterate(NAMESPACE, (_key, _value) -> {
        }));
        assertThrowsExactly(StoreException.class, () -> store.iterate(NAMESPACE, key.getBytes(), key.getBytes(), key.getBytes(), (_key, _value) -> {
        }));
        assertThrowsExactly(StoreException.class, () -> store.put(NAMESPACE, key.getBytes(), key.getBytes()));
        assertThrowsExactly(StoreException.class, () -> store.delete(NAMESPACE, key.getBytes()));
        assertThrowsExactly(StoreException.class, () -> store.flush(true));
    }

    @Test
    public void testIterate() throws IOException, StoreException {
        String path = new File(PATH + UUID.randomUUID()).getCanonicalPath();
        cleanUp(path);
        KVService store = new RocksDBKVService(path);
        assertNotNull(store);

        assertThrowsExactly(StoreException.class, () -> store.iterate(NAMESPACE, null));
        assertThrowsExactly(StoreException.class, () -> store.iterate(NAMESPACE, null, null, null, null));
        assertThrowsExactly(StoreException.class, () -> store.iterate(NAMESPACE, null, "start".getBytes(), null, (key, value) -> {
        }));
        assertThrowsExactly(StoreException.class, () -> store.iterate(NAMESPACE, null, null, "end".getBytes(), (key, value) -> {
        }));

        String prefix1 = "/1/";
        store.put(NAMESPACE, (prefix1 + "0").getBytes(), "0".getBytes());
        store.put(NAMESPACE, (prefix1 + "1").getBytes(), "1".getBytes());

        String prefix2 = "/2/";
        store.put(NAMESPACE, (prefix2 + "2").getBytes(), "2".getBytes());
        store.put(NAMESPACE, (prefix2 + "3").getBytes(), "3".getBytes());
        store.put(NAMESPACE, (prefix2 + "4").getBytes(), "4".getBytes());

        String prefix3 = "/3/";
        store.put(NAMESPACE, (prefix3 + "5").getBytes(), "5".getBytes());
        store.put(NAMESPACE, (prefix3 + "6").getBytes(), "6".getBytes());

        store.flush(true);

        AtomicInteger num = new AtomicInteger();

        store.iterate(NAMESPACE, (key, value) -> {
            String valueStr = new String(value);
            String target = String.valueOf(num.get());

            assertEquals(target, valueStr);
            num.getAndIncrement();
        });
        assertEquals(7, num.get());

        num.set(0);
        store.iterate(NAMESPACE, prefix1.getBytes(), null, null, (key, value) -> {
            String valueStr = new String(value);
            String target = String.valueOf(num.get());

            assertEquals(target, valueStr);
            num.getAndIncrement();
        });
        assertEquals(2, num.get());

        num.set(0);
        store.iterate(NAMESPACE, null, (prefix2 + "1").getBytes(), (prefix2 + "5").getBytes(), (key, value) -> {
            String valueStr = new String(value);
            String target = String.valueOf(num.get() + 2);

            assertEquals(target, valueStr);
            num.getAndIncrement();
        });
        assertEquals(3, num.get());

        store.destroy();
    }

    @Test
    public void testBatch() throws IOException, StoreException {
        String path = new File(PATH + UUID.randomUUID()).getCanonicalPath();
        cleanUp(path);
        KVService store = new RocksDBKVService(path);
        assertNotNull(store);

        assertThrowsExactly(StoreException.class, () -> store.batch(null));
        store.batch(new BatchWriteRequest(NAMESPACE, "0".getBytes(), "0".getBytes()), new BatchWriteRequest(NAMESPACE, "1".getBytes(), "1".getBytes()));

        AtomicInteger num = new AtomicInteger();
        store.iterate(NAMESPACE, (key, value) -> {
            String valueStr = new String(value);
            String target = String.valueOf(num.getAndIncrement());

            assertEquals(target, valueStr);
        });
        assertEquals(2, num.get());

        store.batch(new BatchDeleteRequest(NAMESPACE, "0".getBytes()), new BatchWriteRequest(NAMESPACE, "2".getBytes(), "2".getBytes()));

        num.set(1);
        store.iterate(NAMESPACE, (key, value) -> {
            String valueStr = new String(value);
            String target = String.valueOf(num.getAndIncrement());

            assertEquals(target, valueStr);
        });
        assertEquals(3, num.get());

        store.batch(new BatchDeleteRequest(NAMESPACE, "1".getBytes()), new BatchDeleteRequest(NAMESPACE, "2".getBytes()));
        num.set(0);
        store.iterate(NAMESPACE, (key, value) -> num.getAndIncrement());
        assertEquals(0, num.get());
    }
}

