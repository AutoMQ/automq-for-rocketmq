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

import com.automq.rocketmq.store.service.KVService;
import com.automq.rocketmq.store.service.RocksDBKVService;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.rocksdb.RocksDBException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class KVServiceTest {
    private static final String PATH = "/tmp/test_kv_service/";
    private static final String PARTITION = "rocketmq";

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
    public void testMutation() throws IOException, RocksDBException {
        String key = "Hello world";
        String value = "Hello RocketMQ";

        String path = new File(PATH + UUID.randomUUID()).getCanonicalPath();
        cleanUp(path);
        KVService store = new RocksDBKVService(path);
        assertNotNull(store);
        store.put(PARTITION, key.getBytes(KVService.CHARSET), value.getBytes(KVService.CHARSET));
        store.flush(true);

        final Map<String, String> queryResult = new HashMap<>();
        store.iterate(PARTITION, (key1, value1) ->
            queryResult.put(new String(key1, KVService.CHARSET), new String(value1, KVService.CHARSET)));

        assertEquals(1, queryResult.size());
        assertEquals(key, queryResult.keySet().iterator().next());
        assertEquals(value, queryResult.values().iterator().next());

        byte[] valueFound = store.get(PARTITION, key.getBytes(KVService.CHARSET));
        assertNotNull(valueFound);
        assertEquals(value, new String(valueFound, KVService.CHARSET));

        store.delete(PARTITION, key.getBytes(KVService.CHARSET));
        byte[] valueNotFound = store.get(PARTITION, key.getBytes(KVService.CHARSET));
        assertNull(valueNotFound);

        store.close();
        store.destroy();
        assertFalse(new File(path).exists());

        assertThrowsExactly(RocksDBException.class, () -> store.get(PARTITION, key.getBytes()));
        assertThrowsExactly(RocksDBException.class, () -> store.iterate(PARTITION, (_key, _value) -> {
        }));
        assertThrowsExactly(RocksDBException.class, () -> store.iterate(PARTITION, key, key, key, (_key, _value) -> {
        }));
        assertThrowsExactly(RocksDBException.class, () -> store.put(PARTITION, key.getBytes(), key.getBytes()));
        assertThrowsExactly(RocksDBException.class, () -> store.delete(PARTITION, key.getBytes()));
        assertThrowsExactly(RocksDBException.class, () -> store.flush(true));
    }

    @Test
    public void testIterate() throws IOException, RocksDBException {
        String path = new File(PATH + UUID.randomUUID()).getCanonicalPath();
        cleanUp(path);
        KVService store = new RocksDBKVService(path);
        assertNotNull(store);

        assertThrowsExactly(RocksDBException.class, () -> store.iterate(PARTITION, null));
        assertThrowsExactly(RocksDBException.class, () -> store.iterate(PARTITION, null, null, null, null));
        assertThrowsExactly(RocksDBException.class, () -> store.iterate(PARTITION, null, "start", null, (key, value) -> {
        }));
        assertThrowsExactly(RocksDBException.class, () -> store.iterate(PARTITION, null, null, "end", (key, value) -> {
        }));

        String prefix1 = "/1/";
        store.put(PARTITION, (prefix1 + "0").getBytes(KVService.CHARSET), "0".getBytes(KVService.CHARSET));
        store.put(PARTITION, (prefix1 + "1").getBytes(KVService.CHARSET), "1".getBytes(KVService.CHARSET));

        String prefix2 = "/2/";
        store.put(PARTITION, (prefix2 + "2").getBytes(KVService.CHARSET), "2".getBytes(KVService.CHARSET));
        store.put(PARTITION, (prefix2 + "3").getBytes(KVService.CHARSET), "3".getBytes(KVService.CHARSET));
        store.put(PARTITION, (prefix2 + "4").getBytes(KVService.CHARSET), "4".getBytes(KVService.CHARSET));

        String prefix3 = "/3/";
        store.put(PARTITION, (prefix3 + "5").getBytes(KVService.CHARSET), "5".getBytes(KVService.CHARSET));
        store.put(PARTITION, (prefix3 + "6").getBytes(KVService.CHARSET), "6".getBytes(KVService.CHARSET));

        store.flush(true);

        AtomicInteger num = new AtomicInteger();

        store.iterate(PARTITION, (key, value) -> {
            String valueStr = new String(value, KVService.CHARSET);
            String target = String.valueOf(num.get());

            assertEquals(target, valueStr);
            num.getAndIncrement();
        });
        assertEquals(7, num.get());

        num.set(0);
        store.iterate(PARTITION, prefix1, null, null, (key, value) -> {
            String valueStr = new String(value, KVService.CHARSET);
            String target = String.valueOf(num.get());

            assertEquals(target, valueStr);
            num.getAndIncrement();
        });
        assertEquals(2, num.get());

        num.set(0);
        store.iterate(PARTITION, null, prefix2 + "1", prefix2 + "5", (key, value) -> {
            String valueStr = new String(value, KVService.CHARSET);
            String target = String.valueOf(num.get() + 2);

            assertEquals(target, valueStr);
            num.getAndIncrement();
        });
        assertEquals(3, num.get());

        store.close();
    }
}

