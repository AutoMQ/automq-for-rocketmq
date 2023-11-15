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

import com.automq.rocketmq.store.api.StreamStore;
import com.automq.rocketmq.store.exception.StoreErrorCode;
import com.automq.rocketmq.store.exception.StoreException;
import com.automq.rocketmq.store.mock.MockStreamStore;
import com.automq.rocketmq.store.model.StoreContext;
import com.automq.rocketmq.store.model.operation.OperationSnapshot;
import com.automq.rocketmq.store.model.stream.SingleRecord;
import com.automq.rocketmq.store.service.api.KVService;
import java.io.File;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import org.apache.rocketmq.common.UtilAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class SnapshotServiceTest {
    private static final String PATH = "/tmp/ros/snapshot_service_test/";
    private static final long TOPIC_ID = 13;
    private static final int QUEUE_ID = 1313;
    private static final long OP_STREAM_ID = 131313;
    private static final long SNAPSHOT_STREAM_ID = 13131313;
    private KVService kvService;
    private StreamStore streamStore;
    private SnapshotService snapshotService;

    @BeforeEach
    public void setUp() throws Exception {
        kvService = new RocksDBKVService(PATH);
        streamStore = new MockStreamStore();
        snapshotService = Mockito.spy(new SnapshotService(streamStore, kvService));
        streamStore.start();
        snapshotService.start();
        streamStore.open(OP_STREAM_ID, 0);
        streamStore.open(SNAPSHOT_STREAM_ID, 0);
    }

    @AfterEach
    public void tearDown() throws Exception {
        snapshotService.shutdown();
        streamStore.shutdown();
        kvService.destroy();
        UtilAll.deleteFile(new File(PATH));
    }

    @Test
    public void test_take_snapshot() {
        // 1. append 100 operation records
        for (int i = 0; i < 100; i++) {
            streamStore.append(StoreContext.EMPTY, OP_STREAM_ID, buildRecord());
        }

        SnapshotService.SnapshotTask task = new SnapshotService.SnapshotTask(TOPIC_ID, QUEUE_ID, OP_STREAM_ID, SNAPSHOT_STREAM_ID,
            () -> new OperationSnapshot(88, 0, Collections.emptyList()));

        // 2. add task
        CompletableFuture<SnapshotService.TakeSnapshotResult> taskCf = snapshotService.addSnapshotTask(task);
        taskCf.whenComplete((v, e) -> {
            assertNull(e);
            assertTrue(v.success());
            assertEquals(89, v.newOpStartOffset());
        });
        taskCf.join();
        assertEquals(89, streamStore.startOffset(OP_STREAM_ID));
        assertEquals(100, streamStore.nextOffset(OP_STREAM_ID));
        assertEquals(0, streamStore.startOffset(SNAPSHOT_STREAM_ID));
        assertEquals(1, streamStore.nextOffset(SNAPSHOT_STREAM_ID));
    }

    @Test
    public void test_take_snapshot_fail() {
        // 1. append 100 operation records
        for (int i = 0; i < 100; i++) {
            streamStore.append(StoreContext.EMPTY, OP_STREAM_ID, buildRecord());
        }

        SnapshotService.SnapshotTask task = new SnapshotService.SnapshotTask(TOPIC_ID, QUEUE_ID, OP_STREAM_ID, SNAPSHOT_STREAM_ID,
            () -> {
                throw new StoreException(StoreErrorCode.ILLEGAL_ARGUMENT, "test");
            });

        // 2. add task
        CompletableFuture<SnapshotService.TakeSnapshotResult> taskCf = snapshotService.addSnapshotTask(task);
        try {
            taskCf.join();
        } catch (Exception e) {
            assertTrue(e instanceof CompletionException);
            assertTrue(e.getCause() instanceof StoreException);
        }
        assertTrue(taskCf.isCompletedExceptionally());
        assertEquals(0, streamStore.startOffset(OP_STREAM_ID));
        assertEquals(100, streamStore.nextOffset(OP_STREAM_ID));
        assertEquals(0, streamStore.startOffset(SNAPSHOT_STREAM_ID));
        assertEquals(0, streamStore.nextOffset(SNAPSHOT_STREAM_ID));
    }

    @Test
    public void test_abort_task() throws Exception {
        // 1. append 100 operation records
        for (int i = 0; i < 100; i++) {
            streamStore.append(StoreContext.EMPTY, OP_STREAM_ID, buildRecord());
        }
        CountDownLatch latch = new CountDownLatch(1);
        CountDownLatch latch1 = new CountDownLatch(1);
        SnapshotService.SnapshotTask task0 = new SnapshotService.SnapshotTask(TOPIC_ID, QUEUE_ID, OP_STREAM_ID, SNAPSHOT_STREAM_ID, () -> {
            try {
                latch1.countDown();
                latch.await();
            } catch (InterruptedException e) {
                Assertions.fail(e);
            }
            return new OperationSnapshot(88, 0, Collections.emptyList());
        });

        // 2. add task
        CompletableFuture<SnapshotService.TakeSnapshotResult> taskCf0 = snapshotService.addSnapshotTask(task0);

        // 3. add another task
        SnapshotService.SnapshotTask task1 = new SnapshotService.SnapshotTask(TOPIC_ID, QUEUE_ID, OP_STREAM_ID, SNAPSHOT_STREAM_ID,
            () -> new OperationSnapshot(99, 0, Collections.emptyList()));
        latch1.await();
        CompletableFuture<SnapshotService.TakeSnapshotResult> taskCf1 = snapshotService.addSnapshotTask(task1);
        // 4. shutdown snapshot service
        new Thread(() -> {
            try {
                snapshotService.shutdown();
            } catch (Exception e) {
                Assertions.fail(e);
            }
        }).start();
        Thread.sleep(10);
        latch.countDown();
        CompletableFuture.allOf(taskCf0, taskCf1).join();
        // 5. inflight task should be completed
        SnapshotService.TakeSnapshotResult result = taskCf0.join();
        assertTrue(result.success());
        assertEquals(89, result.newOpStartOffset());
        // 6. second task should be aborted
        SnapshotService.TakeSnapshotResult result1 = taskCf1.join();
        assertFalse(result1.success());
        assertEquals(89, streamStore.startOffset(OP_STREAM_ID));
        assertEquals(100, streamStore.nextOffset(OP_STREAM_ID));
        assertEquals(0, streamStore.startOffset(SNAPSHOT_STREAM_ID));
        assertEquals(1, streamStore.nextOffset(SNAPSHOT_STREAM_ID));
    }

    private SingleRecord buildRecord() {
        ByteBuffer buffer = ByteBuffer.allocate(4);
        buffer.putInt(13);
        buffer.flip();
        return new SingleRecord(buffer);
    }

}
