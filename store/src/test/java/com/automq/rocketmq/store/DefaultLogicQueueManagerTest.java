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

package com.automq.rocketmq.store;

import com.automq.rocketmq.common.config.StoreConfig;
import com.automq.rocketmq.metadata.api.StoreMetadataService;
import com.automq.rocketmq.store.api.LogicQueue;
import com.automq.rocketmq.store.api.StreamStore;
import com.automq.rocketmq.store.exception.StoreException;
import com.automq.rocketmq.store.mock.MockOperationLogService;
import com.automq.rocketmq.store.mock.MockStoreMetadataService;
import com.automq.rocketmq.store.mock.MockStreamStore;
import com.automq.rocketmq.store.queue.DefaultLogicQueueManager;
import com.automq.rocketmq.store.service.InflightService;
import com.automq.rocketmq.store.service.RocksDBKVService;
import com.automq.rocketmq.store.service.api.KVService;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DefaultLogicQueueManagerTest {
    private static final String PATH = "/tmp/ros/topic_queue_manager_test/";
    private static final long TOPIC_ID = 0;
    private static final int QUEUE_ID = 1;

    private static KVService kvService;
    private static StoreMetadataService metadataService;
    private static StreamStore streamStore;
    private static InflightService inflightService;
    private static MockOperationLogService operationLogService;
    private DefaultLogicQueueManager topicQueueManager;

    @BeforeEach
    void setUp() throws StoreException {
        kvService = new RocksDBKVService(PATH);
        metadataService = new MockStoreMetadataService();
        streamStore = new MockStreamStore();
        inflightService = new InflightService();
        operationLogService = new MockOperationLogService();
        topicQueueManager = new DefaultLogicQueueManager(new StoreConfig(), streamStore, kvService, metadataService, operationLogService, inflightService);
    }

    @AfterEach
    public void tearDown() throws StoreException {
        kvService.destroy();
    }

    @Test
    void getOrCreate() {
        // Create new TopicQueue
        CompletableFuture<Optional<LogicQueue>> future = topicQueueManager.getOrCreate(TOPIC_ID, QUEUE_ID);
        assertEquals(topicQueueManager.size(), 1);
        assertTrue(future.isDone());
        assertTrue(future.join().isPresent());

        // Get existing TopicQueue
        future = topicQueueManager.getOrCreate(TOPIC_ID, QUEUE_ID);
        assertEquals(topicQueueManager.size(), 1);
        assertTrue(future.isDone());
        LogicQueue logicQueue = future.join().get();
        assertEquals(TOPIC_ID, logicQueue.topicId());
        assertEquals(QUEUE_ID, logicQueue.queueId());
    }

    @Test
    void getOrCreate_exception() {
        operationLogService.setRecoverFailed(true);

        CompletableFuture<Optional<LogicQueue>> future = topicQueueManager.getOrCreate(TOPIC_ID, QUEUE_ID);
        assertTrue(future.isDone());
        assertFalse(future.join().isPresent());
        assertEquals(topicQueueManager.size(), 0);
    }

    @Test
    void close() {
    }
}