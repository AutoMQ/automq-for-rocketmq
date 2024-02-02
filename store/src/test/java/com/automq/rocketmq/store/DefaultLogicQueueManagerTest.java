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

package com.automq.rocketmq.store;

import com.automq.rocketmq.common.config.StoreConfig;
import com.automq.rocketmq.metadata.api.StoreMetadataService;
import com.automq.rocketmq.store.api.LogicQueue;
import com.automq.rocketmq.store.api.StreamStore;
import com.automq.rocketmq.store.exception.StoreException;
import com.automq.rocketmq.store.mock.MockOperationLogService;
import com.automq.rocketmq.store.mock.MockStoreMetadataService;
import com.automq.rocketmq.store.mock.MockStreamStore;
import com.automq.rocketmq.store.model.StoreContext;
import com.automq.rocketmq.store.queue.DefaultLogicQueueManager;
import com.automq.rocketmq.store.service.InflightService;
import com.automq.rocketmq.store.service.RocksDBKVService;
import com.automq.rocketmq.store.service.StreamReclaimService;
import com.automq.rocketmq.store.service.TimerService;
import com.automq.rocketmq.store.service.api.KVService;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DefaultLogicQueueManagerTest {
    private static final String PATH = "/tmp/ros/topic_queue_manager_test/";
    private static final long TOPIC_ID = 0;
    private static final int QUEUE_ID = 1;

    private KVService kvService;
    private StoreMetadataService metadataService;
    private StreamStore streamStore;
    private MockOperationLogService operationLogService;
    private DefaultLogicQueueManager topicQueueManager;
    private StreamReclaimService streamReclaimService;

    @BeforeEach
    void setUp() throws StoreException {
        kvService = new RocksDBKVService(PATH);
        metadataService = new MockStoreMetadataService();
        streamStore = new MockStreamStore();
        InflightService inflightService = new InflightService();
        operationLogService = new MockOperationLogService();
        streamReclaimService = new StreamReclaimService(streamStore);
        TimerService timerService = new TimerService(MessageStoreTest.KV_NAMESPACE_TIMER_TAG, kvService);
        topicQueueManager = new DefaultLogicQueueManager(new StoreConfig(), streamStore, kvService, timerService, metadataService, operationLogService, inflightService, streamReclaimService);
    }

    @AfterEach
    public void tearDown() throws StoreException {
        kvService.destroy();
    }

    @Test
    void getOrCreate() {
        // Create new TopicQueue
        CompletableFuture<LogicQueue> future = topicQueueManager.getOrCreate(StoreContext.EMPTY, TOPIC_ID, QUEUE_ID);
        assertEquals(topicQueueManager.size(), 1);
        assertTrue(future.isDone());

        // Get existing TopicQueue
        future = topicQueueManager.getOrCreate(StoreContext.EMPTY, TOPIC_ID, QUEUE_ID);
        assertEquals(topicQueueManager.size(), 1);
        assertTrue(future.isDone());
        LogicQueue logicQueue = future.join();
        assertEquals(TOPIC_ID, logicQueue.topicId());
        assertEquals(QUEUE_ID, logicQueue.queueId());

        Optional<LogicQueue> optionalLogicQueue = topicQueueManager.get(TOPIC_ID, QUEUE_ID).join();
        assertTrue(optionalLogicQueue.isPresent());
        assertEquals(logicQueue, optionalLogicQueue.get());
    }

    @Test
    void getOrCreate_exception() {
        operationLogService.setRecoverFailed(true);

        CompletableFuture<LogicQueue> future = topicQueueManager.getOrCreate(StoreContext.EMPTY, TOPIC_ID, QUEUE_ID);
        assertTrue(future.isDone());
        assertEquals(topicQueueManager.size(), 0);
    }

    @Test
    void close() {
    }
}