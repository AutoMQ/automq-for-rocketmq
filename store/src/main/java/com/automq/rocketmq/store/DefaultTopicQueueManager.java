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
import com.automq.rocketmq.store.api.MessageStateMachine;
import com.automq.rocketmq.store.api.StreamStore;
import com.automq.rocketmq.store.api.TopicQueue;
import com.automq.rocketmq.store.api.TopicQueueManager;
import com.automq.rocketmq.store.model.message.TopicQueueId;
import com.automq.rocketmq.store.service.InflightService;
import com.automq.rocketmq.store.service.api.KVService;
import com.automq.rocketmq.store.service.api.OperationLogService;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultTopicQueueManager implements TopicQueueManager {
    protected static final Logger LOGGER = LoggerFactory.getLogger(DefaultTopicQueueManager.class);

    private final StoreConfig storeConfig;
    private final StreamStore streamStore;
    private final KVService kvService;
    private final StoreMetadataService metadataService;
    private final OperationLogService operationLogService;
    private final InflightService inflightService;
    private final Map<TopicQueueId, CompletableFuture<TopicQueue>> topicQueueMap;

    public DefaultTopicQueueManager(StoreConfig storeConfig, StreamStore streamStore,
        KVService kvService, StoreMetadataService metadataService, OperationLogService operationLogService,
        InflightService inflightService) {
        this.storeConfig = storeConfig;
        this.streamStore = streamStore;
        this.kvService = kvService;
        this.metadataService = metadataService;
        this.operationLogService = operationLogService;
        this.inflightService = inflightService;
        this.topicQueueMap = new ConcurrentHashMap<>();
    }

    @Override
    public void start() throws Exception {

    }

    @Override
    public void shutdown() throws Exception {

    }

    public int size() {
        return topicQueueMap.size();
    }

    @Override
    public CompletableFuture<TopicQueue> getOrCreate(long topicId, int queueId) {
        TopicQueueId key = new TopicQueueId(topicId, queueId);
        CompletableFuture<TopicQueue> future = topicQueueMap.get(key);

        if (future != null) {
            return future;
        }

        // Prevent concurrent create.
        synchronized (this) {
            future = topicQueueMap.get(key);
            if (future != null) {
                return future;
            }

            // Create and open the topic queue.
            future = createAndOpen(topicId, queueId);

            // Put the future into the map to serve next request.
            topicQueueMap.put(key, future);
            future.exceptionally(ex -> {
                LOGGER.error("Create topic: {} queue: {} failed.", topicId, queueId, ex);
                topicQueueMap.remove(key);
                return null;
            });
            return future;
        }
    }

    @Override
    public CompletableFuture<Void> close(long topicId, int queueId) {
        LOGGER.info("Close topic: {} queue: {}", topicId, queueId);
        TopicQueueId key = new TopicQueueId(topicId, queueId);
        CompletableFuture<TopicQueue> future = topicQueueMap.remove(key);
        if (future != null) {
            return future.thenCompose(TopicQueue::close);
        }
        return CompletableFuture.completedFuture(null);
    }

    public CompletableFuture<TopicQueue> createAndOpen(long topicId, int queueId) {
        TopicQueueId id = new TopicQueueId(topicId, queueId);
        if (topicQueueMap.containsKey(id)) {
            return CompletableFuture.completedFuture(null);
        }

        MessageStateMachine stateMachine = new DefaultMessageStateMachine(topicId, queueId, kvService);
        TopicQueue topicQueue = new StreamTopicQueue(storeConfig, topicId, queueId,
            metadataService, stateMachine, streamStore, operationLogService, inflightService);

        // TODO: handle exception when open topic queue failed.
        LOGGER.info("Create and open topic: {} queue: {}", topicId, queueId);
        return topicQueue.open()
            .thenApply(nil -> topicQueue);
    }

}
