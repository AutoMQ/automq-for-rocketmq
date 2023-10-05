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

import apache.rocketmq.controller.v1.StreamMetadata;
import com.automq.rocketmq.common.config.StoreConfig;
import com.automq.rocketmq.metadata.api.StoreMetadataService;
import com.automq.rocketmq.store.api.MessageStateMachine;
import com.automq.rocketmq.store.api.StreamStore;
import com.automq.rocketmq.store.api.TopicQueue;
import com.automq.rocketmq.store.api.TopicQueueManager;
import com.automq.rocketmq.store.service.InflightService;
import com.automq.rocketmq.store.service.SnapshotService;
import com.automq.rocketmq.store.service.api.KVService;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

public class DefaultTopicQueueManager implements TopicQueueManager {

    private final StoreConfig storeConfig;
    private final StoreMetadataService metadataService;
    private final StreamStore streamStore;
    private final InflightService inflightService;
    private final SnapshotService snapshotService;
    private final KVService kvService;
    private final Map<TopicQueueId, TopicQueue> topicQueueMap;

    public DefaultTopicQueueManager(StoreConfig storeConfig, StoreMetadataService metadataService,
        StreamStore streamStore, InflightService inflightService, SnapshotService snapshotService,
        KVService kvService) {
        this.storeConfig = storeConfig;
        this.metadataService = metadataService;
        this.streamStore = streamStore;
        this.inflightService = inflightService;
        this.snapshotService = snapshotService;
        this.topicQueueMap = new ConcurrentHashMap<>();
        this.kvService = kvService;
    }

    @Override
    public void start() throws Exception {

    }

    @Override
    public void shutdown() throws Exception {

    }

    @Override
    public TopicQueue get(long topicId, int queueId) {
        return topicQueueMap.get(new TopicQueueId(topicId, queueId));
    }

    @Override
    public CompletableFuture<Void> onTopicQueueOpen(long topicId, int queueId, long epoch) {
        TopicQueueId id = new TopicQueueId(topicId, queueId);
        if (topicQueueMap.containsKey(id)) {
            return CompletableFuture.completedFuture(null);
        }
        CompletableFuture<StreamMetadata> dataStreamCf = metadataService.dataStreamOf(topicId, queueId);
        CompletableFuture<StreamMetadata> opStreamCf = metadataService.operationStreamOf(topicId, queueId);
        CompletableFuture<StreamMetadata> snapshotStreamCf = metadataService.snapshotStreamOf(topicId, queueId);
        return CompletableFuture.allOf(
            dataStreamCf, opStreamCf, snapshotStreamCf
        ).thenApply(nil -> {
            StreamMetadata dataStream = dataStreamCf.join();
            StreamMetadata opStream = opStreamCf.join();
            StreamMetadata snapshotStream = snapshotStreamCf.join();

            MessageStateMachine stateMachine = new DefaultMessageStateMachine(topicId, queueId, kvService);
            StreamTopicQueue queue = new StreamTopicQueue(storeConfig, topicId, queueId, epoch,
                dataStream.getStreamId(), opStream.getStreamId(), snapshotStream.getStreamId(),
                metadataService, stateMachine, streamStore, inflightService, snapshotService);
            return queue;
        }).thenCompose(queue -> {
            return queue.open()
                .thenAccept(nil -> {
                    topicQueueMap.putIfAbsent(id, queue);
                });
        });
    }

    @Override
    public CompletableFuture<Void> onTopicQueueClose(long topicId, int queueId, long epoch) {
        TopicQueueId id = new TopicQueueId(topicId, queueId);
        TopicQueue queue = topicQueueMap.remove(id);
        if (queue == null) {
            return CompletableFuture.completedFuture(null);
        }
        return queue.close();
    }

    static class TopicQueueId {
        private final long topicId;
        private final int queueId;

        public TopicQueueId(long topicId, int queueId) {
            this.topicId = topicId;
            this.queueId = queueId;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;
            TopicQueueId id = (TopicQueueId) o;
            return topicId == id.topicId && queueId == id.queueId;
        }

        @Override
        public int hashCode() {
            return Objects.hash(topicId, queueId);
        }
    }
}
