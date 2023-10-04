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

import com.automq.rocketmq.common.config.S3StreamConfig;
import com.automq.rocketmq.common.config.StoreConfig;
import com.automq.rocketmq.metadata.api.StoreMetadataService;
import com.automq.rocketmq.store.api.MessageStateMachine;
import com.automq.rocketmq.store.api.MessageStore;
import com.automq.rocketmq.store.api.StreamStore;
import com.automq.rocketmq.store.api.TopicQueue;
import com.automq.rocketmq.store.api.TopicQueueManager;
import com.automq.rocketmq.store.exception.StoreException;
import com.automq.rocketmq.store.service.InflightService;
import com.automq.rocketmq.store.service.RocksDBKVService;
import com.automq.rocketmq.store.service.SnapshotService;
import com.automq.rocketmq.store.service.api.KVService;
import java.util.concurrent.ConcurrentHashMap;

public class MessageStoreBuilder {
    public static MessageStore build(StoreConfig storeConfig, S3StreamConfig s3StreamConfig,
        StoreMetadataService metadataService) throws StoreException {
        StreamStore streamStore = new S3StreamStore(s3StreamConfig, metadataService);
        KVService kvService = new RocksDBKVService(storeConfig.kvPath());
        InflightService inflightService = new InflightService();
        SnapshotService snapshotService = new SnapshotService(streamStore, kvService);
        TopicQueueManager topicQueueManager = new TopicQueueManager() {
            private final ConcurrentHashMap<String, TopicQueue> topicQueueMap = new ConcurrentHashMap<>();
            @Override
            public TopicQueue get(long topicId, int queueId) {
                String key = topicId + "_" + queueId;
                return topicQueueMap.computeIfAbsent(key, k -> {
                    MessageStateMachine stateMachine = new DefaultMessageStateMachine(topicId, queueId, kvService);
                    StreamTopicQueue queue = new StreamTopicQueue(storeConfig, topicId, queueId, metadataService, stateMachine, streamStore, inflightService, snapshotService);
                    queue.open().join();
                    return queue;
                });
            }

            @Override
            public void start() throws Exception {
                topicQueueMap.values().forEach(tq -> {
                    try {
                        tq.open().join();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                });
            }

            @Override
            public void shutdown() throws Exception {
                topicQueueMap.values().forEach(tq -> {
                    try {
                        tq.close();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                });
            }
        };

        return new MessageStoreImpl(storeConfig, streamStore, metadataService, kvService, inflightService, topicQueueManager, snapshotService);
    }
}
