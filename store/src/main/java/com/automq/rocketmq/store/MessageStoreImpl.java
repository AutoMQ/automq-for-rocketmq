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
import com.automq.rocketmq.common.model.generated.FlatMessage;
import com.automq.rocketmq.metadata.api.StoreMetadataService;
import com.automq.rocketmq.store.api.MessageStore;
import com.automq.rocketmq.store.api.StreamStore;
import com.automq.rocketmq.store.api.TopicQueue;
import com.automq.rocketmq.store.api.TopicQueueManager;
import com.automq.rocketmq.store.model.generated.ReceiptHandle;
import com.automq.rocketmq.store.model.message.AckResult;
import com.automq.rocketmq.store.model.message.ChangeInvisibleDurationResult;
import com.automq.rocketmq.store.model.message.Filter;
import com.automq.rocketmq.store.model.message.PopResult;
import com.automq.rocketmq.store.model.message.PutResult;
import com.automq.rocketmq.store.service.SnapshotService;
import com.automq.rocketmq.store.service.api.KVService;
import com.automq.rocketmq.store.service.InflightService;
import com.automq.rocketmq.store.service.ReviveService;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.automq.rocketmq.store.util.SerializeUtil.decodeReceiptHandle;

public class MessageStoreImpl implements MessageStore {
    public static final String KV_NAMESPACE_CHECK_POINT = "check_point";
    public static final String KV_NAMESPACE_TIMER_TAG = "timer_tag";
    public static final String KV_NAMESPACE_FIFO_INDEX = "fifo_index";

    private final AtomicBoolean started = new AtomicBoolean(false);

    StoreConfig config;

    private final StreamStore streamStore;
    private final StoreMetadataService metadataService;
    private final KVService kvService;

    private ReviveService reviveService;
    private final InflightService inflightService;
    private final SnapshotService snapshotService;

    private final TopicQueueManager topicQueueManager;

    public MessageStoreImpl(StoreConfig config, StreamStore streamStore,
        StoreMetadataService metadataService, KVService kvService, InflightService inflightService,
        TopicQueueManager topicQueueManager, SnapshotService snapshotService) {
        this.config = config;
        this.streamStore = streamStore;
        this.metadataService = metadataService;
        this.kvService = kvService;
        this.inflightService = inflightService;
        this.topicQueueManager = topicQueueManager;
        this.snapshotService = snapshotService;
        this.reviveService = new ReviveService(KV_NAMESPACE_CHECK_POINT, KV_NAMESPACE_TIMER_TAG, kvService, metadataService, inflightService, topicQueueManager);
    }

    @Override
    public void start() throws Exception {
        if (!started.compareAndSet(false, true)) {
            return;
        }
        streamStore.start();
        reviveService.start();
        snapshotService.start();
        topicQueueManager.start();
    }

    @Override
    public void shutdown() throws Exception {
        if (!started.compareAndSet(true, false)) {
            return;
        }
        topicQueueManager.shutdown();
        snapshotService.shutdown();
        reviveService.shutdown();
        streamStore.shutdown();
    }

    @Override
    public CompletableFuture<PopResult> pop(long consumerGroupId, long topicId, int queueId, Filter filter,
        int batchSize, boolean fifo, boolean retry, long invisibleDuration) {
        if (fifo && retry) {
            return CompletableFuture.failedFuture(new RuntimeException("Fifo and retry cannot be true at the same time"));
        }
        TopicQueue topicQueue = topicQueueManager.get(topicId, queueId);
        if (fifo) {
            return topicQueue.popFifo(consumerGroupId, filter, batchSize, invisibleDuration);
        }
        if (retry) {
            return topicQueue.popRetry(consumerGroupId, filter, batchSize, invisibleDuration);
        }
        return topicQueue.popNormal(consumerGroupId, filter, batchSize, invisibleDuration);
    }

    @Override
    public CompletableFuture<PutResult> put(FlatMessage message) {
        TopicQueue topicQueue = topicQueueManager.get(message.topicId(), message.queueId());
        return topicQueue.put(message);
    }

    @Override
    public CompletableFuture<AckResult> ack(String receiptHandle) {
        // Write ack operation to operation log.
        // Operation id should be monotonically increasing for each queue
        ReceiptHandle handle = decodeReceiptHandle(receiptHandle);
        TopicQueue topicQueue = topicQueueManager.get(handle.topicId(), handle.queueId());
        return topicQueue.ack(receiptHandle);
    }

    @Override
    public CompletableFuture<ChangeInvisibleDurationResult> changeInvisibleDuration(String receiptHandle,
        long invisibleDuration) {
        // Write change invisible duration operation to operation log.
        // Operation id should be monotonically increasing for each queue
        ReceiptHandle handle = decodeReceiptHandle(receiptHandle);
        TopicQueue topicQueue = topicQueueManager.get(handle.topicId(), handle.queueId());
        return topicQueue.changeInvisibleDuration(receiptHandle, invisibleDuration);
    }

    @Override
    public CompletableFuture<Void> closeQueue(long topicId, int queueId) {
        TopicQueue topicQueue = topicQueueManager.get(topicId, queueId);
        return topicQueue.close();
    }

    @Override
    public CompletableFuture<Integer> getInflightStats(long consumerGroupId, long topicId, int queueId) {
        // Get check point count of specified consumer, topic and queue.
        TopicQueue topicQueue = topicQueueManager.get(topicId, queueId);
        return topicQueue.getInflightStats(consumerGroupId);
    }

    @Override
    public CompletableFuture<TopicQueue.QueueOffsetRange> getOffsetRange(long topicId, int queueId) {
        TopicQueue topicQueue = topicQueueManager.get(topicId, queueId);
        return topicQueue.getOffsetRange();
    }
}
