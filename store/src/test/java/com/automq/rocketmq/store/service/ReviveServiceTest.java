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

import com.automq.rocketmq.common.config.StoreConfig;
import com.automq.rocketmq.common.model.generated.FlatMessage;
import com.automq.rocketmq.metadata.api.StoreMetadataService;
import com.automq.rocketmq.store.DefaultMessageStateMachine;
import com.automq.rocketmq.store.MessageStoreImpl;
import com.automq.rocketmq.store.StreamTopicQueue;
import com.automq.rocketmq.store.api.MessageStateMachine;
import com.automq.rocketmq.store.api.StreamStore;
import com.automq.rocketmq.store.api.TopicQueue;
import com.automq.rocketmq.store.api.TopicQueueManager;
import com.automq.rocketmq.store.exception.StoreException;
import com.automq.rocketmq.store.mock.MockStoreMetadataService;
import com.automq.rocketmq.store.mock.MockStreamStore;
import com.automq.rocketmq.store.model.generated.ReceiptHandle;
import com.automq.rocketmq.store.model.message.Filter;
import com.automq.rocketmq.store.model.message.PopResult;
import com.automq.rocketmq.store.model.message.PullResult;
import com.automq.rocketmq.store.service.api.KVService;
import com.automq.rocketmq.store.util.SerializeUtil;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import static com.automq.rocketmq.store.mock.MockMessageUtil.buildMessage;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

class ReviveServiceTest {
    private static final String PATH = "/tmp/test_revive_service/";
    protected static final String KV_NAMESPACE_CHECK_POINT = "check_point";
    protected static final String KV_NAMESPACE_TIMER_TAG = "timer_tag";

    private static final long TOPIC_ID = 1313;
    private static final int QUEUE_ID = 13;
    private static final long CONSUMER_GROUP_ID = 131313;
    private static final long DATA_STREAM_ID = 13131313;
    private static final long OP_STREAM_ID = 1313131313;
    private static final long SNAPSHOT_STREAM_ID = 131313131313L;
    private static final long EPOCH = 13131313131313L;

    private KVService kvService;
    private StoreMetadataService metadataService;
    private StreamStore streamStore;
    private InflightService inflightService;
    private ReviveService reviveService;
    private MessageStateMachine stateMachine;
    private TopicQueue topicQueue;

    @BeforeEach
    public void setUp() throws StoreException {
        kvService = new RocksDBKVService(PATH);
        metadataService = new MockStoreMetadataService();
        streamStore = new MockStreamStore();
        inflightService = new InflightService();
        streamStore = new MockStreamStore();
        stateMachine = new DefaultMessageStateMachine(TOPIC_ID, QUEUE_ID, kvService);
        inflightService = new InflightService();
        SnapshotService snapshotService = new SnapshotService(streamStore, kvService);
        topicQueue = new StreamTopicQueue(new StoreConfig(), TOPIC_ID, QUEUE_ID,
            metadataService, stateMachine, streamStore, inflightService, snapshotService);
        TopicQueueManager manager = Mockito.mock(TopicQueueManager.class);
        Mockito.when(manager.getOrCreate(TOPIC_ID, QUEUE_ID)).thenReturn(CompletableFuture.completedFuture(topicQueue));
        reviveService = new ReviveService(KV_NAMESPACE_CHECK_POINT, KV_NAMESPACE_TIMER_TAG, kvService, metadataService, inflightService, manager);
        topicQueue.open().join();
    }

    @AfterEach
    public void tearDown() throws StoreException {
        kvService.destroy();
    }

    @Test
    void tryRevive() throws StoreException {
        // Append mock message.
        FlatMessage message = FlatMessage.getRootAsFlatMessage(buildMessage(TOPIC_ID, QUEUE_ID, "TagA"));
        topicQueue.put(message).join();
        // pop message
        int invisibleDuration = 1000 * 1000 * 1000;
        PopResult popResult = topicQueue.popNormal(CONSUMER_GROUP_ID, Filter.DEFAULT_FILTER, 1, invisibleDuration).join();
        assertEquals(1, popResult.messageList().size());
        // check ck exist
        ReceiptHandle handle = SerializeUtil.decodeReceiptHandle(popResult.messageList().get(0).receiptHandle().get());
        byte[] bytes = kvService.get(MessageStoreImpl.KV_NAMESPACE_CHECK_POINT, SerializeUtil.buildCheckPointKey(TOPIC_ID, QUEUE_ID, handle.operationId()));
        byte[] ckValue = kvService.get(KV_NAMESPACE_CHECK_POINT, SerializeUtil.buildCheckPointKey(TOPIC_ID, QUEUE_ID, handle.operationId()));
        assertNotNull(ckValue);
        // now revive but can't clear ck
        reviveService.tryRevive();
        ckValue = kvService.get(KV_NAMESPACE_CHECK_POINT, SerializeUtil.buildCheckPointKey(TOPIC_ID, QUEUE_ID, handle.operationId()));
        assertNotNull(ckValue);
        // after 1s revive can clear ck
        long reviveTimestamp = System.nanoTime() + invisibleDuration;
        await().until(() -> {
            reviveService.tryRevive();
            return reviveService.reviveTimestamp() >= reviveTimestamp;
        });

        ckValue = kvService.get(KV_NAMESPACE_CHECK_POINT, SerializeUtil.buildCheckPointKey(TOPIC_ID, QUEUE_ID, handle.operationId()));
        assertNull(ckValue);

        // check if this message has been appended to retry stream
        PullResult retryPullResult = topicQueue.pullRetry(CONSUMER_GROUP_ID, Filter.DEFAULT_FILTER, 0, 100).join();
        assertEquals(1, retryPullResult.messageList().size());
    }
}