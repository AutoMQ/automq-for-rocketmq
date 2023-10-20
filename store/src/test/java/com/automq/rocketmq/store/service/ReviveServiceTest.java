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
import com.automq.rocketmq.common.model.FlatMessageExt;
import com.automq.rocketmq.common.model.generated.FlatMessage;
import com.automq.rocketmq.metadata.api.StoreMetadataService;
import com.automq.rocketmq.store.api.DLQSender;
import com.automq.rocketmq.store.api.LogicQueue;
import com.automq.rocketmq.store.api.LogicQueueManager;
import com.automq.rocketmq.store.api.MessageStateMachine;
import com.automq.rocketmq.store.api.StreamStore;
import com.automq.rocketmq.store.exception.StoreException;
import com.automq.rocketmq.store.mock.MockStoreMetadataService;
import com.automq.rocketmq.store.mock.MockStreamStore;
import com.automq.rocketmq.store.model.generated.ReceiptHandle;
import com.automq.rocketmq.store.model.message.Filter;
import com.automq.rocketmq.store.model.message.PopResult;
import com.automq.rocketmq.store.model.message.PullResult;
import com.automq.rocketmq.store.queue.DefaultLogicQueueStateMachine;
import com.automq.rocketmq.store.queue.StreamLogicQueue;
import com.automq.rocketmq.store.service.api.KVService;
import com.automq.rocketmq.store.service.api.OperationLogService;
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
    private DLQSender dlqSender;
    private LogicQueue logicQueue;
    private LogicQueueManager manager;

    @BeforeEach
    public void setUp() throws StoreException {
        kvService = new RocksDBKVService(PATH);
        metadataService = Mockito.spy(new MockStoreMetadataService());
        streamStore = new MockStreamStore();
        inflightService = new InflightService();
        streamStore = new MockStreamStore();
        stateMachine = new DefaultLogicQueueStateMachine(TOPIC_ID, QUEUE_ID, kvService);
        inflightService = new InflightService();
        SnapshotService snapshotService = new SnapshotService(streamStore, kvService);
        OperationLogService operationLogService = new StreamOperationLogService(streamStore, snapshotService, new StoreConfig());
        logicQueue = new StreamLogicQueue(new StoreConfig(), TOPIC_ID, QUEUE_ID,
            metadataService, stateMachine, streamStore, operationLogService, inflightService);
        manager = Mockito.mock(LogicQueueManager.class);
        Mockito.doAnswer(ink -> {
            return CompletableFuture.completedFuture(logicQueue);
        }).when(manager).getOrCreate(TOPIC_ID, QUEUE_ID);
        dlqSender = Mockito.mock(DLQSender.class);
        reviveService = new ReviveService(KV_NAMESPACE_CHECK_POINT, KV_NAMESPACE_TIMER_TAG, kvService, metadataService, inflightService,
            manager, dlqSender);
        logicQueue.open().join();
    }

    @AfterEach
    public void tearDown() throws StoreException {
        kvService.destroy();
    }

    @Test
    void tryRevive_normal() throws StoreException {
        Mockito.doAnswer(ink -> {
            long consumerGroupId = ink.getArgument(0);
            assertEquals(CONSUMER_GROUP_ID, consumerGroupId);
            FlatMessageExt flatMessageExt = ink.getArgument(1);
            assertNotNull(flatMessageExt);
            return CompletableFuture.completedFuture(null);
        }).when(dlqSender).send(Mockito.anyLong(), Mockito.any(FlatMessageExt.class));
        // mock max delivery attempts
        Mockito.doReturn(CompletableFuture.completedFuture(2))
            .when(metadataService).maxDeliveryAttemptsOf(Mockito.anyLong());
        // Append mock message.
        FlatMessage message = FlatMessage.getRootAsFlatMessage(buildMessage(TOPIC_ID, QUEUE_ID, "TagA"));
        logicQueue.put(message).join();
        // pop message
        int invisibleDuration = 1000;
        PopResult popResult = logicQueue.popNormal(CONSUMER_GROUP_ID, Filter.DEFAULT_FILTER, 1, invisibleDuration).join();
        assertEquals(1, popResult.messageList().size());
        // check ck exist
        ReceiptHandle handle = SerializeUtil.decodeReceiptHandle(popResult.messageList().get(0).receiptHandle().get());
        byte[] ckValue = kvService.get(KV_NAMESPACE_CHECK_POINT, SerializeUtil.buildCheckPointKey(TOPIC_ID, QUEUE_ID, handle.operationId()));
        assertNotNull(ckValue);
        // now revive but can't clear ck
        reviveService.tryRevive();
        ckValue = kvService.get(KV_NAMESPACE_CHECK_POINT, SerializeUtil.buildCheckPointKey(TOPIC_ID, QUEUE_ID, handle.operationId()));
        assertNotNull(ckValue);
        // after 1s revive can clear ck
        long reviveTimestamp = System.currentTimeMillis() + invisibleDuration;
        await().until(() -> {
            reviveService.tryRevive();
            return reviveService.reviveTimestamp() >= reviveTimestamp && reviveService.inflightReviveCount() == 0;
        });

        ckValue = kvService.get(KV_NAMESPACE_CHECK_POINT, SerializeUtil.buildCheckPointKey(TOPIC_ID, QUEUE_ID, handle.operationId()));
        assertNull(ckValue);

        // check if this message has been appended to retry stream
        PullResult retryPullResult = logicQueue.pullRetry(CONSUMER_GROUP_ID, Filter.DEFAULT_FILTER, 0, invisibleDuration).join();
        assertEquals(1, retryPullResult.messageList().size());

        // pop retry
        PopResult retryPopResult = logicQueue.popRetry(CONSUMER_GROUP_ID, Filter.DEFAULT_FILTER, 1, invisibleDuration).join();
        assertEquals(1, retryPopResult.messageList().size());
        FlatMessageExt msg = retryPopResult.messageList().get(0);
        assertEquals(2, msg.deliveryAttempts());

        long reviveTimestamp1 = System.currentTimeMillis() + invisibleDuration;
        // after 1s
        await().until(() -> {
            reviveService.tryRevive();
            long ts0 = reviveService.reviveTimestamp();
            return ts0 >= reviveTimestamp1;
        });
        // wait inflight all complete
        await().until(() -> {
            return reviveService.inflightReviveCount() == 0;
        });

        // check if this message has been sent to DLQ
        Mockito.verify(dlqSender, Mockito.times(1))
            .send(Mockito.anyLong(), Mockito.any(FlatMessageExt.class));
        PopResult popResult1 = logicQueue.popRetry(CONSUMER_GROUP_ID, Filter.DEFAULT_FILTER, 1, invisibleDuration).join();
        assertEquals(0, popResult1.messageList().size());
    }

    @Test
    void tryRevive_fifo() throws StoreException {
        Mockito.doAnswer(ink -> {
            long consumerGroupId = ink.getArgument(0);
            assertEquals(CONSUMER_GROUP_ID, consumerGroupId);
            FlatMessageExt flatMessageExt = ink.getArgument(1);
            assertNotNull(flatMessageExt);
            return CompletableFuture.completedFuture(null);
        }).when(dlqSender).send(Mockito.anyLong(), Mockito.any(FlatMessageExt.class));
        // mock max delivery attempts
        Mockito.doReturn(CompletableFuture.completedFuture(2))
            .when(metadataService).maxDeliveryAttemptsOf(Mockito.anyLong());
        // Append mock message.
        for (int i = 0; i < 2; i++) {
            FlatMessage message = FlatMessage.getRootAsFlatMessage(buildMessage(TOPIC_ID, QUEUE_ID, "TagA"));
            logicQueue.put(message).join();
        }
        // pop message
        int invisibleDuration = 1000;
        PopResult popResult = logicQueue.popFifo(CONSUMER_GROUP_ID, Filter.DEFAULT_FILTER, 1, invisibleDuration).join();
        assertEquals(1, popResult.messageList().size());
        // check ck exist
        ReceiptHandle handle = SerializeUtil.decodeReceiptHandle(popResult.messageList().get(0).receiptHandle().get());
        byte[] ckValue = kvService.get(KV_NAMESPACE_CHECK_POINT, SerializeUtil.buildCheckPointKey(TOPIC_ID, QUEUE_ID, handle.operationId()));
        assertNotNull(ckValue);
        // now revive but can't clear ck
        reviveService.tryRevive();
        ckValue = kvService.get(KV_NAMESPACE_CHECK_POINT, SerializeUtil.buildCheckPointKey(TOPIC_ID, QUEUE_ID, handle.operationId()));
        assertNotNull(ckValue);
        // after 1s revive can clear ck
        long reviveTimestamp = System.currentTimeMillis() + invisibleDuration;
        await().until(() -> {
            reviveService.tryRevive();
            return reviveService.reviveTimestamp() >= reviveTimestamp && reviveService.inflightReviveCount() == 0;
        });

        ckValue = kvService.get(KV_NAMESPACE_CHECK_POINT, SerializeUtil.buildCheckPointKey(TOPIC_ID, QUEUE_ID, handle.operationId()));
        assertNull(ckValue);

        // pop again
        PopResult retryPopResult = logicQueue.popFifo(CONSUMER_GROUP_ID, Filter.DEFAULT_FILTER, 1, invisibleDuration).join();
        assertEquals(1, retryPopResult.messageList().size());
        FlatMessageExt msg = retryPopResult.messageList().get(0);
        assertEquals(2, msg.deliveryAttempts());
        assertEquals(0, msg.offset());

        long reviveTimestamp1 = System.currentTimeMillis() + invisibleDuration;
        // after 1s
        await().until(() -> {
            reviveService.tryRevive();
            long ts0 = reviveService.reviveTimestamp();
            return ts0 >= reviveTimestamp1;
        });
        // wait inflight all complete
        await().until(() -> {
            return reviveService.inflightReviveCount() == 0;
        });

        // check if this message has been sent to DLQ
        Mockito.verify(dlqSender, Mockito.times(1))
            .send(Mockito.anyLong(), Mockito.any(FlatMessageExt.class));

        assertEquals(1, logicQueue.getAckOffset(CONSUMER_GROUP_ID));

        // pop again
        PopResult retryPopResult1 = logicQueue.popFifo(CONSUMER_GROUP_ID, Filter.DEFAULT_FILTER, 1, invisibleDuration).join();
        assertEquals(1, retryPopResult1.messageList().size());
        FlatMessageExt msg1 = retryPopResult1.messageList().get(0);
        assertEquals(1, msg1.deliveryAttempts());
        assertEquals(1, msg1.offset());
    }

    @Test
    void tryRevive_queue_not_opened() throws Exception {
        Mockito.doAnswer(ink -> {
            long consumerGroupId = ink.getArgument(0);
            assertEquals(CONSUMER_GROUP_ID, consumerGroupId);
            FlatMessageExt flatMessageExt = ink.getArgument(1);
            assertNotNull(flatMessageExt);
            return CompletableFuture.completedFuture(null);
        }).when(dlqSender).send(Mockito.anyLong(), Mockito.any(FlatMessageExt.class));
        // mock max delivery attempts
        Mockito.doReturn(CompletableFuture.completedFuture(2))
            .when(metadataService).maxDeliveryAttemptsOf(Mockito.anyLong());
        // Append mock message.
        FlatMessage message = FlatMessage.getRootAsFlatMessage(buildMessage(TOPIC_ID, QUEUE_ID, "TagA"));
        logicQueue.put(message).join();
        // pop message
        int invisibleDuration = 1000;
        PopResult popResult = logicQueue.popNormal(CONSUMER_GROUP_ID, Filter.DEFAULT_FILTER, 1, invisibleDuration).join();
        assertEquals(1, popResult.messageList().size());
        // check ck exist
        ReceiptHandle handle = SerializeUtil.decodeReceiptHandle(popResult.messageList().get(0).receiptHandle().get());
        byte[] ckValue = kvService.get(KV_NAMESPACE_CHECK_POINT, SerializeUtil.buildCheckPointKey(TOPIC_ID, QUEUE_ID, handle.operationId()));
        assertNotNull(ckValue);
        // now revive but can't clear ck
        reviveService.tryRevive();
        ckValue = kvService.get(KV_NAMESPACE_CHECK_POINT, SerializeUtil.buildCheckPointKey(TOPIC_ID, QUEUE_ID, handle.operationId()));
        assertNotNull(ckValue);
        // after 1s revive can clear ck
        long reviveTimestamp = System.currentTimeMillis() + invisibleDuration;
        await().until(() -> {
            reviveService.tryRevive();
            return reviveService.reviveTimestamp() >= reviveTimestamp && reviveService.inflightReviveCount() == 0;
        });

        ckValue = kvService.get(KV_NAMESPACE_CHECK_POINT, SerializeUtil.buildCheckPointKey(TOPIC_ID, QUEUE_ID, handle.operationId()));
        assertNull(ckValue);

        // check if this message has been appended to retry stream
        PullResult retryPullResult = logicQueue.pullRetry(CONSUMER_GROUP_ID, Filter.DEFAULT_FILTER, 0, invisibleDuration).join();
        assertEquals(1, retryPullResult.messageList().size());

        // pop retry
        PopResult retryPopResult = logicQueue.popRetry(CONSUMER_GROUP_ID, Filter.DEFAULT_FILTER, 1, invisibleDuration).join();
        assertEquals(1, retryPopResult.messageList().size());
        FlatMessageExt msg = retryPopResult.messageList().get(0);
        assertEquals(2, msg.deliveryAttempts());

        long reviveTimestamp1 = System.currentTimeMillis() + invisibleDuration;
        // mock that queue is opening
        LogicQueue mockedQueue = Mockito.mock(LogicQueue.class);
        Mockito.doReturn(LogicQueue.State.OPENING)
            .when(mockedQueue).getState();
        Mockito.doReturn(CompletableFuture.completedFuture(mockedQueue))
            .when(manager).getOrCreate(TOPIC_ID, QUEUE_ID);
        // after 1s
        await().until(() -> {
            reviveService.tryRevive();
            long ts0 = reviveService.reviveTimestamp();
            return ts0 >= reviveTimestamp1;
        });
        // wait inflight all complete
        await().until(() -> {
            return reviveService.inflightReviveCount() == 0;
        });

        // check if this message has been sent to DLQ
        Mockito.verify(dlqSender, Mockito.times(0)).send(Mockito.anyLong(), Mockito.any(FlatMessageExt.class));
        // check ck still exist
        handle = SerializeUtil.decodeReceiptHandle(retryPopResult.messageList().get(0).receiptHandle().get());
        ckValue = kvService.get(KV_NAMESPACE_CHECK_POINT, SerializeUtil.buildCheckPointKey(TOPIC_ID, QUEUE_ID, handle.operationId()));
        assertNotNull(ckValue);

        // mock that queue is closed
        Mockito.doReturn(LogicQueue.State.CLOSED)
            .when(mockedQueue).getState();

        reviveService.tryRevive();

        // wait inflight all complete
        await().until(() -> {
            return reviveService.inflightReviveCount() == 0;
        });

        // check if this message has been sent to DLQ
        Mockito.verify(dlqSender, Mockito.times(0)).send(Mockito.anyLong(), Mockito.any(FlatMessageExt.class));
        // check ck still exist
        handle = SerializeUtil.decodeReceiptHandle(retryPopResult.messageList().get(0).receiptHandle().get());
        ckValue = kvService.get(KV_NAMESPACE_CHECK_POINT, SerializeUtil.buildCheckPointKey(TOPIC_ID, QUEUE_ID, handle.operationId()));
        assertNotNull(ckValue);

        // mock that queue is opened
        Mockito.doReturn(CompletableFuture.completedFuture(logicQueue))
            .when(manager).getOrCreate(TOPIC_ID, QUEUE_ID);

        reviveService.tryRevive();

        // wait inflight all complete
        await().until(() -> {
            return reviveService.inflightReviveCount() == 0;
        });

        // check if this message has been sent to DLQ
        Mockito.verify(dlqSender, Mockito.times(1)).send(Mockito.anyLong(), Mockito.any(FlatMessageExt.class));
        // check ck not exist
        handle = SerializeUtil.decodeReceiptHandle(retryPopResult.messageList().get(0).receiptHandle().get());
        ckValue = kvService.get(KV_NAMESPACE_CHECK_POINT, SerializeUtil.buildCheckPointKey(TOPIC_ID, QUEUE_ID, handle.operationId()));
        assertNull(ckValue);
    }

}