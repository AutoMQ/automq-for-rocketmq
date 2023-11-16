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
import com.automq.rocketmq.store.MessageStoreTest;
import com.automq.rocketmq.store.api.DeadLetterSender;
import com.automq.rocketmq.store.api.LogicQueue;
import com.automq.rocketmq.store.api.LogicQueueManager;
import com.automq.rocketmq.store.api.MessageStateMachine;
import com.automq.rocketmq.store.api.StreamStore;
import com.automq.rocketmq.store.exception.StoreException;
import com.automq.rocketmq.store.mock.MockStoreMetadataService;
import com.automq.rocketmq.store.mock.MockStreamStore;
import com.automq.rocketmq.store.model.StoreContext;
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
import static org.junit.jupiter.api.Assertions.assertTrue;

class ReviveServiceTest {
    private static final String PATH = "/tmp/test_revive_service/";
    protected static final String KV_NAMESPACE_CHECK_POINT = "check_point";
    private static final long TOPIC_ID = 1313;
    private static final int QUEUE_ID = 13;
    private static final long CONSUMER_GROUP_ID = 131313;

    private KVService kvService;
    private StoreMetadataService metadataService;
    private TimerService timerService;
    private ReviveService reviveService;
    private DeadLetterSender deadLetterSender;
    private LogicQueue logicQueue;

    @BeforeEach
    public void setUp() throws StoreException {
        kvService = new RocksDBKVService(PATH);
        metadataService = Mockito.spy(new MockStoreMetadataService());
        StreamStore streamStore = new MockStreamStore();
        InflightService inflightService = new InflightService();
        timerService = new TimerService(MessageStoreTest.KV_NAMESPACE_TIMER_TAG, kvService);
        MessageStateMachine stateMachine = new DefaultLogicQueueStateMachine(TOPIC_ID, QUEUE_ID, kvService, timerService);
        SnapshotService snapshotService = new SnapshotService(streamStore, kvService);
        OperationLogService operationLogService = new StreamOperationLogService(streamStore, snapshotService, new StoreConfig());
        StreamReclaimService streamReclaimService = new StreamReclaimService(streamStore);
        logicQueue = new StreamLogicQueue(new StoreConfig(), TOPIC_ID, QUEUE_ID,
            metadataService, stateMachine, streamStore, operationLogService, inflightService, streamReclaimService);
        LogicQueueManager manager = Mockito.mock(LogicQueueManager.class);
        Mockito.doAnswer(ink -> CompletableFuture.completedFuture(logicQueue)).when(manager).getOrCreate(Mockito.any(), Mockito.eq(TOPIC_ID), Mockito.eq(QUEUE_ID));
        deadLetterSender = Mockito.mock(DeadLetterSender.class);
        reviveService = new ReviveService(KV_NAMESPACE_CHECK_POINT, kvService, timerService, metadataService, new MessageArrivalNotificationService(),
            manager, deadLetterSender);
        logicQueue.open().join();
    }

    @AfterEach
    public void tearDown() throws StoreException {
        kvService.destroy();
    }

    @Test
    void revive_normal() throws StoreException {
        Mockito.doAnswer(ink -> {
            long consumerGroupId = ink.getArgument(0);
            assertEquals(CONSUMER_GROUP_ID, consumerGroupId);
            FlatMessageExt flatMessageExt = ink.getArgument(1);
            assertNotNull(flatMessageExt);
            return CompletableFuture.completedFuture(null);
        }).when(deadLetterSender).send(Mockito.any(), Mockito.anyLong(), Mockito.any(FlatMessageExt.class));
        // mock max delivery attempts
        Mockito.doReturn(CompletableFuture.completedFuture(2))
            .when(metadataService).maxDeliveryAttemptsOf(Mockito.anyLong());
        // Append mock message.
        FlatMessage message = FlatMessage.getRootAsFlatMessage(buildMessage(TOPIC_ID, QUEUE_ID, "TagA"));
        logicQueue.put(StoreContext.EMPTY, message).join();
        // pop message
        int invisibleDuration = 100;
        PopResult popResult = logicQueue.popNormal(StoreContext.EMPTY, CONSUMER_GROUP_ID, Filter.DEFAULT_FILTER, 1, invisibleDuration).join();
        assertEquals(1, popResult.messageList().size());
        // check ck exist
        assertTrue(popResult.messageList().get(0).receiptHandle().isPresent());
        ReceiptHandle handle = SerializeUtil.decodeReceiptHandle(popResult.messageList().get(0).receiptHandle().get());
        byte[] ckValue = kvService.get(KV_NAMESPACE_CHECK_POINT, SerializeUtil.buildCheckPointKey(TOPIC_ID, QUEUE_ID, handle.consumerGroupId(), handle.operationId()));
        assertNotNull(ckValue);
        // now revive but can't clear ck
        timerService.dequeue();
        ckValue = kvService.get(KV_NAMESPACE_CHECK_POINT, SerializeUtil.buildCheckPointKey(TOPIC_ID, QUEUE_ID, handle.consumerGroupId(), handle.operationId()));
        assertNotNull(ckValue);
        // Wait clearing ck.
        await().until(() -> {
            timerService.dequeue();
            byte[] ck = kvService.get(KV_NAMESPACE_CHECK_POINT, SerializeUtil.buildCheckPointKey(TOPIC_ID, QUEUE_ID, handle.consumerGroupId(), handle.operationId()));
            return ck == null;
        });

        // check if this message has been appended to retry stream
        PullResult retryPullResult = logicQueue.pullRetry(CONSUMER_GROUP_ID, Filter.DEFAULT_FILTER, 0, invisibleDuration).join();
        assertEquals(1, retryPullResult.messageList().size());

        // pop retry
        long reviveTimestamp1 = System.currentTimeMillis() + invisibleDuration;
        PopResult retryPopResult = logicQueue.popRetry(StoreContext.EMPTY, CONSUMER_GROUP_ID, Filter.DEFAULT_FILTER, 1, invisibleDuration).join();
        assertEquals(1, retryPopResult.messageList().size());
        FlatMessageExt msg = retryPopResult.messageList().get(0);
        assertEquals(2, msg.deliveryAttempts());

        // after 1s
        await().until(() -> {
            timerService.dequeue();
            return reviveService.reviveTimestamp() >= reviveTimestamp1;
        });
        // wait inflight all complete
        await().until(() -> reviveService.inflightReviveCount() == 0);

        // check if this message has been sent to DLQ
        Mockito.verify(deadLetterSender, Mockito.times(1))
            .send(Mockito.any(), Mockito.anyLong(), Mockito.any(FlatMessageExt.class));
        PopResult popResult1 = logicQueue.popRetry(StoreContext.EMPTY, CONSUMER_GROUP_ID, Filter.DEFAULT_FILTER, 1, invisibleDuration).join();
        assertEquals(0, popResult1.messageList().size());
    }

    @Test
    void revive_fifo() throws StoreException {
        Mockito.doAnswer(ink -> {
            long consumerGroupId = ink.getArgument(1);
            assertEquals(CONSUMER_GROUP_ID, consumerGroupId);
            FlatMessageExt flatMessageExt = ink.getArgument(2);
            assertNotNull(flatMessageExt);
            return CompletableFuture.completedFuture(null);
        }).when(deadLetterSender).send(Mockito.any(), Mockito.anyLong(), Mockito.any(FlatMessageExt.class));
        // mock max delivery attempts
        Mockito.doReturn(CompletableFuture.completedFuture(2))
            .when(metadataService).maxDeliveryAttemptsOf(Mockito.anyLong());
        // Append mock message.
        for (int i = 0; i < 2; i++) {
            FlatMessage message = FlatMessage.getRootAsFlatMessage(buildMessage(TOPIC_ID, QUEUE_ID, "TagA"));
            logicQueue.put(StoreContext.EMPTY, message).join();
        }
        // pop message
        int invisibleDuration = 100;
        PopResult popResult = logicQueue.popFifo(StoreContext.EMPTY, CONSUMER_GROUP_ID, Filter.DEFAULT_FILTER, 1, invisibleDuration).join();
        assertEquals(1, popResult.messageList().size());
        // check ck exist
        assertTrue(popResult.messageList().get(0).receiptHandle().isPresent());
        ReceiptHandle handle = SerializeUtil.decodeReceiptHandle(popResult.messageList().get(0).receiptHandle().get());
        byte[] ckValue = kvService.get(KV_NAMESPACE_CHECK_POINT, SerializeUtil.buildCheckPointKey(TOPIC_ID, QUEUE_ID, handle.consumerGroupId(), handle.operationId()));
        assertNotNull(ckValue);
        // now revive but can't clear ck
        timerService.dequeue();
        ckValue = kvService.get(KV_NAMESPACE_CHECK_POINT, SerializeUtil.buildCheckPointKey(TOPIC_ID, QUEUE_ID, handle.consumerGroupId(), handle.operationId()));
        assertNotNull(ckValue);
        // after 1s revive can clear ck
        await().until(() -> {
            timerService.dequeue();
            byte[] ck = kvService.get(KV_NAMESPACE_CHECK_POINT, SerializeUtil.buildCheckPointKey(TOPIC_ID, QUEUE_ID, handle.consumerGroupId(), handle.operationId()));
            return ck == null;
        });

        // pop again
        long reviveTimestamp1 = System.currentTimeMillis() + invisibleDuration;
        PopResult retryPopResult = logicQueue.popFifo(StoreContext.EMPTY, CONSUMER_GROUP_ID, Filter.DEFAULT_FILTER, 1, invisibleDuration).join();
        assertEquals(1, retryPopResult.messageList().size());
        FlatMessageExt msg = retryPopResult.messageList().get(0);
        assertEquals(2, msg.deliveryAttempts());
        assertEquals(0, msg.offset());

        // after 1s
        await().until(() -> {
            timerService.dequeue();
            return reviveService.reviveTimestamp() >= reviveTimestamp1;
        });
        // wait inflight all complete
        await().until(() -> reviveService.inflightReviveCount() == 0);

        // check if this message has been sent to DLQ
        Mockito.verify(deadLetterSender, Mockito.times(1))
            .send(Mockito.any(), Mockito.anyLong(), Mockito.any(FlatMessageExt.class));

        assertEquals(1, logicQueue.getAckOffset(CONSUMER_GROUP_ID));

        // pop again
        PopResult retryPopResult1 = logicQueue.popFifo(StoreContext.EMPTY, CONSUMER_GROUP_ID, Filter.DEFAULT_FILTER, 1, invisibleDuration).join();
        assertEquals(1, retryPopResult1.messageList().size());
        FlatMessageExt msg1 = retryPopResult1.messageList().get(0);
        assertEquals(1, msg1.deliveryAttempts());
        assertEquals(1, msg1.offset());
    }

    @Test
    void revive_dead_letter() throws Exception {
        Mockito.doAnswer(ink -> {
            long consumerGroupId = ink.getArgument(1);
            assertEquals(CONSUMER_GROUP_ID, consumerGroupId);
            FlatMessageExt flatMessageExt = ink.getArgument(2);
            assertNotNull(flatMessageExt);
            return CompletableFuture.completedFuture(null);
        }).when(deadLetterSender).send(Mockito.any(), Mockito.anyLong(), Mockito.any(FlatMessageExt.class));
        // mock max delivery attempts
        Mockito.doReturn(CompletableFuture.completedFuture(2))
            .when(metadataService).maxDeliveryAttemptsOf(Mockito.anyLong());
        // Append mock message.
        FlatMessage message = FlatMessage.getRootAsFlatMessage(buildMessage(TOPIC_ID, QUEUE_ID, "TagA"));
        logicQueue.put(StoreContext.EMPTY, message).join();
        // pop message
        int invisibleDuration = 100;
        PopResult popResult = logicQueue.popNormal(StoreContext.EMPTY, CONSUMER_GROUP_ID, Filter.DEFAULT_FILTER, 1, invisibleDuration).join();
        assertEquals(1, popResult.messageList().size());
        // check ck exist
        assertTrue(popResult.messageList().get(0).receiptHandle().isPresent());
        ReceiptHandle handle = SerializeUtil.decodeReceiptHandle(popResult.messageList().get(0).receiptHandle().get());
        byte[] ckValue = kvService.get(KV_NAMESPACE_CHECK_POINT, SerializeUtil.buildCheckPointKey(TOPIC_ID, QUEUE_ID, handle.consumerGroupId(), handle.operationId()));
        assertNotNull(ckValue);
        // now revive but can't clear ck
        timerService.dequeue();
        ckValue = kvService.get(KV_NAMESPACE_CHECK_POINT, SerializeUtil.buildCheckPointKey(TOPIC_ID, QUEUE_ID, handle.consumerGroupId(), handle.operationId()));
        assertNotNull(ckValue);

        // Wait clearing ck.
        ReceiptHandle finalHandle = handle;
        await().until(() -> {
            timerService.dequeue();
            byte[] ck = kvService.get(KV_NAMESPACE_CHECK_POINT, SerializeUtil.buildCheckPointKey(TOPIC_ID, QUEUE_ID, finalHandle.consumerGroupId(), finalHandle.operationId()));
            return ck == null;
        });

        // check if this message has been appended to retry stream
        PullResult retryPullResult = logicQueue.pullRetry(CONSUMER_GROUP_ID, Filter.DEFAULT_FILTER, 0, 32).join();
        assertEquals(1, retryPullResult.messageList().size());

        // pop retry
        long deliveryTimestamp = System.currentTimeMillis() + invisibleDuration;
        PopResult retryPopResult = logicQueue.popRetry(StoreContext.EMPTY, CONSUMER_GROUP_ID, Filter.DEFAULT_FILTER, 1, invisibleDuration).join();
        assertEquals(1, retryPopResult.messageList().size());
        FlatMessageExt msg = retryPopResult.messageList().get(0);
        assertEquals(2, msg.deliveryAttempts());

        // wait inflight all complete
        await().until(() -> {
            timerService.dequeue();
            return reviveService.reviveTimestamp() >= deliveryTimestamp && reviveService.inflightReviveCount() == 0;
        });

        // check if this message has been sent to DLQ
        Mockito.verify(deadLetterSender, Mockito.times(1)).send(Mockito.any(), Mockito.anyLong(), Mockito.any(FlatMessageExt.class));
        // check ck not exist
        assertTrue(retryPopResult.messageList().get(0).receiptHandle().isPresent());
        handle = SerializeUtil.decodeReceiptHandle(retryPopResult.messageList().get(0).receiptHandle().get());
        ckValue = kvService.get(KV_NAMESPACE_CHECK_POINT, SerializeUtil.buildCheckPointKey(TOPIC_ID, QUEUE_ID, handle.consumerGroupId(), handle.operationId()));
        assertNull(ckValue);
    }
}