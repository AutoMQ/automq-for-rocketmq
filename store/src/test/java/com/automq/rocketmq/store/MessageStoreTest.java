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

import apache.rocketmq.controller.v1.StreamMetadata;
import com.automq.rocketmq.common.config.StoreConfig;
import com.automq.rocketmq.common.model.FlatMessageExt;
import com.automq.rocketmq.common.model.generated.FlatMessage;
import com.automq.rocketmq.metadata.api.StoreMetadataService;
import com.automq.rocketmq.store.api.DeadLetterSender;
import com.automq.rocketmq.store.api.LogicQueue;
import com.automq.rocketmq.store.api.LogicQueueManager;
import com.automq.rocketmq.store.api.MessageStore;
import com.automq.rocketmq.store.api.S3ObjectOperator;
import com.automq.rocketmq.store.api.StreamStore;
import com.automq.rocketmq.store.exception.StoreException;
import com.automq.rocketmq.store.mock.MockStoreMetadataService;
import com.automq.rocketmq.store.mock.MockStreamStore;
import com.automq.rocketmq.store.model.StoreContext;
import com.automq.rocketmq.store.model.message.AckResult;
import com.automq.rocketmq.store.model.message.ChangeInvisibleDurationResult;
import com.automq.rocketmq.store.model.message.Filter;
import com.automq.rocketmq.store.model.message.PopResult;
import com.automq.rocketmq.store.model.message.ResetConsumeOffsetResult;
import com.automq.rocketmq.store.queue.DefaultLogicQueueManager;
import com.automq.rocketmq.store.service.InflightService;
import com.automq.rocketmq.store.service.MessageArrivalNotificationService;
import com.automq.rocketmq.store.service.ReviveService;
import com.automq.rocketmq.store.service.RocksDBKVService;
import com.automq.rocketmq.store.service.SnapshotService;
import com.automq.rocketmq.store.service.StreamOperationLogService;
import com.automq.rocketmq.store.service.StreamReclaimService;
import com.automq.rocketmq.store.service.TimerService;
import com.automq.rocketmq.store.service.TransactionService;
import com.automq.rocketmq.store.service.api.KVService;
import com.automq.rocketmq.store.service.api.OperationLogService;
import com.automq.stream.s3.operator.MemoryS3Operator;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.rocketmq.common.UtilAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import static com.automq.rocketmq.store.MessageStoreImpl.KV_NAMESPACE_CHECK_POINT;
import static com.automq.rocketmq.store.MessageStoreImpl.KV_NAMESPACE_FIFO_INDEX;
import static com.automq.rocketmq.store.mock.MockMessageUtil.buildMessage;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class MessageStoreTest {
    private static final String PATH = "/tmp/ros/message_store_test/";
    public static final String KV_NAMESPACE_TIMER_TAG = "timer_0";
    private static final long TOPIC_ID = 1313;
    private static final int QUEUE_ID = 13;
    private static final long CONSUMER_GROUP_ID = 131313;

    private KVService kvService;
    private StoreMetadataService metadataService;
    private StreamStore streamStore;
    private MessageStore messageStore;
    private StoreConfig config;
    private LogicQueueManager logicQueueManager;
    private ReviveService reviveService;

    @BeforeEach
    public void setUp() throws Exception {
        UtilAll.deleteFile(new java.io.File(PATH));
        kvService = Mockito.spy(new RocksDBKVService(PATH));
        metadataService = new MockStoreMetadataService();
        streamStore = new MockStreamStore();
        InflightService inflightService = new InflightService();
        config = new StoreConfig();
        SnapshotService snapshotService = new SnapshotService(streamStore, kvService);
        OperationLogService operationLogService = new StreamOperationLogService(streamStore, snapshotService, config);
        StreamReclaimService streamReclaimService = new StreamReclaimService(streamStore);
        TimerService timerService = new TimerService(KV_NAMESPACE_TIMER_TAG, kvService);
        logicQueueManager = new DefaultLogicQueueManager(config, streamStore, kvService, timerService, metadataService, operationLogService, inflightService, streamReclaimService);
        DeadLetterSender deadLetterSender = Mockito.mock(DeadLetterSender.class);
        Mockito.doReturn(CompletableFuture.completedFuture(null))
            .when(deadLetterSender).send(Mockito.any(), Mockito.anyLong(), Mockito.any(FlatMessage.class));
        MessageArrivalNotificationService messageArrivalNotificationService = new MessageArrivalNotificationService();
        reviveService = new ReviveService(KV_NAMESPACE_CHECK_POINT, kvService, timerService, metadataService, messageArrivalNotificationService,
            logicQueueManager, deadLetterSender);
        TransactionService transactionService = new TransactionService(config, timerService);
        S3ObjectOperator operator = new S3ObjectOperatorImpl(new MemoryS3Operator());
        messageStore = new MessageStoreImpl(config, streamStore, metadataService, kvService, timerService,
            inflightService, snapshotService, logicQueueManager, reviveService, operator,
            messageArrivalNotificationService, transactionService);
        messageStore.start();
    }

    @AfterEach
    public void tearDown() throws Exception {
        messageStore.shutdown();
        kvService.destroy();
        UtilAll.deleteFile(new java.io.File(PATH));
    }

    @Test
    public void pop_normal() {
        // 1. append 5 message
        for (int i = 0; i < 5; i++) {
            FlatMessage message = FlatMessage.getRootAsFlatMessage(buildMessage(TOPIC_ID, QUEUE_ID, "TagA"));
            messageStore.put(StoreContext.EMPTY, message).join();
        }
        List<String> receiptHandles = new ArrayList<>();
        // 2. pop 3 message
        int invisibleDuration = 50;
        PopResult popResult = messageStore.pop(StoreContext.EMPTY, CONSUMER_GROUP_ID, TOPIC_ID, QUEUE_ID, Filter.DEFAULT_FILTER, 3, false, false, invisibleDuration).join();
        assertEquals(3, popResult.messageList().size());
        for (FlatMessageExt message : popResult.messageList()) {
            assertTrue(message.receiptHandle().isPresent());
            receiptHandles.add(message.receiptHandle().get());
        }
        // 3. pop 3 message
        long nextVisibleTimestamp = System.currentTimeMillis() + invisibleDuration;
        popResult = messageStore.pop(StoreContext.EMPTY, CONSUMER_GROUP_ID, TOPIC_ID, QUEUE_ID, Filter.DEFAULT_FILTER, 3, false, false, invisibleDuration).join();
        assertEquals(2, popResult.messageList().size());
        for (FlatMessageExt message : popResult.messageList()) {
            assertTrue(message.receiptHandle().isPresent());
            receiptHandles.add(message.receiptHandle().get());
        }

        // 4. ack msg_2, msg_3
        messageStore.ack(receiptHandles.get(2)).join();
        messageStore.ack(receiptHandles.get(3)).join();

        // 5. pop again
        popResult = messageStore.pop(StoreContext.EMPTY, CONSUMER_GROUP_ID, TOPIC_ID, QUEUE_ID, Filter.DEFAULT_FILTER, 3, false, false, invisibleDuration).join();
        assertEquals(PopResult.Status.END_OF_QUEUE, popResult.status());
        popResult = messageStore.pop(StoreContext.EMPTY, CONSUMER_GROUP_ID, TOPIC_ID, QUEUE_ID, Filter.DEFAULT_FILTER, 3, false, true, invisibleDuration).join();
        assertEquals(PopResult.Status.END_OF_QUEUE, popResult.status());

        // 6. after 1100ms, pop again
        await().atMost(Duration.ofSeconds(3))
            .until(() -> reviveService.reviveTimestamp() >= nextVisibleTimestamp && reviveService.inflightReviveCount() == 0);
        long nextVisibleTimestamp1 = System.currentTimeMillis() + invisibleDuration;
        popResult = messageStore.pop(StoreContext.EMPTY, CONSUMER_GROUP_ID, TOPIC_ID, QUEUE_ID, Filter.DEFAULT_FILTER, 3, false, true, invisibleDuration).join();
        assertEquals(3, popResult.messageList().size());
        assertEquals(0, popResult.messageList().get(0).offset());
        assertEquals(0, popResult.messageList().get(0).originalOffset());
        assertEquals(2, popResult.messageList().get(0).deliveryAttempts());
        assertEquals(1, popResult.messageList().get(1).offset());
        assertEquals(1, popResult.messageList().get(1).originalOffset());
        assertEquals(2, popResult.messageList().get(1).deliveryAttempts());
        assertEquals(2, popResult.messageList().get(2).offset());
        assertEquals(4, popResult.messageList().get(2).originalOffset());
        assertEquals(2, popResult.messageList().get(2).deliveryAttempts());

        popResult = messageStore.pop(StoreContext.EMPTY, CONSUMER_GROUP_ID, TOPIC_ID, QUEUE_ID, Filter.DEFAULT_FILTER, 3, false, false, invisibleDuration).join();
        assertEquals(PopResult.Status.END_OF_QUEUE, popResult.status());

        // 7. after 1100ms, pop again
        await().atMost(Duration.ofSeconds(3))
            .until(() -> reviveService.reviveTimestamp() >= nextVisibleTimestamp1 && reviveService.inflightReviveCount() == 0);

        popResult = messageStore.pop(StoreContext.EMPTY, CONSUMER_GROUP_ID, TOPIC_ID, QUEUE_ID, Filter.DEFAULT_FILTER, 3, false, true, invisibleDuration).join();
        assertEquals(PopResult.Status.FOUND, popResult.status());
        assertEquals(3, popResult.messageList().size());
        assertEquals(3, popResult.messageList().get(0).offset());
        assertEquals(0, popResult.messageList().get(0).originalOffset());
        assertEquals(3, popResult.messageList().get(0).deliveryAttempts());
        assertEquals(4, popResult.messageList().get(1).offset());
        assertEquals(1, popResult.messageList().get(1).originalOffset());
        assertEquals(3, popResult.messageList().get(1).deliveryAttempts());
        assertEquals(5, popResult.messageList().get(2).offset());
        assertEquals(4, popResult.messageList().get(2).originalOffset());
        assertEquals(3, popResult.messageList().get(2).deliveryAttempts());

        popResult = messageStore.pop(StoreContext.EMPTY, CONSUMER_GROUP_ID, TOPIC_ID, QUEUE_ID, Filter.DEFAULT_FILTER, 3, false, false, invisibleDuration).join();
        assertEquals(PopResult.Status.END_OF_QUEUE, popResult.status());
    }

    @Test
    public void pop_order() {
        // 1. append 5 message
        for (int i = 0; i < 5; i++) {
            FlatMessage message = FlatMessage.getRootAsFlatMessage(buildMessage(TOPIC_ID, QUEUE_ID, "TagA"));
            messageStore.put(StoreContext.EMPTY, message).join();
        }
        List<String> receiptHandles = new ArrayList<>();
        // 2. pop 3 message
        int invisibleDuration = 100;
        PopResult popResult = messageStore.pop(StoreContext.EMPTY, CONSUMER_GROUP_ID, TOPIC_ID, QUEUE_ID, Filter.DEFAULT_FILTER, 3, true, false, invisibleDuration).join();
        assertEquals(3, popResult.messageList().size());
        for (FlatMessageExt message : popResult.messageList()) {
            assertTrue(message.receiptHandle().isPresent());
            receiptHandles.add(message.receiptHandle().get());
        }
        // 3. pop 3 message
        popResult = messageStore.pop(StoreContext.EMPTY, CONSUMER_GROUP_ID, TOPIC_ID, QUEUE_ID, Filter.DEFAULT_FILTER, 3, true, false, invisibleDuration).join();
        assertEquals(PopResult.Status.LOCKED, popResult.status());

        // 4. ack msg_1
        messageStore.ack(receiptHandles.get(1)).join();

        // 5. pop again
        popResult = messageStore.pop(StoreContext.EMPTY, CONSUMER_GROUP_ID, TOPIC_ID, QUEUE_ID, Filter.DEFAULT_FILTER, 3, true, false, invisibleDuration).join();
        assertEquals(PopResult.Status.LOCKED, popResult.status());

        // 6. ack msg_0
        messageStore.ack(receiptHandles.get(0)).join();

        // 7. pop again
        popResult = messageStore.pop(StoreContext.EMPTY, CONSUMER_GROUP_ID, TOPIC_ID, QUEUE_ID, Filter.DEFAULT_FILTER, 3, true, false, invisibleDuration).join();
        assertEquals(PopResult.Status.LOCKED, popResult.status());

        // 8. ack msg_2
        messageStore.ack(receiptHandles.get(2)).join();

        // 9. pop again
        long reviveTimestamp = System.currentTimeMillis() + invisibleDuration;
        popResult = messageStore.pop(StoreContext.EMPTY, CONSUMER_GROUP_ID, TOPIC_ID, QUEUE_ID, Filter.DEFAULT_FILTER, 3, true, false, invisibleDuration).join();
        assertEquals(PopResult.Status.FOUND, popResult.status());
        assertEquals(2, popResult.messageList().size());
        assertEquals(3, popResult.messageList().get(0).offset());
        assertEquals(4, popResult.messageList().get(1).offset());
        for (int i = 0; i < 2; i++) {
            Optional<String> receiptHandle = popResult.messageList().get(i).receiptHandle();
            assertTrue(receiptHandle.isPresent());
            receiptHandles.add(receiptHandle.get());
        }

        // 10. pop again
        popResult = messageStore.pop(StoreContext.EMPTY, CONSUMER_GROUP_ID, TOPIC_ID, QUEUE_ID, Filter.DEFAULT_FILTER, 3, true, false, invisibleDuration).join();
        assertEquals(PopResult.Status.LOCKED, popResult.status());

        // 11. after 1100ms, pop again
        await().atMost(2, TimeUnit.SECONDS)
            .until(() -> reviveService.reviveTimestamp() >= reviveTimestamp && reviveService.inflightReviveCount() == 0);

        popResult = messageStore.pop(StoreContext.EMPTY, CONSUMER_GROUP_ID, TOPIC_ID, QUEUE_ID, Filter.DEFAULT_FILTER, 3, true, false, invisibleDuration).join();
        assertEquals(PopResult.Status.FOUND, popResult.status());
        assertEquals(2, popResult.messageList().size());
        assertEquals(3, popResult.messageList().get(0).offset());
        assertEquals(2, popResult.messageList().get(0).deliveryAttempts());
        assertEquals(4, popResult.messageList().get(1).offset());
        assertEquals(2, popResult.messageList().get(1).deliveryAttempts());
    }

    @Test
    public void pop_snapshot() {
        // set snapshot interval to 7
        config.setOperationSnapshotInterval(7);
        // 1. append 5 message
        for (int i = 0; i < 5; i++) {
            FlatMessage message = FlatMessage.getRootAsFlatMessage(buildMessage(TOPIC_ID, QUEUE_ID, "TagA"));
            messageStore.put(StoreContext.EMPTY, message).join();
        }
        List<String> receiptHandles = new ArrayList<>();
        // 2. pop 3 message
        int invisibleDuration = 100;
        PopResult popResult = messageStore.pop(StoreContext.EMPTY, CONSUMER_GROUP_ID, TOPIC_ID, QUEUE_ID, Filter.DEFAULT_FILTER, 3, false, false, invisibleDuration).join();
        assertEquals(3, popResult.messageList().size());
        for (FlatMessageExt message : popResult.messageList()) {
            assertTrue(message.receiptHandle().isPresent());
            receiptHandles.add(message.receiptHandle().get());
        }

        // 3. ack msg_1, msg_2
        messageStore.ack(receiptHandles.get(1)).join();
        messageStore.ack(receiptHandles.get(2)).join();

        // 4. pop 3 message
        long reviveTimestamp = System.currentTimeMillis() + invisibleDuration;
        popResult = messageStore.pop(StoreContext.EMPTY, CONSUMER_GROUP_ID, TOPIC_ID, QUEUE_ID, Filter.DEFAULT_FILTER, 3, false, false, invisibleDuration).join();
        assertEquals(2, popResult.messageList().size());
        for (FlatMessageExt message : popResult.messageList()) {
            assertTrue(message.receiptHandle().isPresent());
            receiptHandles.add(message.receiptHandle().get());
        }

        // 5. pop again
        popResult = messageStore.pop(StoreContext.EMPTY, CONSUMER_GROUP_ID, TOPIC_ID, QUEUE_ID, Filter.DEFAULT_FILTER, 3, false, false, invisibleDuration).join();
        assertEquals(PopResult.Status.END_OF_QUEUE, popResult.status());

        popResult = messageStore.pop(StoreContext.EMPTY, CONSUMER_GROUP_ID, TOPIC_ID, QUEUE_ID, Filter.DEFAULT_FILTER, 3, false, true, invisibleDuration).join();
        assertEquals(PopResult.Status.END_OF_QUEUE, popResult.status());

        // 6. after 500ms, check if snapshot is taken
        StreamMetadata opStream = metadataService.operationStreamOf(TOPIC_ID, QUEUE_ID).join();
        await().until(() -> streamStore.startOffset(opStream.getStreamId()) == 7);

        StreamMetadata snapshotStream = metadataService.snapshotStreamOf(TOPIC_ID, QUEUE_ID).join();
        assertEquals(0, streamStore.startOffset(snapshotStream.getStreamId()));
        assertEquals(1, streamStore.nextOffset(snapshotStream.getStreamId()));

        // 6. after 1100ms, pop again
        await().until(() -> reviveService.reviveTimestamp() >= reviveTimestamp);

        popResult = messageStore.pop(StoreContext.EMPTY, CONSUMER_GROUP_ID, TOPIC_ID, QUEUE_ID, Filter.DEFAULT_FILTER, 3, false, false, invisibleDuration).join();
        assertEquals(PopResult.Status.END_OF_QUEUE, popResult.status());
        popResult = messageStore.pop(StoreContext.EMPTY, CONSUMER_GROUP_ID, TOPIC_ID, QUEUE_ID, Filter.DEFAULT_FILTER, 3, false, true, invisibleDuration).join();
        assertEquals(PopResult.Status.FOUND, popResult.status());
        assertEquals(3, popResult.messageList().size());
        assertEquals(0, popResult.messageList().get(0).offset());
        assertEquals(0, popResult.messageList().get(0).originalOffset());
        assertEquals(2, popResult.messageList().get(0).deliveryAttempts());
        assertEquals(1, popResult.messageList().get(1).offset());
        assertEquals(3, popResult.messageList().get(1).originalOffset());
        assertEquals(2, popResult.messageList().get(1).deliveryAttempts());
        assertEquals(2, popResult.messageList().get(2).offset());
        assertEquals(4, popResult.messageList().get(2).originalOffset());
        assertEquals(2, popResult.messageList().get(2).deliveryAttempts());

        LogicQueue logicQueue = logicQueueManager.getOrCreate(StoreContext.EMPTY, TOPIC_ID, QUEUE_ID).join();

        assertEquals(5, logicQueue.getConsumeOffset(CONSUMER_GROUP_ID));
        assertEquals(5, logicQueue.getAckOffset(CONSUMER_GROUP_ID));
        assertEquals(3, logicQueue.getRetryConsumeOffset(CONSUMER_GROUP_ID));
        assertEquals(0, logicQueue.getRetryAckOffset(CONSUMER_GROUP_ID));

        // 7. ack msg_0, msg_3, msg_4
        assertTrue(popResult.messageList().get(0).receiptHandle().isPresent());
        messageStore.ack(popResult.messageList().get(0).receiptHandle().get()).join();

        assertTrue(popResult.messageList().get(1).receiptHandle().isPresent());
        messageStore.ack(popResult.messageList().get(1).receiptHandle().get()).join();

        assertTrue(popResult.messageList().get(2).receiptHandle().isPresent());
        messageStore.ack(popResult.messageList().get(2).receiptHandle().get()).join();

        assertEquals(3, logicQueue.getRetryAckOffset(CONSUMER_GROUP_ID));
    }

    @Test
    public void recover_pop_snapshot() throws Exception {
        // set snapshot interval to 7
        config.setOperationSnapshotInterval(7);
        // 1. append 5 message
        for (int i = 0; i < 5; i++) {
            FlatMessage message = FlatMessage.getRootAsFlatMessage(buildMessage(TOPIC_ID, QUEUE_ID, "TagA"));
            messageStore.put(StoreContext.EMPTY, message).join();
        }
        List<String> receiptHandles = new ArrayList<>();
        // 2. pop 3 message
        PopResult popResult = messageStore.pop(StoreContext.EMPTY, CONSUMER_GROUP_ID, TOPIC_ID, QUEUE_ID, Filter.DEFAULT_FILTER, 3, false, false, 800).join();
        assertEquals(3, popResult.messageList().size());
        for (FlatMessageExt message : popResult.messageList()) {
            assertTrue(message.receiptHandle().isPresent());
            receiptHandles.add(message.receiptHandle().get());
        }

        // 3. ack msg_1, msg_2
        messageStore.ack(receiptHandles.get(1)).join();
        messageStore.ack(receiptHandles.get(2)).join();

        // 4. pop 3 message
        popResult = messageStore.pop(StoreContext.EMPTY, CONSUMER_GROUP_ID, TOPIC_ID, QUEUE_ID, Filter.DEFAULT_FILTER, 3, false, false, 800).join();
        assertEquals(2, popResult.messageList().size());
        for (FlatMessageExt message : popResult.messageList()) {
            assertTrue(message.receiptHandle().isPresent());
            receiptHandles.add(message.receiptHandle().get());
        }

        // 5. pop again
        popResult = messageStore.pop(StoreContext.EMPTY, CONSUMER_GROUP_ID, TOPIC_ID, QUEUE_ID, Filter.DEFAULT_FILTER, 3, false, false, 800).join();
        assertEquals(PopResult.Status.END_OF_QUEUE, popResult.status());
        popResult = messageStore.pop(StoreContext.EMPTY, CONSUMER_GROUP_ID, TOPIC_ID, QUEUE_ID, Filter.DEFAULT_FILTER, 3, false, true, 800).join();
        assertEquals(PopResult.Status.END_OF_QUEUE, popResult.status());

        // 6. wait for the snapshot to be taken
        StreamMetadata opStream = metadataService.operationStreamOf(TOPIC_ID, QUEUE_ID).join();
        await().until(() -> streamStore.startOffset(opStream.getStreamId()) == 7);

        StreamMetadata snapshotStream = metadataService.snapshotStreamOf(TOPIC_ID, QUEUE_ID).join();
        assertEquals(0, streamStore.startOffset(snapshotStream.getStreamId()));
        assertEquals(1, streamStore.nextOffset(snapshotStream.getStreamId()));
        // 7. close and reopen
        logicQueueManager.close(TOPIC_ID, QUEUE_ID).join();

        // check if all tq related data is cleared
        byte[] tqPrefix = ByteBuffer.allocate(12)
            .putLong(TOPIC_ID)
            .putInt(QUEUE_ID)
            .array();
        kvService.iterate(MessageStoreImpl.KV_NAMESPACE_CHECK_POINT, tqPrefix, null, null,
            (key, value) -> Assertions.fail("check point should be cleared"));

        // check if all ck have been recovered
        {
            final LogicQueue logicQueue = logicQueueManager.getOrCreate(StoreContext.EMPTY, TOPIC_ID, QUEUE_ID).join();
            Assertions.assertEquals(logicQueue.getState(), LogicQueue.State.OPENED);
            AtomicInteger ckNum = new AtomicInteger();
            kvService.iterate(MessageStoreImpl.KV_NAMESPACE_CHECK_POINT, tqPrefix, null, null,
                (key, value) -> ckNum.getAndIncrement());
            assertEquals(3, ckNum.get());
        }

        // 8. after 1100ms, pop again
        Thread.sleep(1100);
        popResult = messageStore.pop(StoreContext.EMPTY, CONSUMER_GROUP_ID, TOPIC_ID, QUEUE_ID, Filter.DEFAULT_FILTER, 3, false, false, 800).join();
        assertEquals(PopResult.Status.END_OF_QUEUE, popResult.status());
        popResult = messageStore.pop(StoreContext.EMPTY, CONSUMER_GROUP_ID, TOPIC_ID, QUEUE_ID, Filter.DEFAULT_FILTER, 3, false, true, 800).join();
        assertEquals(PopResult.Status.FOUND, popResult.status());
        assertEquals(3, popResult.messageList().size());
        assertEquals(0, popResult.messageList().get(0).offset());
        assertEquals(0, popResult.messageList().get(0).originalOffset());
        assertEquals(2, popResult.messageList().get(0).deliveryAttempts());
        assertEquals(1, popResult.messageList().get(1).offset());
        assertEquals(3, popResult.messageList().get(1).originalOffset());
        assertEquals(2, popResult.messageList().get(1).deliveryAttempts());
        assertEquals(2, popResult.messageList().get(2).offset());
        assertEquals(4, popResult.messageList().get(2).originalOffset());
        assertEquals(2, popResult.messageList().get(2).deliveryAttempts());

        LogicQueue logicQueue = logicQueueManager.getOrCreate(StoreContext.EMPTY, TOPIC_ID, QUEUE_ID).join();

        assertEquals(5, logicQueue.getConsumeOffset(CONSUMER_GROUP_ID));
        assertEquals(5, logicQueue.getAckOffset(CONSUMER_GROUP_ID));
        assertEquals(3, logicQueue.getRetryConsumeOffset(CONSUMER_GROUP_ID));
        assertEquals(0, logicQueue.getRetryAckOffset(CONSUMER_GROUP_ID));

        // 7. ack msg_0, msg_3, msg_4
        assertTrue(popResult.messageList().get(0).receiptHandle().isPresent());
        messageStore.ack(popResult.messageList().get(0).receiptHandle().get()).join();

        assertTrue(popResult.messageList().get(1).receiptHandle().isPresent());
        messageStore.ack(popResult.messageList().get(1).receiptHandle().get()).join();

        assertTrue(popResult.messageList().get(2).receiptHandle().isPresent());
        messageStore.ack(popResult.messageList().get(2).receiptHandle().get()).join();

        assertEquals(3, logicQueue.getRetryAckOffset(CONSUMER_GROUP_ID));
    }

    @Test
    public void recover_all_operation() throws StoreException {
        config.setOperationSnapshotInterval(7);

        // 1. append 4 message
        for (int i = 0; i < 4; i++) {
            FlatMessage message = FlatMessage.getRootAsFlatMessage(buildMessage(TOPIC_ID, QUEUE_ID, "TagA"));
            messageStore.put(StoreContext.EMPTY, message).join();
        }

        List<String> receiptHandles = new ArrayList<>();
        // 2. pop 3 message
        PopResult popResult = messageStore.pop(StoreContext.EMPTY, CONSUMER_GROUP_ID, TOPIC_ID, QUEUE_ID, Filter.DEFAULT_FILTER, 3, false, false, 800).join();
        assertEquals(3, popResult.messageList().size());
        for (FlatMessageExt message : popResult.messageList()) {
            assertTrue(message.receiptHandle().isPresent());
            receiptHandles.add(message.receiptHandle().get());
        }

        // 3. ack them all
        for (String handle : receiptHandles) {
            AckResult ackResult = messageStore.ack(handle).join();
            assertEquals(AckResult.Status.SUCCESS, ackResult.status());
        }
        receiptHandles.clear();

        // 4. pop the last message
        popResult = messageStore.pop(StoreContext.EMPTY, CONSUMER_GROUP_ID, TOPIC_ID, QUEUE_ID, Filter.DEFAULT_FILTER, 3, false, false, 800).join();
        assertEquals(1, popResult.messageList().size());

        // 5. wait for the snapshot to be taken
        StreamMetadata opStream = metadataService.operationStreamOf(TOPIC_ID, QUEUE_ID).join();
        await().until(() -> streamStore.startOffset(opStream.getStreamId()) == 7);

        StreamMetadata snapshotStream = metadataService.snapshotStreamOf(TOPIC_ID, QUEUE_ID).join();
        assertEquals(0, streamStore.startOffset(snapshotStream.getStreamId()));
        assertEquals(1, streamStore.nextOffset(snapshotStream.getStreamId()));

        // 6. rest consume offset to 0
        LogicQueue logicQueue = logicQueueManager.getOrCreate(StoreContext.EMPTY, TOPIC_ID, QUEUE_ID).join();
        ResetConsumeOffsetResult resetConsumeOffsetResult = logicQueue.resetConsumeOffset(CONSUMER_GROUP_ID, 0).join();
        assertEquals(ResetConsumeOffsetResult.Status.SUCCESS, resetConsumeOffsetResult.status());

        long offset = messageStore.getConsumeOffset(CONSUMER_GROUP_ID, TOPIC_ID, QUEUE_ID);
        assertEquals(0, offset);

        // 7. pop 1 message with long invisible duration
        popResult = messageStore.pop(StoreContext.EMPTY, CONSUMER_GROUP_ID, TOPIC_ID, QUEUE_ID, Filter.DEFAULT_FILTER, 1, false, false, Long.MAX_VALUE).join();
        assertEquals(1, popResult.messageList().size());
        FlatMessageExt messageExt = popResult.messageList().get(0);
        assertTrue(messageExt.receiptHandle().isPresent());

        // 8. change invisible duration
        ChangeInvisibleDurationResult changeInvisibleDurationResult = messageStore.changeInvisibleDuration(messageExt.receiptHandle().get(), 1000).join();
        assertEquals(ChangeInvisibleDurationResult.Status.SUCCESS, changeInvisibleDurationResult.status());

        // 8. pop 1 message with short invisible duration
        popResult = messageStore.pop(StoreContext.EMPTY, CONSUMER_GROUP_ID, TOPIC_ID, QUEUE_ID, Filter.DEFAULT_FILTER, 1, false, false, 1000).join();
        assertEquals(1, popResult.messageList().size());
        long nextVisibleTimestamp = System.currentTimeMillis() + 800;

        // check consume offset and retry message count before closing
        offset = messageStore.getConsumeOffset(CONSUMER_GROUP_ID, TOPIC_ID, QUEUE_ID);
        assertEquals(2, offset);

        StreamMetadata retryStream = metadataService.retryStreamOf(CONSUMER_GROUP_ID, TOPIC_ID, QUEUE_ID).join();
        assertFalse(streamStore.isOpened(retryStream.getStreamId()));

        // 9. close the queue
        logicQueueManager.close(TOPIC_ID, QUEUE_ID).join();

        // check if all tq related data is cleared
        byte[] tqPrefix = ByteBuffer.allocate(12)
            .putLong(TOPIC_ID)
            .putInt(QUEUE_ID)
            .array();
        kvService.iterate(MessageStoreImpl.KV_NAMESPACE_CHECK_POINT, tqPrefix, null, null,
            (key, value) -> Assertions.fail("check point should be cleared"));

        // 10. reopen the queue and check if all operations are recovered
        logicQueue = logicQueueManager.getOrCreate(StoreContext.EMPTY, TOPIC_ID, QUEUE_ID).join();
        Assertions.assertEquals(logicQueue.getState(), LogicQueue.State.OPENED);
        AtomicInteger checkpointCount = new AtomicInteger();
        kvService.iterate(MessageStoreImpl.KV_NAMESPACE_CHECK_POINT, tqPrefix, null, null,
            (key, value) -> checkpointCount.getAndIncrement());
        assertEquals(2, checkpointCount.get());

        AtomicInteger timerTagCount = new AtomicInteger();
        kvService.iterate(KV_NAMESPACE_TIMER_TAG + "_tag", (key, value) -> timerTagCount.getAndIncrement());
        assertEquals(2, timerTagCount.get());

        // check consume offset and retry message count after opening
        offset = messageStore.getConsumeOffset(CONSUMER_GROUP_ID, TOPIC_ID, QUEUE_ID);
        assertEquals(2, offset);
        assertFalse(streamStore.isOpened(retryStream.getStreamId()));

        await().atMost(Duration.ofSeconds(5))
            .until(() -> reviveService.reviveTimestamp() >= nextVisibleTimestamp && reviveService.inflightReviveCount() == 0);
        assertTrue(streamStore.isOpened(retryStream.getStreamId()));
        assertEquals(2, streamStore.nextOffset(retryStream.getStreamId()));
    }

    @Test
    public void restart_normal() throws Exception {
        // 1. append 5 message
        for (int i = 0; i < 5; i++) {
            FlatMessage message = FlatMessage.getRootAsFlatMessage(buildMessage(TOPIC_ID, QUEUE_ID, "TagA"));
            messageStore.put(StoreContext.EMPTY, message).join();
        }
        // 2. pop 3 message
        // regard as forever invisible
        int invisibleDuration = 999999999;
        PopResult popResult = messageStore.pop(StoreContext.EMPTY, CONSUMER_GROUP_ID, TOPIC_ID, QUEUE_ID, Filter.DEFAULT_FILTER, 3, true, false, invisibleDuration).join();
        assertEquals(3, popResult.messageList().size());
        for (FlatMessageExt message : popResult.messageList()) {
            assertTrue(message.receiptHandle().isPresent());
        }

        // verify rocksdb is not empty
        assertTrue(verifyStatesExist());

        // 3. normal shutdown
        messageStore.shutdown();

        // verify rocksdb is empty
        assertFalse(verifyStatesExist());

        // 4. restart
        messageStore.start();

        // 5. verify rocksdb is empty
        assertFalse(verifyStatesExist());
    }

    @Test
    public void restart_force() throws Exception {
        // 1. append 5 message
        for (int i = 0; i < 5; i++) {
            FlatMessage message = FlatMessage.getRootAsFlatMessage(buildMessage(TOPIC_ID, QUEUE_ID, "TagA"));
            messageStore.put(StoreContext.EMPTY, message).join();
        }
        // 2. pop 3 message
        // regard as forever invisible
        int invisibleDuration = 999999999;
        PopResult popResult = messageStore.pop(StoreContext.EMPTY, CONSUMER_GROUP_ID, TOPIC_ID, QUEUE_ID, Filter.DEFAULT_FILTER, 3, true, false, invisibleDuration).join();
        assertEquals(3, popResult.messageList().size());
        for (FlatMessageExt message : popResult.messageList()) {
            assertTrue(message.receiptHandle().isPresent());
        }

        // verify rocksdb is not empty
        assertTrue(verifyStatesExist());

        Mockito.doNothing().when(kvService).clear(Mockito.anyString());

        // 3. shutdown but not clean data
        messageStore.shutdown();

        // verify rocksdb is not empty
        assertTrue(verifyStatesExist());

        Mockito.doCallRealMethod().when(kvService).clear(Mockito.anyString());

        // 4. restart
        messageStore.start();

        // 5. verify rocksdb is empty
        assertFalse(verifyStatesExist());
    }

    private boolean verifyStatesExist() {
        AtomicBoolean exist = new AtomicBoolean(false);
        try {
            kvService.iterate(KV_NAMESPACE_TIMER_TAG, (key, value) -> exist.set(true));
            if (exist.get()) {
                return true;
            }
            kvService.iterate(KV_NAMESPACE_CHECK_POINT, (key, value) -> exist.set(true));
            if (exist.get()) {
                return true;
            }
            kvService.iterate(KV_NAMESPACE_FIFO_INDEX, (key, value) -> exist.set(true));
            return exist.get();
        } catch (Exception e) {
            Assertions.fail(e);
            return false;
        }
    }
}
