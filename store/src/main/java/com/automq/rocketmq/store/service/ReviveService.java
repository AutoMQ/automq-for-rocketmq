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

import com.automq.rocketmq.common.model.FlatMessageExt;
import com.automq.rocketmq.common.util.Lifecycle;
import com.automq.rocketmq.metadata.api.StoreMetadataService;
import com.automq.rocketmq.store.api.DLQSender;
import com.automq.rocketmq.store.api.LogicQueue;
import com.automq.rocketmq.store.api.LogicQueueManager;
import com.automq.rocketmq.store.exception.StoreErrorCode;
import com.automq.rocketmq.store.exception.StoreException;
import com.automq.rocketmq.store.model.generated.CheckPoint;
import com.automq.rocketmq.store.model.generated.ReceiptHandle;
import com.automq.rocketmq.store.model.message.AckResult;
import com.automq.rocketmq.store.model.message.Filter;
import com.automq.rocketmq.store.model.message.PullResult;
import com.automq.rocketmq.store.model.message.TopicQueueId;
import com.automq.rocketmq.store.model.operation.PopOperation;
import com.automq.rocketmq.store.service.api.KVService;
import com.automq.rocketmq.store.util.SerializeUtil;
import com.automq.stream.utils.FutureUtil;
import com.automq.stream.utils.ThreadUtils;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.automq.rocketmq.store.exception.StoreErrorCode.QUEUE_NOT_OPENED;
import static com.automq.rocketmq.store.exception.StoreErrorCode.QUEUE_OPENING;

public class ReviveService implements Runnable, Lifecycle {
    private static final Logger LOGGER = LoggerFactory.getLogger(ReviveService.class);
    private final AtomicBoolean started = new AtomicBoolean(false);
    private final String checkPointNamespace;
    private final String timerTagNamespace;
    private final KVService kvService;
    private final StoreMetadataService metadataService;
    private final InflightService inflightService;
    private final LogicQueueManager logicQueueManager;
    // Indicate the timestamp that the revive service has reached.
    private volatile long reviveTimestamp = 0;
    private final String identity = "[ReviveService]";
    private final ConcurrentMap<Long/*operationId*/, CompletableFuture<Void>> inflightRevive;
    private ScheduledExecutorService mainExecutor;
    private ExecutorService backgroundExecutor;
    private final DLQSender dlqSender;

    public ReviveService(String checkPointNamespace, String timerTagNamespace, KVService kvService,
        StoreMetadataService metadataService, InflightService inflightService,
        LogicQueueManager logicQueueManager, DLQSender dlqSender) {
        this.checkPointNamespace = checkPointNamespace;
        this.timerTagNamespace = timerTagNamespace;
        this.kvService = kvService;
        this.metadataService = metadataService;
        this.inflightService = inflightService;
        this.logicQueueManager = logicQueueManager;
        this.inflightRevive = new ConcurrentHashMap<>();
        this.dlqSender = dlqSender;
        this.backgroundExecutor = Executors.newSingleThreadExecutor(
            ThreadUtils.createThreadFactory("revive-service-background", false));
        this.mainExecutor = Executors.newSingleThreadScheduledExecutor(
            ThreadUtils.createThreadFactory("revive-service-main", false));
    }

    @Override
    public void start() {
        if (!started.compareAndSet(false, true)) {
            return;
        }
        if (this.backgroundExecutor == null || this.backgroundExecutor.isShutdown()) {
            this.backgroundExecutor = Executors.newSingleThreadExecutor(
                ThreadUtils.createThreadFactory("revive-service-background", false));
        }
        if (this.mainExecutor == null || this.mainExecutor.isShutdown()) {
            this.mainExecutor = Executors.newSingleThreadScheduledExecutor(
                ThreadUtils.createThreadFactory("revive-service-main", false));
        }
        this.mainExecutor.scheduleWithFixedDelay(this, 0, 1000, TimeUnit.MILLISECONDS);
    }

    @Override
    public void shutdown() {
        if (!started.compareAndSet(true, false)) {
            return;
        }
        if (this.mainExecutor != null) {
            this.mainExecutor.shutdown();
            this.mainExecutor = null;
        }
        if (this.backgroundExecutor != null) {
            this.backgroundExecutor.shutdown();
            this.backgroundExecutor = null;
        }
    }

    @Override
    public void run() {
        try {
            tryRevive();
        } catch (Exception e) {
            Throwable cause = FutureUtil.cause(e);
            LOGGER.error("{}: Failed to revive", identity, cause);
        }
    }

    protected void tryRevive() throws StoreException {
        byte[] start = ByteBuffer.allocate(8).putLong(0).array();
        long endTimestamp = System.currentTimeMillis() - 1;
        byte[] end = ByteBuffer.allocate(8).putLong(endTimestamp).array();

        // Iterate timer tag until now to find messages need to reconsume.
        kvService.iterate(timerTagNamespace, null, start, end, (key, value) -> {
            // Fetch the origin message from stream store.
            ReceiptHandle receiptHandle = ReceiptHandle.getRootAsReceiptHandle(ByteBuffer.wrap(value));
            long consumerGroupId = receiptHandle.consumerGroupId();
            long topicId = receiptHandle.topicId();
            int queueId = receiptHandle.queueId();
            long operationId = receiptHandle.operationId();
            byte[] ckKey = SerializeUtil.buildCheckPointKey(topicId, queueId, operationId);
            CompletableFuture<Void> cf = CompletableFuture.completedFuture(null);
            CompletableFuture<Void> preCf = inflightRevive.putIfAbsent(operationId, cf);
            if (preCf != null) {
                LOGGER.trace("{}: Inflight revive operation: {}", identity, operationId);
                return;
            }

            CompletableFuture<Triple<LogicQueue, PullResult, PopOperation.PopOperationType>> pullMsgCf = cf.thenCompose(nil -> logicQueueManager.getOrCreate(topicId, queueId))
                .thenComposeAsync(queue -> {
                    LogicQueue.State queueState = queue.getState();
                    if (queueState == LogicQueue.State.OPENING) {
                        // queue is opening, ignore its revive operation
                        LOGGER.info("{}: Queue: {} is opening, ignore revive operation: {}", identity, TopicQueueId.of(topicId, queueId), operationId);
                        throw new CompletionException(new StoreException(QUEUE_OPENING, "Queue is opening"));
                    }
                    if (queueState != LogicQueue.State.OPENED) {
                        throw new CompletionException(new StoreException(QUEUE_NOT_OPENED, "Queue is not opened"));
                    }
                    byte[] ckValue;
                    try {
                        ckValue = kvService.get(checkPointNamespace, ckKey);
                    } catch (StoreException e) {
                        LOGGER.error("{}: Failed to get check point with topicId: {}, queueId: {}, operationId: {}", identity, topicId, queueId, operationId, e);
                        throw new CompletionException(e);
                    }
                    if (ckValue == null) {
                        LOGGER.error("{}: Not found check point with topicId: {}, queueId: {}, operationId: {}", identity, topicId, queueId, operationId);
                        throw new CompletionException(new StoreException(StoreErrorCode.ILLEGAL_ARGUMENT, "Not found check point"));
                    }
                    CheckPoint checkPoint = SerializeUtil.decodeCheckPoint(ByteBuffer.wrap(ckValue));
                    PopOperation.PopOperationType operationType = PopOperation.PopOperationType.valueOf(checkPoint.popOperationType());

                    CompletableFuture<PullResult> pullCf;
                    if (operationType == PopOperation.PopOperationType.POP_RETRY) {
                        pullCf = queue.pullRetry(consumerGroupId, Filter.DEFAULT_FILTER, checkPoint.messageOffset(), 1);
                    } else {
                        pullCf = queue.pullNormal(consumerGroupId, Filter.DEFAULT_FILTER, checkPoint.messageOffset(), 1);
                    }
                    return pullCf.thenApply(result -> Triple.of(queue, result, operationType));
                }, backgroundExecutor);

            CompletableFuture<Triple<LogicQueue, FlatMessageExt, Pair<PopOperation.PopOperationType, Integer>>> checkCf = pullMsgCf.thenCombineAsync(metadataService.maxDeliveryAttemptsOf(consumerGroupId), (pair, maxDeliveryAttempts) -> {
                LogicQueue logicQueue = pair.getLeft();
                PullResult pullResult = pair.getMiddle();
                PopOperation.PopOperationType operationType = pair.getRight();
                if (pullResult.messageList().size() != 1) {
                    throw new CompletionException(new StoreException(StoreErrorCode.ILLEGAL_ARGUMENT, "Revive message not found"));
                }
                // Build the retry message and append it to retry stream or dead letter stream.
                FlatMessageExt messageExt = pullResult.messageList().get(0);
                return Triple.of(logicQueue, messageExt, Pair.of(operationType, maxDeliveryAttempts));
            }, backgroundExecutor);

            CompletableFuture<Pair<Boolean, LogicQueue>> resendCf = checkCf.thenCompose(triple -> {
                LogicQueue logicQueue = triple.getLeft();
                FlatMessageExt messageExt = triple.getMiddle();
                int maxDeliveryAttempts = triple.getRight().getRight();
                PopOperation.PopOperationType operationType = triple.getRight().getLeft();

                if (operationType == PopOperation.PopOperationType.POP_ORDER) {
                    int consumeTimes = logicQueue.getConsumeTimes(consumerGroupId, messageExt.offset());
                    if (consumeTimes >= maxDeliveryAttempts) {
                        messageExt.setDeliveryAttempts(consumeTimes);
                        // send as DLQ
                        return dlqSender.send(consumerGroupId, messageExt)
                            .thenApply(nil -> Pair.of(true, logicQueue));
                    }
                    return CompletableFuture.completedFuture(Pair.of(false, logicQueue));
                }
                if (messageExt.deliveryAttempts() >= maxDeliveryAttempts) {
                    // send as DLQ
                    return dlqSender.send(consumerGroupId, messageExt)
                        .thenApply(nil -> Pair.of(true, logicQueue));
                }
                messageExt.setOriginalQueueOffset(messageExt.originalOffset());
                messageExt.setDeliveryAttempts(messageExt.deliveryAttempts() + 1);
                return logicQueue.putRetry(consumerGroupId, messageExt.message())
                    .thenApply(nil -> Pair.of(false, logicQueue));
            });
            CompletableFuture<AckResult> ackCf = resendCf.thenComposeAsync(pair -> {
                boolean sendDLQ = pair.getLeft();
                LogicQueue queue = pair.getRight();
                if (sendDLQ) {
                    // regard sending to DLQ as ack
                    return queue.ack(SerializeUtil.encodeReceiptHandle(receiptHandle));
                }
                // ack timeout
                return queue.ackTimeout(SerializeUtil.encodeReceiptHandle(receiptHandle));
            }, backgroundExecutor);
            ackCf.thenAcceptAsync(nil -> {
                inflightRevive.remove(operationId);
            }, backgroundExecutor).exceptionally(e -> {
                inflightRevive.remove(operationId);
                Throwable cause = FutureUtil.cause(e);
                if (cause instanceof StoreException) {
                    StoreException storeException = (StoreException) cause;
                    switch (storeException.code()) {
                        case QUEUE_OPENING:
                            // ignore
                            break;
                        case QUEUE_NOT_OPENED:
                            LOGGER.error("{}: Failed to revive ck with operationId: {}, queue not opened", identity, operationId, storeException);
                            break;
                        default:
                            LOGGER.error("{}: Failed to revive ck with operationId: {}", identity, operationId, storeException);
                            break;
                    }
                    return null;
                }
                LOGGER.error("{}: Failed to revive ck with operationId: {}", identity, operationId, cause);
                return null;
            });
        });
        this.reviveTimestamp = endTimestamp;
    }

    public long reviveTimestamp() {
        return reviveTimestamp;
    }

    public int inflightReviveCount() {
        return inflightRevive.size();
    }
}
