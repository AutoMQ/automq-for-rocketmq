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

package com.automq.rocketmq.store.service;

import com.automq.rocketmq.common.model.FlatMessageExt;
import com.automq.rocketmq.common.trace.TraceHelper;
import com.automq.rocketmq.metadata.api.StoreMetadataService;
import com.automq.rocketmq.store.api.DeadLetterSender;
import com.automq.rocketmq.store.api.LogicQueue;
import com.automq.rocketmq.store.api.LogicQueueManager;
import com.automq.rocketmq.store.api.MessageArrivalListener;
import com.automq.rocketmq.store.exception.StoreErrorCode;
import com.automq.rocketmq.store.exception.StoreException;
import com.automq.rocketmq.store.model.StoreContext;
import com.automq.rocketmq.store.model.generated.CheckPoint;
import com.automq.rocketmq.store.model.generated.ReceiptHandle;
import com.automq.rocketmq.store.model.generated.TimerHandlerType;
import com.automq.rocketmq.store.model.generated.TimerTag;
import com.automq.rocketmq.store.model.message.AckResult;
import com.automq.rocketmq.store.model.message.Filter;
import com.automq.rocketmq.store.model.message.PullResult;
import com.automq.rocketmq.store.model.operation.PopOperation;
import com.automq.rocketmq.store.service.api.KVService;
import com.automq.rocketmq.store.util.SerializeUtil;
import com.automq.stream.utils.FutureUtil;
import com.automq.stream.utils.ThreadUtils;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.Tracer;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReviveService {
    private static final Logger LOGGER = LoggerFactory.getLogger(ReviveService.class);
    private final String checkPointNamespace;
    private final KVService kvService;
    private final StoreMetadataService metadataService;
    private final MessageArrivalNotificationService messageArrivalNotificationService;
    private final LogicQueueManager logicQueueManager;
    // Indicate the timestamp that the revive service has reached.
    private volatile long reviveTimestamp = 0;
    private final String identity = "[ReviveService]";
    private final ConcurrentMap<Long/*operationId*/, CompletableFuture<Void>> inflightRevive;
    private final ExecutorService backgroundExecutor;
    private final DeadLetterSender deadLetterSender;

    public ReviveService(String checkPointNamespace, KVService kvService, TimerService timerService,
        StoreMetadataService metadataService, MessageArrivalNotificationService messageArrivalNotificationService,
        LogicQueueManager logicQueueManager, DeadLetterSender deadLetterSender) throws StoreException {
        this(checkPointNamespace, kvService, timerService, metadataService, messageArrivalNotificationService,
            logicQueueManager, deadLetterSender, Executors.newFixedThreadPool(1, ThreadUtils.createThreadFactory("revive-service-background", false)));
    }

    public ReviveService(String checkPointNamespace, KVService kvService, TimerService timerService,
        StoreMetadataService metadataService, MessageArrivalNotificationService messageArrivalNotificationService,
        LogicQueueManager logicQueueManager, DeadLetterSender deadLetterSender,
        ExecutorService executorService) throws StoreException {
        this.checkPointNamespace = checkPointNamespace;
        this.kvService = kvService;
        this.metadataService = metadataService;
        this.messageArrivalNotificationService = messageArrivalNotificationService;
        this.logicQueueManager = logicQueueManager;
        this.inflightRevive = new ConcurrentHashMap<>();
        this.deadLetterSender = deadLetterSender;
        this.backgroundExecutor = executorService;

        timerService.registerHandler(TimerHandlerType.POP_REVIVE, this::tryRevive);
    }

    class ReviveTask implements Runnable {
        private final long deliveryTimestamp;
        private final ByteBuffer payload;

        ReviveTask(long deliveryTimestamp, ByteBuffer payload) {
            this.deliveryTimestamp = deliveryTimestamp;
            this.payload = payload;
        }

        @Override
        public void run() {
            Tracer tracer = TraceHelper.getTracer();
            StoreContext context = new StoreContext("", "", tracer);
            Span rootSpan = tracer.spanBuilder("ReviveTask")
                .setNoParent()
                .setSpanKind(SpanKind.INTERNAL)
                .startSpan();
            context.attachSpan(rootSpan);

            ReceiptHandle receiptHandle = ReceiptHandle.getRootAsReceiptHandle(payload);
            long consumerGroupId = receiptHandle.consumerGroupId();
            long topicId = receiptHandle.topicId();
            int queueId = receiptHandle.queueId();
            long operationId = receiptHandle.operationId();
            context.span().ifPresent(s -> {
                s.setAttribute("topicId", topicId);
                s.setAttribute("queueId", queueId);
                s.setAttribute("consumerGroupId", consumerGroupId);
            });

            CompletableFuture<Void> future = CompletableFuture.completedFuture(null);
            CompletableFuture<Void> preFuture = inflightRevive.putIfAbsent(operationId, future);
            if (preFuture != null) {
                LOGGER.trace("{}: Inflight revive operation: {}", identity, operationId);
                return;
            }
            reviveTimestamp = deliveryTimestamp;

            CompletableFuture<Triple<LogicQueue, PullResult, PopOperation.PopOperationType>> fetchMessageFuture =
                future.thenCompose(nil -> logicQueueManager.getOrCreate(StoreContext.EMPTY, topicId, queueId))
                    .thenComposeAsync(queue -> {
                        byte[] ckKey = SerializeUtil.buildCheckPointKey(topicId, queueId, consumerGroupId, operationId);
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
                        context.span().ifPresent(s -> s.setAttribute("operationType", operationType.name()));

                        CompletableFuture<PullResult> pullFuture;
                        if (operationType == PopOperation.PopOperationType.POP_RETRY) {
                            pullFuture = queue.pullRetry(context, consumerGroupId, Filter.DEFAULT_FILTER, checkPoint.messageOffset(), 1);
                        } else {
                            pullFuture = queue.pullNormal(context, consumerGroupId, Filter.DEFAULT_FILTER, checkPoint.messageOffset(), 1);
                        }
                        return pullFuture.thenApply(result -> {
                            if (result.messageList().isEmpty()) {
                                context.span().ifPresent(s -> s.addEvent("message not found"));
                            }
                            return Triple.of(queue, result, operationType);
                        });
                    }, backgroundExecutor);

            AtomicReference<Integer> maxDeliveryAttemptsRef = new AtomicReference<>();
            CompletableFuture<Triple<LogicQueue, FlatMessageExt, PopOperation.PopOperationType>> checkFuture =
                fetchMessageFuture.thenCombineAsync(metadataService.maxDeliveryAttemptsOf(consumerGroupId), (triple, maxDeliveryAttempts) -> {
                    LogicQueue logicQueue = triple.getLeft();
                    PullResult pullResult = triple.getMiddle();
                    PopOperation.PopOperationType operationType = triple.getRight();
                    if (pullResult.messageList().size() != 1) {
                        throw new CompletionException(new StoreException(StoreErrorCode.ILLEGAL_ARGUMENT, "Revive message not found"));
                    }
                    // Build the retry message and append it to retry stream or dead letter stream.
                    FlatMessageExt messageExt = pullResult.messageList().get(0);
                    maxDeliveryAttemptsRef.set(maxDeliveryAttempts);
                    context.span().ifPresent(s -> s.setAttribute("maxDeliveryAttempts", maxDeliveryAttempts));
                    return Triple.of(logicQueue, messageExt, operationType);
                }, backgroundExecutor);

            CompletableFuture<Pair<Boolean, LogicQueue>> resendFuture = checkFuture.thenCompose(triple -> {
                LogicQueue logicQueue = triple.getLeft();
                FlatMessageExt messageExt = triple.getMiddle();
                PopOperation.PopOperationType operationType = triple.getRight();
                int maxDeliveryAttempts = maxDeliveryAttemptsRef.get();

                if (operationType == PopOperation.PopOperationType.POP_ORDER) {
                    int consumeTimes = logicQueue.getConsumeTimes(consumerGroupId, messageExt.offset());
                    context.span().ifPresent(s -> s.setAttribute("deliveryAttempts", consumeTimes));

                    if (consumeTimes >= maxDeliveryAttempts) {
                        messageExt.setDeliveryAttempts(consumeTimes);
                        // Send to dead letter topic specified in consumer group config.
                        return deadLetterSender.send(context, consumerGroupId, messageExt.message())
                            .thenApply(nil -> Pair.of(true, logicQueue));
                    }
                    return CompletableFuture.completedFuture(Pair.of(false, logicQueue));
                }

                context.span().ifPresent(s -> s.setAttribute("deliveryAttempts", messageExt.deliveryAttempts()));

                if (messageExt.deliveryAttempts() >= maxDeliveryAttempts) {
                    // Send to dead letter topic specified in consumer group config.
                    return deadLetterSender.send(context, consumerGroupId, messageExt.message())
                        .thenApply(nil -> Pair.of(true, logicQueue));
                }
                messageExt.setOriginalQueueOffset(messageExt.originalOffset());
                messageExt.setDeliveryAttempts(messageExt.deliveryAttempts() + 1);
                return logicQueue.putRetry(context, consumerGroupId, messageExt.message())
                    .thenCompose(result -> metadataService.topicOf(topicId)
                        .thenAccept(topic -> messageArrivalNotificationService.notify(MessageArrivalListener.MessageSource.RETRY_MESSAGE_PUT, topic, queueId, result.offset(), messageExt.message().tag())))
                    .thenApply(nil -> Pair.of(false, logicQueue));
            });

            CompletableFuture<AckResult> ackFuture = resendFuture.thenComposeAsync(pair -> {
                boolean sendDeadLetter = pair.getLeft();
                LogicQueue queue = pair.getRight();
                if (sendDeadLetter) {
                    // regard sending to DLQ as ack
                    return queue.ack(SerializeUtil.encodeReceiptHandle(receiptHandle));
                }
                // ack timeout
                return queue.ackTimeout(SerializeUtil.encodeReceiptHandle(receiptHandle));
            }, backgroundExecutor);

            ackFuture.whenComplete((nil, e) -> {
                inflightRevive.remove(operationId);
                TraceHelper.endSpan(context, rootSpan, e);

                if (e != null) {
                    Throwable cause = FutureUtil.cause(e);
                    if (cause instanceof StoreException storeException) {
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
                        return;
                    }
                    LOGGER.error("{}: Failed to revive ck with operationId: {}", identity, operationId, cause);
                }
            });
        }
    }

    protected void tryRevive(TimerTag timerTag) {
        backgroundExecutor.execute(new ReviveTask(timerTag.deliveryTimestamp(), timerTag.payloadAsByteBuffer()));
    }

    public long reviveTimestamp() {
        return reviveTimestamp;
    }

    public int inflightReviveCount() {
        return inflightRevive.size();
    }
}
