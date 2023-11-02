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

package com.automq.rocketmq.store.queue;

import apache.rocketmq.common.v1.Code;
import com.automq.rocketmq.common.config.StoreConfig;
import com.automq.rocketmq.common.exception.ControllerException;
import com.automq.rocketmq.metadata.api.StoreMetadataService;
import com.automq.rocketmq.store.api.LogicQueue;
import com.automq.rocketmq.store.api.LogicQueueManager;
import com.automq.rocketmq.store.api.MessageStateMachine;
import com.automq.rocketmq.store.api.StreamStore;
import com.automq.rocketmq.store.exception.StoreErrorCode;
import com.automq.rocketmq.store.exception.StoreException;
import com.automq.rocketmq.store.model.StoreContext;
import com.automq.rocketmq.store.model.message.TopicQueueId;
import com.automq.rocketmq.store.service.InflightService;
import com.automq.rocketmq.store.service.StreamReclaimService;
import com.automq.rocketmq.store.service.TimerService;
import com.automq.rocketmq.store.service.api.KVService;
import com.automq.rocketmq.store.service.api.OperationLogService;
import com.automq.stream.utils.FutureUtil;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.instrumentation.annotations.SpanAttribute;
import io.opentelemetry.instrumentation.annotations.WithSpan;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultLogicQueueManager implements LogicQueueManager {
    protected static final Logger LOGGER = LoggerFactory.getLogger(DefaultLogicQueueManager.class);

    private final StoreConfig storeConfig;
    private final StreamStore streamStore;
    private final KVService kvService;
    private final TimerService timerService;
    private final StoreMetadataService metadataService;
    private final OperationLogService operationLogService;
    private final InflightService inflightService;
    private final StreamReclaimService streamReclaimService;
    private final ConcurrentMap<TopicQueueId, CompletableFuture<LogicQueue>> logicQueueMap;
    private final String identity = "[DefaultLogicQueueManager]";

    public DefaultLogicQueueManager(StoreConfig storeConfig, StreamStore streamStore,
        KVService kvService, TimerService timerService, StoreMetadataService metadataService,
        OperationLogService operationLogService,
        InflightService inflightService, StreamReclaimService streamReclaimService) {
        this.storeConfig = storeConfig;
        this.streamStore = streamStore;
        this.kvService = kvService;
        this.timerService = timerService;
        this.metadataService = metadataService;
        this.operationLogService = operationLogService;
        this.inflightService = inflightService;
        this.streamReclaimService = streamReclaimService;
        this.logicQueueMap = new ConcurrentHashMap<>();
    }

    @Override
    public void start() throws Exception {

    }

    @Override
    public void shutdown() throws Exception {

    }

    public ConcurrentMap<TopicQueueId, CompletableFuture<LogicQueue>> logicQueueMap() {
        return logicQueueMap;
    }

    public int size() {
        return logicQueueMap.size();
    }

    @Override
    @WithSpan(kind = SpanKind.SERVER)
    public CompletableFuture<LogicQueue> getOrCreate(StoreContext context, @SpanAttribute long topicId,
        @SpanAttribute int queueId) {
        Optional<Integer> ownerNode = metadataService.ownerNode(topicId, queueId);
        if (ownerNode.isEmpty()) {
            return CompletableFuture.failedFuture(new StoreException(StoreErrorCode.QUEUE_NOT_FOUND, "This node does not own the requested queue"));
        } else {
            Integer ownerNodeId = ownerNode.get();
            if (ownerNodeId != metadataService.nodeConfig().nodeId()) {
                return CompletableFuture.failedFuture(new StoreException(StoreErrorCode.QUEUE_NOT_FOUND, "This node does not own the requested queue"));
            }
        }

        TopicQueueId key = new TopicQueueId(topicId, queueId);
        CompletableFuture<LogicQueue> future = logicQueueMap.get(key);

        if (future != null) {
            context.span().ifPresent(span -> span.setAttribute("needCreate", false));
            return future;
        }

        // Prevent concurrent create.
        synchronized (this) {
            future = logicQueueMap.get(key);
            if (future != null) {
                context.span().ifPresent(span -> span.setAttribute("needCreate", false));
                return future;
            }

            // Create and open the topic queue.
            context.span().ifPresent(span -> span.setAttribute("needCreate", true));
            future = createAndOpen(topicId, queueId);
            logicQueueMap.put(key, future);
            future.exceptionally(ex -> {
                Throwable cause = FutureUtil.cause(ex);
                LOGGER.error("{}: Create logic queue failed: topic: {} queue: {}", identity, topicId, queueId, cause);
                logicQueueMap.remove(key);
                return null;
            });
            return future;
        }
    }

    @Override
    public CompletableFuture<Optional<LogicQueue>> get(long topicId, int queueId) {
        TopicQueueId key = new TopicQueueId(topicId, queueId);
        CompletableFuture<LogicQueue> future = logicQueueMap.get(key);
        if (future != null) {
            return future.thenApply(Optional::of);
        }
        return CompletableFuture.completedFuture(Optional.empty());
    }

    @Override
    public CompletableFuture<Void> close(long topicId, int queueId) {
        LOGGER.info("{}: Close logic queue: {} queue: {}", identity, topicId, queueId);
        TopicQueueId key = new TopicQueueId(topicId, queueId);
        CompletableFuture<LogicQueue> future = logicQueueMap.remove(key);
        if (future != null) {
            return future.thenCompose(LogicQueue::close);
        }
        return CompletableFuture.completedFuture(null);
    }

    public CompletableFuture<LogicQueue> createAndOpen(long topicId, int queueId) {
        TopicQueueId id = new TopicQueueId(topicId, queueId);
        if (logicQueueMap.containsKey(id)) {
            return CompletableFuture.completedFuture(null);
        }

        MessageStateMachine stateMachine = new DefaultLogicQueueStateMachine(topicId, queueId, kvService, timerService);
        LogicQueue logicQueue = new StreamLogicQueue(storeConfig, topicId, queueId,
            metadataService, stateMachine, streamStore, operationLogService, inflightService, streamReclaimService);

        LOGGER.info("{}: Create and open logic queue success: topic: {} queue: {}", identity, topicId, queueId);
        return logicQueue.open()
            .thenApply(nil -> logicQueue)
            .exceptionally(ex -> {
                Throwable cause = FutureUtil.cause(ex);
                LOGGER.error("{}: Open logic queue failed: topic: {} queue: {}", identity, topicId, queueId, cause);
                if (cause instanceof ControllerException controllerException) {
                    switch (controllerException.getErrorCode()) {
                        case Code.FENCED_VALUE ->
                            throw new CompletionException(new StoreException(StoreErrorCode.QUEUE_FENCED, cause.getMessage()));
                        case Code.NOT_FOUND_VALUE ->
                            throw new CompletionException(new StoreException(StoreErrorCode.QUEUE_NOT_FOUND, cause.getMessage()));
                        default ->
                            throw new CompletionException(new StoreException(StoreErrorCode.INNER_ERROR, cause.getMessage()));
                    }
                } else if (cause instanceof StoreException) {
                    throw new CompletionException(cause);
                } else {
                    throw new CompletionException(new StoreException(StoreErrorCode.INNER_ERROR, cause.getMessage()));
                }
            });
    }

}
