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
import com.automq.rocketmq.store.api.LogicQueue;
import com.automq.rocketmq.store.api.TopicQueueManager;
import com.automq.rocketmq.store.exception.StoreErrorCode;
import com.automq.rocketmq.store.exception.StoreException;
import com.automq.rocketmq.store.model.generated.CheckPoint;
import com.automq.rocketmq.store.model.generated.ReceiptHandle;
import com.automq.rocketmq.store.model.message.Filter;
import com.automq.rocketmq.store.model.message.PullResult;
import com.automq.rocketmq.store.model.operation.PopOperation;
import com.automq.rocketmq.store.service.api.KVService;
import com.automq.rocketmq.store.util.SerializeUtil;
import com.automq.stream.utils.FutureUtil;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReviveService implements Runnable, Lifecycle {
    private static final Logger LOGGER = LoggerFactory.getLogger(ReviveService.class);
    private Thread thread;
    private final AtomicBoolean started = new AtomicBoolean(false);

    protected volatile boolean stopped = false;

    private final String checkPointNamespace;
    private final String timerTagNamespace;
    private final KVService kvService;
    private final StoreMetadataService metadataService;
    private final InflightService inflightService;
    private final TopicQueueManager topicQueueManager;
    // Indicate the timestamp that the revive service has reached.
    private long reviveTimestamp = 0;
    private final String identity = "[ReviveService]";

    public ReviveService(String checkPointNamespace, String timerTagNamespace, KVService kvService,
        StoreMetadataService metadataService, InflightService inflightService,
        TopicQueueManager topicQueueManager) {
        this.checkPointNamespace = checkPointNamespace;
        this.timerTagNamespace = timerTagNamespace;
        this.kvService = kvService;
        this.metadataService = metadataService;
        this.inflightService = inflightService;
        this.topicQueueManager = topicQueueManager;
    }

    public String getServiceName() {
        return "ReviveService";
    }

    @Override
    public void start() {
        if (!started.compareAndSet(false, true)) {
            return;
        }
        stopped = false;
        this.thread = new Thread(this, getServiceName());
        this.thread.setDaemon(true);
        this.thread.start();
    }

    @Override
    public void shutdown() {
        this.stopped = true;
        this.thread.interrupt();
    }

    @Override
    public void run() {
        while (!stopped) {
            try {
                tryRevive();
                Thread.sleep(1000);
            } catch (InterruptedException ignored) {
            } catch (Exception e) {
                Throwable cause = FutureUtil.cause(e);
                LOGGER.error("{}: Failed to revive", identity, cause);
            }
        }
    }

    protected void tryRevive() throws StoreException {
        byte[] start = ByteBuffer.allocate(8).putLong(0).array();
        long endTimestamp = System.nanoTime() - 1;
        byte[] end = ByteBuffer.allocate(8).putLong(endTimestamp).array();

        // Iterate timer tag until now to find messages need to reconsume.
        kvService.iterate(timerTagNamespace, null, start, end, (key, value) -> {
            // Fetch the origin message from stream store.
            // TODO: async revive retry message
            ReceiptHandle receiptHandle = ReceiptHandle.getRootAsReceiptHandle(ByteBuffer.wrap(value));
            long consumerGroupId = receiptHandle.consumerGroupId();
            long topicId = receiptHandle.topicId();
            int queueId = receiptHandle.queueId();
            long operationId = receiptHandle.operationId();
            byte[] ckKey = SerializeUtil.buildCheckPointKey(topicId, queueId, operationId);
            try {
                byte[] ckValue = kvService.get(checkPointNamespace, ckKey);
                if (ckValue == null) {
                    throw new StoreException(StoreErrorCode.ILLEGAL_ARGUMENT, "Not found check point");
                }
                CheckPoint checkPoint = SerializeUtil.decodeCheckPoint(ByteBuffer.wrap(ckValue));
                PopOperation.PopOperationType operationType = PopOperation.PopOperationType.valueOf(checkPoint.popOperationType());
                LogicQueue logicQueue = topicQueueManager.getOrCreate(topicId, queueId).join();

                // TODO: async
                PullResult pullResult;
                if (operationType == PopOperation.PopOperationType.POP_RETRY) {
                    pullResult = logicQueue.pullRetry(consumerGroupId, Filter.DEFAULT_FILTER, checkPoint.messageOffset(), 1).join();
                } else {
                    pullResult = logicQueue.pullNormal(consumerGroupId, Filter.DEFAULT_FILTER, checkPoint.messageOffset(), 1).join();
                }
                assert pullResult.messageList().size() <= 1;
                // Message has already been deleted.
                if (pullResult.messageList().isEmpty()) {
                    throw new StoreException(StoreErrorCode.ILLEGAL_ARGUMENT, "Not found need retry message");
                }
                // Build the retry message and append it to retry stream or dead letter stream.
                FlatMessageExt messageExt = pullResult.messageList().get(0);
                messageExt.setOriginalQueueOffset(messageExt.originalOffset());
                messageExt.setDeliveryAttempts(messageExt.deliveryAttempts() + 1);
                if (operationType != PopOperation.PopOperationType.POP_ORDER) {
                    if (messageExt.deliveryAttempts() <= metadataService.maxDeliveryAttemptsOf(consumerGroupId).join()) {
                        logicQueue.putRetry(consumerGroupId, messageExt.message()).join();
                    } else {
                        // TODO: dead letter
                    }
                }

                logicQueue.ackTimeout(SerializeUtil.encodeReceiptHandle(receiptHandle)).join();
            } catch (Exception e) {
                Throwable cause = FutureUtil.cause(e);
                LOGGER.error("{}: Failed to revive message", identity, cause);
            }
        });
        this.reviveTimestamp = endTimestamp;
    }

    public long reviveTimestamp() {
        return reviveTimestamp;
    }
}
