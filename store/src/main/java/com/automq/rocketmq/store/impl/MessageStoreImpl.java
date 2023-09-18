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

package com.automq.rocketmq.store.impl;

import com.automq.rocketmq.common.model.generated.Message;
import com.automq.rocketmq.metadata.StoreMetadataService;
import com.automq.rocketmq.store.MessageStore;
import com.automq.rocketmq.store.StreamStore;
import com.automq.rocketmq.store.model.generated.CheckPoint;
import com.automq.rocketmq.store.model.generated.ReceiptHandle;
import com.automq.rocketmq.store.model.kv.BatchDeleteRequest;
import com.automq.rocketmq.store.model.kv.BatchRequest;
import com.automq.rocketmq.store.model.kv.BatchWriteRequest;
import com.automq.rocketmq.store.model.message.AckResult;
import com.automq.rocketmq.store.model.message.ChangeInvisibleDurationResult;
import com.automq.rocketmq.store.model.message.PopResult;
import com.automq.rocketmq.store.service.KVService;
import com.automq.rocketmq.store.service.OperationLogService;
import com.automq.rocketmq.store.service.impl.ReviveService;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.rocksdb.RocksDBException;

import static com.automq.rocketmq.store.util.SerializeUtil.buildCheckPointKey;
import static com.automq.rocketmq.store.util.SerializeUtil.buildCheckPointValue;
import static com.automq.rocketmq.store.util.SerializeUtil.buildOrderIndexKey;
import static com.automq.rocketmq.store.util.SerializeUtil.buildOrderIndexValue;
import static com.automq.rocketmq.store.util.SerializeUtil.buildTimerTagKey;
import static com.automq.rocketmq.store.util.SerializeUtil.buildTimerTagValue;
import static com.automq.rocketmq.store.util.SerializeUtil.decodeReceiptHandle;

public class MessageStoreImpl implements MessageStore {
    protected static final String KV_NAMESPACE_CHECK_POINT = "check_point";
    protected static final String KV_NAMESPACE_TIMER_TAG = "timer_tag";
    protected static final String KV_NAMESPACE_ORDER_INDEX = "order_index";

    private final StreamStore streamStore;

    private final OperationLogService operationLogService;
    private final StoreMetadataService metadataService;
    private final KVService kvService;

    private ReviveService reviveService;

    public MessageStoreImpl(StreamStore streamStore, OperationLogService operationLogService,
        StoreMetadataService metadataService, KVService kvService) {
        this.streamStore = streamStore;
        this.operationLogService = operationLogService;
        this.metadataService = metadataService;
        this.kvService = kvService;
    }

    public void startReviveService() {
        reviveService = new ReviveService(KV_NAMESPACE_CHECK_POINT, KV_NAMESPACE_TIMER_TAG, kvService, metadataService, streamStore);
        reviveService.start();
    }

    @Override
    public CompletableFuture<PopResult> pop(long consumerGroupId, long topicId, int queueId, long offset, int batchSize,
        boolean fifo, boolean retry, long invisibleDuration) {
        if (fifo && retry) {
            return CompletableFuture.failedFuture(new RuntimeException("Fifo and retry cannot be true at the same time"));
        }

        // Write pop operation to operation log.
        // Operation id should be monotonically increasing for each queue
        long operationTimestamp = System.nanoTime();
        CompletableFuture<Long> logOperationFuture = operationLogService.logPopOperation(consumerGroupId, topicId, queueId, offset, batchSize, fifo, invisibleDuration, operationTimestamp);

        long streamId;
        if (retry) {
            streamId = metadataService.getRetryStreamId(consumerGroupId, topicId, queueId);
        } else {
            streamId = metadataService.getStreamId(topicId, queueId);
        }

        CompletableFuture<List<Message>> fetchMessageFuture = streamStore.fetch(streamId, offset, batchSize)
            .thenApply(fetchResult -> {
                // TODO: Assume message count is always 1 in each batch for now.
                return fetchResult.recordBatchList()
                    .stream()
                    .map(batch -> Message.getRootAsMessage(batch.rawPayload()))
                    .toList();
            });

        return logOperationFuture.thenCombine(fetchMessageFuture, (operationId, messageList) -> {
            long nextVisibleTimestamp = operationTimestamp + invisibleDuration;
            // If pop orderly, check whether the message is already consumed.
            Map<Long, CheckPoint> orderCheckPointMap = new HashMap<>();
            if (fifo) {
                for (int i = 0; i < batchSize; i++) {
                    try {
                        // TODO: Undefined behavior if last operation is not orderly.
                        byte[] orderIndexKey = buildOrderIndexKey(consumerGroupId, topicId, queueId, offset + i);
                        byte[] bytes = kvService.get(KV_NAMESPACE_ORDER_INDEX, orderIndexKey);
                        // If order index not found, this message has not been consumed.
                        if (bytes == null) {
                            continue;
                        }
                        long lastOperationId = ByteBuffer.wrap(bytes).getLong();
                        byte[] checkPoint = kvService.get(KV_NAMESPACE_CHECK_POINT, buildCheckPointKey(topicId, queueId, offset + i, lastOperationId));
                        if (checkPoint != null) {
                            orderCheckPointMap.put(offset + i, CheckPoint.getRootAsCheckPoint(ByteBuffer.wrap(checkPoint)));
                        } else {
                            // TODO: log finding a orphan index, this maybe a bug
                            kvService.delete(KV_NAMESPACE_ORDER_INDEX, orderIndexKey);
                        }
                    } catch (RocksDBException e) {
                        // TODO: handle exception
                        throw new RuntimeException(e);
                    }
                }
            }

            // Insert or renew check point and timer tag into KVService.
            for (Message message : messageList) {
                try {
                    // If pop orderly, the message already consumed will not trigger writing new check point.
                    // But reconsume count should be increased.
                    if (fifo && orderCheckPointMap.containsKey(message.offset())) {
                        // Delete last check point and timer tag.
                        CheckPoint lastCheckPoint = orderCheckPointMap.get(message.offset());
                        BatchDeleteRequest deleteLastCheckPointRequest = new BatchDeleteRequest(KV_NAMESPACE_CHECK_POINT,
                            buildCheckPointKey(topicId, queueId, message.offset(), lastCheckPoint.operationId()));

                        BatchDeleteRequest deleteLastTimerTagRequest = new BatchDeleteRequest(KV_NAMESPACE_TIMER_TAG,
                            buildTimerTagKey(lastCheckPoint.nextVisibleTimestamp(), topicId, queueId, message.offset(), lastCheckPoint.operationId()));

                        // Write new check point, timer tag, and order index.
                        BatchWriteRequest writeCheckPointRequest = new BatchWriteRequest(KV_NAMESPACE_CHECK_POINT,
                            buildCheckPointKey(topicId, queueId, message.offset(), operationId),
                            buildCheckPointValue(topicId, queueId, message.offset(), consumerGroupId, operationId, true, false, operationTimestamp, nextVisibleTimestamp, lastCheckPoint.reconsumeCount() + 1));

                        BatchWriteRequest writeTimerTagRequest = new BatchWriteRequest(KV_NAMESPACE_TIMER_TAG,
                            buildTimerTagKey(nextVisibleTimestamp, topicId, queueId, message.offset(), operationId),
                            buildTimerTagValue(nextVisibleTimestamp, consumerGroupId, topicId, queueId, streamId, message.offset(), operationId));

                        BatchWriteRequest writeOrderIndexRequest = new BatchWriteRequest(KV_NAMESPACE_ORDER_INDEX,
                            buildOrderIndexKey(consumerGroupId, topicId, queueId, message.offset()), buildOrderIndexValue(operationId));
                        kvService.batch(deleteLastCheckPointRequest, deleteLastTimerTagRequest, writeCheckPointRequest, writeTimerTagRequest, writeOrderIndexRequest);
                        continue;
                    }

                    // If this message is not orderly or has not been consumed, write check point and timer tag to KV service atomically.
                    List<BatchRequest> requestList = new ArrayList<>();
                    BatchWriteRequest writeCheckPointRequest = new BatchWriteRequest(KV_NAMESPACE_CHECK_POINT,
                        buildCheckPointKey(topicId, queueId, message.offset(), operationId),
                        buildCheckPointValue(topicId, queueId, message.offset(), consumerGroupId, operationId, fifo, retry, operationTimestamp, nextVisibleTimestamp, 0));
                    requestList.add(writeCheckPointRequest);

                    BatchWriteRequest writeTimerTagRequest = new BatchWriteRequest(KV_NAMESPACE_TIMER_TAG,
                        buildTimerTagKey(nextVisibleTimestamp, topicId, queueId, message.offset(), operationId),
                        buildTimerTagValue(nextVisibleTimestamp, consumerGroupId, topicId, queueId, streamId, message.offset(), operationId));
                    requestList.add(writeTimerTagRequest);

                    // If this message is orderly, write order index to KV service.
                    if (fifo) {
                        BatchWriteRequest writeOrderIndexRequest = new BatchWriteRequest(KV_NAMESPACE_ORDER_INDEX,
                            buildOrderIndexKey(consumerGroupId, topicId, queueId, message.offset()), buildOrderIndexValue(operationId));
                        requestList.add(writeOrderIndexRequest);
                    }

                    kvService.batch(requestList.toArray(new BatchRequest[0]));
                } catch (RocksDBException e) {
                    // TODO: handle exception
                    throw new RuntimeException(e);
                }
            }

            return new PopResult(0, operationId, operationTimestamp, messageList);
        });
    }

    @Override
    public CompletableFuture<AckResult> ack(String receiptHandle) {
        // Write ack operation to operation log.
        // Operation id should be monotonically increasing for each queue
        ReceiptHandle handle = decodeReceiptHandle(receiptHandle);
        return operationLogService.logAckOperation(handle, System.nanoTime())
            .thenApply(operationId -> {
                // Delete check point and timer tag according to receiptHandle
                try {
                    // Check if check point exists.
                    byte[] checkPointKey = buildCheckPointKey(handle.topicId(), handle.queueId(), handle.messageOffset(), handle.operationId());
                    byte[] buffer = kvService.get(KV_NAMESPACE_CHECK_POINT, checkPointKey);
                    if (buffer == null) {
                        // TODO: Check point not found
                        return new AckResult();
                    }

                    // TODO: Data race between ack and revive.
                    CheckPoint checkPoint = CheckPoint.getRootAsCheckPoint(ByteBuffer.wrap(buffer));

                    List<BatchRequest> requestList = new ArrayList<>();
                    BatchDeleteRequest deleteCheckPointRequest = new BatchDeleteRequest(KV_NAMESPACE_CHECK_POINT, checkPointKey);
                    requestList.add(deleteCheckPointRequest);

                    BatchDeleteRequest deleteTimerTagRequest = new BatchDeleteRequest(KV_NAMESPACE_TIMER_TAG,
                        buildTimerTagKey(checkPoint.nextVisibleTimestamp(), handle.topicId(), handle.queueId(), handle.messageOffset(), checkPoint.operationId()));
                    requestList.add(deleteTimerTagRequest);

                    if (checkPoint.fifo()) {
                        BatchDeleteRequest deleteOrderIndexRequest = new BatchDeleteRequest(KV_NAMESPACE_ORDER_INDEX,
                            buildOrderIndexKey(checkPoint.consumerGroupId(), handle.topicId(), handle.queueId(), checkPoint.messageOffset()));
                        requestList.add(deleteOrderIndexRequest);
                    }

                    kvService.batch(requestList.toArray(new BatchRequest[0]));
                } catch (RocksDBException e) {
                    // TODO: handle exception
                    throw new RuntimeException(e);
                }

                return new AckResult();
            });
    }

    @Override
    public CompletableFuture<ChangeInvisibleDurationResult> changeInvisibleDuration(String receiptHandle,
        long invisibleDuration) {
        // Write change invisible duration operation to operation log.
        // Operation id should be monotonically increasing for each queue
        long operationTimestamp = System.nanoTime();
        ReceiptHandle handle = decodeReceiptHandle(receiptHandle);

        return operationLogService.logChangeInvisibleDurationOperation(handle, invisibleDuration, operationTimestamp)
            .thenApply(operationId -> {
                long nextInvisibleTimestamp = operationTimestamp + invisibleDuration;
                // change invisibleTime in check point info and regenerate timer tag
                try {
                    // Check if check point exists.
                    byte[] checkPointKey = buildCheckPointKey(handle.topicId(), handle.queueId(), handle.messageOffset(), handle.operationId());
                    byte[] buffer = kvService.get(KV_NAMESPACE_CHECK_POINT, checkPointKey);
                    if (buffer == null) {
                        // TODO: Check point not found
                        return new ChangeInvisibleDurationResult();
                    }

                    // Delete last timer tag.
                    CheckPoint checkPoint = CheckPoint.getRootAsCheckPoint(ByteBuffer.wrap(buffer));

                    BatchDeleteRequest deleteLastTimerTagRequest = new BatchDeleteRequest(KV_NAMESPACE_TIMER_TAG,
                        buildTimerTagKey(checkPoint.nextVisibleTimestamp(), checkPoint.topicId(), checkPoint.queueId(), checkPoint.messageOffset(), checkPoint.operationId()));

                    // Write new check point and timer tag.
                    BatchWriteRequest writeCheckPointRequest = new BatchWriteRequest(KV_NAMESPACE_CHECK_POINT,
                        buildCheckPointKey(checkPoint.topicId(), checkPoint.queueId(), checkPoint.messageOffset(), checkPoint.operationId()),
                        buildCheckPointValue(checkPoint.topicId(), checkPoint.queueId(), checkPoint.messageOffset(),
                            checkPoint.consumerGroupId(), checkPoint.operationId(), checkPoint.fifo(), checkPoint.retry(),
                            checkPoint.deliveryTimestamp(), nextInvisibleTimestamp, checkPoint.reconsumeCount()));

                    long streamId;
                    if (checkPoint.retry()) {
                        streamId = metadataService.getRetryStreamId(checkPoint.consumerGroupId(), checkPoint.topicId(), checkPoint.queueId());
                    } else {
                        streamId = metadataService.getStreamId(checkPoint.topicId(), checkPoint.queueId());
                    }
                    BatchWriteRequest writeTimerTagRequest = new BatchWriteRequest(KV_NAMESPACE_TIMER_TAG,
                        buildTimerTagKey(nextInvisibleTimestamp, checkPoint.topicId(), checkPoint.queueId(), checkPoint.messageOffset(), checkPoint.operationId()),
                        buildTimerTagValue(nextInvisibleTimestamp, checkPoint.consumerGroupId(), checkPoint.topicId(), checkPoint.queueId(),
                            streamId, checkPoint.messageOffset(), checkPoint.operationId()));

                    kvService.batch(deleteLastTimerTagRequest, writeCheckPointRequest, writeTimerTagRequest);
                } catch (RocksDBException e) {
                    // TODO: handle exception
                    throw new RuntimeException(e);
                }

                return new ChangeInvisibleDurationResult();
            });
    }

    @Override
    public int getInflightStatsByQueue(long topicId, int queueId) {
        // get check point count of specified topic and queue
        return 0;
    }

    @Override
    public boolean cleanMetadata(long topicId, int queueId) {
        // clean all check points and timer tags of specified topic and queue
        return false;
    }
}
