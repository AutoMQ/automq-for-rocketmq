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

package com.automq.rocketmq.proxy.mock;

import com.automq.rocketmq.common.model.FlatMessageExt;
import com.automq.rocketmq.common.model.generated.FlatMessage;
import com.automq.rocketmq.store.api.LogicQueue;
import com.automq.rocketmq.store.api.MessageArrivalListener;
import com.automq.rocketmq.store.api.MessageStore;
import com.automq.rocketmq.store.model.StoreContext;
import com.automq.rocketmq.store.model.message.AckResult;
import com.automq.rocketmq.store.model.message.ChangeInvisibleDurationResult;
import com.automq.rocketmq.store.model.message.ClearRetryMessagesResult;
import com.automq.rocketmq.store.model.message.Filter;
import com.automq.rocketmq.store.model.message.PopResult;
import com.automq.rocketmq.store.model.message.PullResult;
import com.automq.rocketmq.store.model.message.PutResult;
import com.automq.rocketmq.store.model.message.ResetConsumeOffsetResult;
import com.automq.rocketmq.store.service.InflightService;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.lang3.tuple.Pair;

public class MockMessageStore implements MessageStore {
    private final HashMap<Long, AtomicLong> offsetMap = new HashMap<>();
    private final Set<String> receiptHandleSet = new HashSet<>();
    private final Map<Long, List<FlatMessageExt>> messageMap = new HashMap<>();
    private final InflightService inflightService = new InflightService();

    private final Map<Pair<Long, Integer>, Long> consumerOffsetMap = new HashMap<>();

    public MockMessageStore() {
        receiptHandleSet.add("FAAAAAAAAAAMABwABAAAAAwAFAAMAAAAAgAAAAAAAAACAAAAAAAAAAMAAAAAAAAA");
    }

    @Override
    public void start() {

    }

    @Override
    public void shutdown() {

    }

    @Override
    public CompletableFuture<PopResult> pop(StoreContext context, long consumerGroupId, long topicId, int queueId,
        Filter filter,
        int batchSize, boolean fifo, boolean retry, long invisibleDuration) {
        if (retry) {
            return CompletableFuture.completedFuture(new PopResult(PopResult.Status.END_OF_QUEUE, 0L, new ArrayList<>(), 0));
        }

        List<FlatMessageExt> messageList = messageMap.computeIfAbsent(topicId + queueId, v -> new ArrayList<>());
        long consumeOffset = consumerOffsetMap.computeIfAbsent(Pair.of(topicId, queueId), v -> 0L);
        int start = consumeOffset > messageList.size() ? -1 : (int) consumeOffset;
        int end = consumeOffset + batchSize >= messageList.size() ? messageList.size() : (int) consumeOffset + batchSize;

        PopResult.Status status;
        if (start < 0) {
            status = PopResult.Status.END_OF_QUEUE;
            messageList = new ArrayList<>();
        } else {
            status = PopResult.Status.FOUND;
            messageList = messageList.subList(start, end);
            consumerOffsetMap.put(Pair.of(topicId, queueId), consumeOffset + messageList.size());
            inflightService.increaseInflightCount(consumerGroupId, topicId, queueId, messageList.size());
        }
        return CompletableFuture.completedFuture(new PopResult(status, 0L, messageList, messageList.size() - end));
    }

    @Override
    public CompletableFuture<PullResult> pull(long consumerGroupId, long topicId, int queueId, Filter filter,
        long offset, int batchSize, boolean retry) {
        if (retry) {
            return CompletableFuture.completedFuture(new PullResult(PullResult.Status.NO_NEW_MSG, 0L, 0L, 0L, new ArrayList<>()));
        }

        List<FlatMessageExt> messageList = messageMap.computeIfAbsent(topicId + queueId, v -> new ArrayList<>());
        int start = offset >= messageList.size() ? -1 : (int) offset;
        int end = offset + batchSize >= messageList.size() ? messageList.size() : (int) offset + batchSize;

        PullResult.Status status;
        if (start < 0) {
            status = PullResult.Status.NO_NEW_MSG;
            messageList = new ArrayList<>();
        } else {
            status = PullResult.Status.FOUND;
            messageList = messageList.subList(start, end);
        }
        return CompletableFuture.completedFuture(new PullResult(status, offset + messageList.size(), 0L, messageList.size() - end, messageList));
    }

    @Override
    public CompletableFuture<PutResult> put(StoreContext context, FlatMessage message) {
        long offset = this.offsetMap.computeIfAbsent(message.topicId() + message.queueId(), queueId -> new AtomicLong()).getAndIncrement();
        FlatMessageExt messageExt = FlatMessageExt.Builder.builder().message(message).offset(offset).build();
        List<FlatMessageExt> messageList = messageMap.computeIfAbsent(message.topicId() + message.queueId(), v -> new ArrayList<>());
        messageList.add(messageExt);
        return CompletableFuture.completedFuture(new PutResult(PutResult.Status.PUT_OK, offset));
    }

    @Override
    public CompletableFuture<AckResult> ack(String receiptHandle) {
        AckResult.Status status;
        if (receiptHandleSet.contains(receiptHandle)) {
            status = AckResult.Status.SUCCESS;
            inflightService.decreaseInflightCount(8, 2, 0, 1);
        } else {
            status = AckResult.Status.ERROR;
        }
        return CompletableFuture.completedFuture(new AckResult(status));
    }

    @Override
    public CompletableFuture<ChangeInvisibleDurationResult> changeInvisibleDuration(String receiptHandle,
        long invisibleDuration) {
        ChangeInvisibleDurationResult.Status status;
        if (receiptHandleSet.contains(receiptHandle)) {
            status = ChangeInvisibleDurationResult.Status.SUCCESS;
        } else {
            status = ChangeInvisibleDurationResult.Status.ERROR;
        }
        return CompletableFuture.completedFuture(new ChangeInvisibleDurationResult(status));
    }

    @Override
    public CompletableFuture<Void> closeQueue(long topicId, int queueId) {
        return null;
    }

    @Override
    public CompletableFuture<Integer> getInflightStats(long consumerGroupId, long topicId, int queueId) {
        return CompletableFuture.completedFuture(inflightService.getInflightCount(consumerGroupId, topicId, queueId));
    }

    @Override
    public CompletableFuture<LogicQueue.QueueOffsetRange> getOffsetRange(long topicId, int queueId) {
        long startOffset = 0;
        List<FlatMessageExt> messageList = messageMap.computeIfAbsent(topicId + queueId, v -> new ArrayList<>());
        if (!messageList.isEmpty()) {
            startOffset = messageList.get(0).offset();
        }
        long endOffset = offsetMap.computeIfAbsent(topicId + queueId, v -> new AtomicLong()).get();
        return CompletableFuture.completedFuture(new LogicQueue.QueueOffsetRange(startOffset, endOffset));
    }

    @Override
    public CompletableFuture<Long> getConsumeOffset(long consumerGroupId, long topicId, int queueId) {
        return CompletableFuture.completedFuture(consumerOffsetMap.getOrDefault(Pair.of(topicId, queueId), 0L));
    }

    @Override
    public CompletableFuture<ResetConsumeOffsetResult> resetConsumeOffset(long consumerGroupId, long topicId,
        int queueId, long offset) {
        consumerOffsetMap.put(Pair.of(topicId, queueId), offset);
        return CompletableFuture.completedFuture(new ResetConsumeOffsetResult(ResetConsumeOffsetResult.Status.SUCCESS));
    }

    @Override
    public CompletableFuture<ClearRetryMessagesResult> clearRetryMessages(long consumerGroupId, long topicId,
        int queueId) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public void registerMessageArriveListener(MessageArrivalListener listener) {
    }
}
