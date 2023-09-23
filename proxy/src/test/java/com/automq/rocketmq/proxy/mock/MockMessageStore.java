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

import com.automq.rocketmq.common.model.MessageExt;
import com.automq.rocketmq.common.model.generated.Message;
import com.automq.rocketmq.store.api.MessageStore;
import com.automq.rocketmq.store.model.message.AckResult;
import com.automq.rocketmq.store.model.message.ChangeInvisibleDurationResult;
import com.automq.rocketmq.store.model.message.Filter;
import com.automq.rocketmq.store.model.message.PopResult;
import com.automq.rocketmq.store.model.message.PutResult;
import com.automq.rocketmq.store.service.InflightService;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;

public class MockMessageStore implements MessageStore {
    private final HashMap<Long, AtomicLong> offsetMap = new HashMap<>();
    private final Set<String> receiptHandleSet = new HashSet<>();
    private final Map<Long, List<MessageExt>> messageMap = new HashMap<>();
    private final InflightService inflightService = new InflightService();

    public MockMessageStore() {
        receiptHandleSet.add("FAAAAAAAAAAMABwABAAAAAwAFAAMAAAAAgAAAAAAAAACAAAAAAAAAAMAAAAAAAAA");
    }

    @Override
    public CompletableFuture<PopResult> pop(long consumerGroupId, long topicId, int queueId, long offset, Filter filter,
        int batchSize, boolean fifo, boolean retry, long invisibleDuration) {
        if (retry) {
            return CompletableFuture.completedFuture(new PopResult(PopResult.Status.END_OF_QUEUE, 0L, 0L, new ArrayList<>()));
        }

        List<MessageExt> messageList = messageMap.computeIfAbsent(topicId + queueId, v -> new ArrayList<>());
        int start = offset > messageList.size() ? -1 : (int) offset;
        int end = offset + batchSize >= messageList.size() ? messageList.size() : (int) offset + batchSize;

        PopResult.Status status;
        if (start < 0) {
            status = PopResult.Status.END_OF_QUEUE;
            messageList = new ArrayList<>();
        } else {
            status = PopResult.Status.FOUND;
            messageList = messageList.subList(start, end);
            inflightService.increaseInflightCount(consumerGroupId, topicId, queueId, messageList.size());
        }
        return CompletableFuture.completedFuture(new PopResult(status, 0L, 0L, messageList));
    }

    @Override
    public CompletableFuture<PutResult> put(Message message, Map<String, String> systemProperties) {
        long offset = this.offsetMap.computeIfAbsent(message.topicId() + message.queueId(), queueId -> new AtomicLong()).getAndIncrement();
        MessageExt messageExt = MessageExt.Builder.builder().message(message).offset(offset).build();
        List<MessageExt> messageList = messageMap.computeIfAbsent(message.topicId() + message.queueId(), v -> new ArrayList<>());
        messageList.add(messageExt);
        return CompletableFuture.completedFuture(new PutResult(offset));
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
    public int getInflightStats(long consumerGroupId, long topicId, int queueId) {
        return inflightService.getInflightCount(consumerGroupId, topicId, queueId);
    }

    @Override
    public long startOffset(long topicId, int queueId) {
        List<MessageExt> messageList = messageMap.computeIfAbsent(topicId + queueId, v -> new ArrayList<>());
        if (messageList.isEmpty()) {
            return 0;
        }
        return messageList.get(0).offset();
    }

    @Override
    public long nextOffset(long topicId, int queueId) {
        return offsetMap.computeIfAbsent(topicId + queueId, v -> new AtomicLong()).get();
    }
}
