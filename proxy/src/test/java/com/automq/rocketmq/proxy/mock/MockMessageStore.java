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

import com.automq.rocketmq.common.model.generated.Message;
import com.automq.rocketmq.store.MessageStore;
import com.automq.rocketmq.store.model.message.AckResult;
import com.automq.rocketmq.store.model.message.ChangeInvisibleDurationResult;
import com.automq.rocketmq.store.model.message.Filter;
import com.automq.rocketmq.store.model.message.PopResult;
import com.automq.rocketmq.store.model.message.PutResult;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;

public class MockMessageStore implements MessageStore {
    private AtomicLong offset = new AtomicLong();
    private Set<String> receiptHandleSet = new HashSet<>() {{
        this.add("receiptHandle");
    }};

    @Override
    public CompletableFuture<PopResult> pop(long consumerGroupId, long topicId, int queueId, long offset, Filter filter,
        int batchSize, boolean fifo, boolean retry, long invisibleDuration) {
        return null;
    }

    @Override
    public CompletableFuture<PutResult> put(Message message, Map<String, String> systemProperties) {
        return CompletableFuture.completedFuture(new PutResult(offset.getAndIncrement()));
    }

    @Override
    public CompletableFuture<AckResult> ack(String receiptHandle) {
        AckResult.Status status;
        if (receiptHandleSet.contains(receiptHandle)) {
            status = AckResult.Status.SUCCESS;
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
    public int getInflightStatsByQueue(long topicId, int queueId) {
        return 0;
    }

    @Override
    public boolean cleanMetadata(long topicId, int queueId) {
        return false;
    }
}
