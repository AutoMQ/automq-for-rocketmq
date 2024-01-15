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

package com.automq.rocketmq.store;

import com.automq.rocketmq.common.api.DataStore;
import com.automq.rocketmq.store.api.LogicQueueManager;
import com.automq.rocketmq.store.api.S3ObjectOperator;
import com.automq.rocketmq.store.api.StreamStore;
import com.automq.rocketmq.store.model.StoreContext;
import com.google.common.base.Stopwatch;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataStoreFacade implements DataStore {

    private static final Logger LOGGER = LoggerFactory.getLogger(DataStoreFacade.class);

    private final StreamStore streamStore;
    private final S3ObjectOperator s3ObjectOperator;
    private final LogicQueueManager logicQueueManager;

    public DataStoreFacade(StreamStore streamStore, S3ObjectOperator s3ObjectOperator,
        LogicQueueManager logicQueueManager) {
        this.streamStore = streamStore;
        this.s3ObjectOperator = s3ObjectOperator;
        this.logicQueueManager = logicQueueManager;
    }

    @Override
    public CompletableFuture<Void> openQueue(long topicId, int queueId) {
        return logicQueueManager.getOrCreate(StoreContext.EMPTY, topicId, queueId)
            .thenAccept(ignore -> {
            });
    }

    @Override
    public CompletableFuture<Void> closeQueue(long topicId, int queueId) {
        return logicQueueManager.close(topicId, queueId);
    }

    @Override
    public CompletableFuture<Void> trimStream(long streamId, long offset) {
        return streamStore.trim(streamId, offset);
    }

    @Override
    public CompletableFuture<List<Long>> batchDeleteS3Objects(List<Long> objectIds) {
        Stopwatch stopwatch = Stopwatch.createStarted();
        CompletableFuture<List<Long>> future = s3ObjectOperator.delete(objectIds);
        return future.thenApply(list -> {
            LOGGER.info("batchDeleteS3Objects costs {}ms", stopwatch.elapsed(TimeUnit.MILLISECONDS));
            return list;
        });
    }
}
