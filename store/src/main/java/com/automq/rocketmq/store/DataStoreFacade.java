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
