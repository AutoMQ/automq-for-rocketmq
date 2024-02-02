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

package com.automq.rocketmq.store.api;

import com.automq.rocketmq.common.util.Lifecycle;
import com.automq.rocketmq.store.model.StoreContext;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

public interface LogicQueueManager extends Lifecycle {

    /**
     * Get or create the logic queue of the specified topic and queue id.
     *
     * @param context context propagation
     * @param topicId topic id
     * @param queueId queue id
     * @return {@link CompletableFuture} of {@link LogicQueue}
     */
    CompletableFuture<LogicQueue> getOrCreate(StoreContext context, long topicId, int queueId);

    /**
     * Get the logic queue of the specified topic and queue id.
     *
     * @param topicId topic id
     * @param queueId queue id
     * @return {@link CompletableFuture} of {@link Optional<LogicQueue>} which contains the logic queue if exists
     */
    CompletableFuture<Optional<LogicQueue>> get(long topicId, int queueId);

    CompletableFuture<Void> close(long topicId, int queueId);
}
