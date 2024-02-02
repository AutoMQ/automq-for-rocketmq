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

package com.automq.rocketmq.common.api;

import java.util.List;
import java.util.concurrent.CompletableFuture;

public interface DataStore {

    CompletableFuture<Void> openQueue(long topicId, int queueId);

    CompletableFuture<Void> closeQueue(long topicId, int queueId);

    /**
     * Trim stream such that records with offsets prior to <code>offset</code> will be inaccessible.
     *
     * @param streamId ID of the stream to trim
     * @param offset   Minimum offset of the stream after trim operation
     */
    CompletableFuture<Void> trimStream(long streamId, long offset);

    /**
     * Delete a list of S3 objects by object id.
     * <p>
     * Regard non-exist object as success delete.
     *
     * @param objectIds the objects to delete.
     * @return the future of delete result, contains the deleted object id.
     */
    CompletableFuture<List<Long>> batchDeleteS3Objects(List<Long> objectIds);

    default void blockingShutdown() {

    }

}
