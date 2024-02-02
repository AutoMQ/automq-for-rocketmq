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
import com.automq.stream.api.AppendResult;
import com.automq.stream.api.FetchResult;
import com.automq.stream.api.RecordBatch;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * A high level abstraction of stream store, hidden the details of S3Stream module.
 */
public interface StreamStore extends Lifecycle {
    /**
     * Open the specified stream if not opened yet.
     * The stream should be opened before any operation.
     *
     * @param streamId stream id.
     * @param epoch    epoch of the stream.
     * @return the opened stream.
     */
    CompletableFuture<Void> open(long streamId, long epoch);

    /**
     * Check if the specified stream is opened.
     *
     * @param streamId stream id.
     * @return true if the stream is opened, otherwise false.
     */
    boolean isOpened(long streamId);

    /**
     * Fetch records from stream store.
     *
     * @param streamId    the target stream id.
     * @param startOffset the start offset of the fetch.
     * @param maxCount    the max return count of the fetch.
     * @return the future of fetch result.
     */
    CompletableFuture<FetchResult> fetch(StoreContext context, long streamId, long startOffset, int maxCount);

    /**
     * Append record batch to stream store.
     *
     * @param streamId    the target stream id.
     * @param recordBatch the record batch to append.
     * @return the future of append result.
     */
    CompletableFuture<AppendResult> append(StoreContext context, long streamId, RecordBatch recordBatch);

    /**
     * Close streams.
     *
     * @param streamIds stream id list to close.
     * @return the future of close result.
     */
    CompletableFuture<Void> close(List<Long> streamIds);

    CompletableFuture<Void> trim(long streamId, long newStartOffset);

    /**
     * Get stream start record offset.
     */
    long startOffset(long streamId);

    /**
     * Get stream confirm record offset.
     */
    long confirmOffset(long streamId);

    /**
     * Get stream next record offset.
     */
    long nextOffset(long streamId);
}
