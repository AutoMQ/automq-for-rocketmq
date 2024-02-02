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

package com.automq.rocketmq.store.mock;

import com.automq.rocketmq.store.api.StreamStore;
import com.automq.rocketmq.store.model.StoreContext;
import com.automq.stream.api.AppendResult;
import com.automq.stream.api.FetchResult;
import com.automq.stream.api.RecordBatch;
import com.automq.stream.api.Stream;
import com.automq.stream.api.StreamClient;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

public class MockStreamStore implements StreamStore {

    private final StreamClient streamClient;
    private final Map<Long, Stream> openedStreams = new ConcurrentHashMap<>();

    public MockStreamStore() {
        streamClient = new MemoryStreamClient();
    }

    @Override
    public CompletableFuture<FetchResult> fetch(StoreContext context, long streamId, long startOffset, int maxCount) {
        if (!openedStreams.containsKey(streamId)) {
            throw new IllegalStateException("Stream " + streamId + " is not opened.");
        }
        Stream stream = openedStreams.get(streamId);
        return stream.fetch(startOffset, startOffset + maxCount, Integer.MAX_VALUE);
    }

    @Override
    public CompletableFuture<AppendResult> append(StoreContext context, long streamId, RecordBatch recordBatch) {
        if (!openedStreams.containsKey(streamId)) {
            throw new IllegalStateException("Stream " + streamId + " is not opened.");
        }
        Stream stream = openedStreams.get(streamId);
        return stream.append(recordBatch);
    }

    @Override
    public CompletableFuture<Void> trim(long streamId, long newStartOffset) {
        if (!openedStreams.containsKey(streamId)) {
            throw new IllegalStateException("Stream " + streamId + " is not opened.");
        }
        Stream stream = openedStreams.get(streamId);
        return stream.trim(newStartOffset);
    }

    @Override
    public CompletableFuture<Void> close(List<Long> streams) {
        streams.forEach(this::closeStream);
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Void> open(long streamId, long epoch) {
        openStream(streamId);
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public boolean isOpened(long streamId) {
        return openedStreams.containsKey(streamId);
    }

    @Override
    public long startOffset(long streamId) {
        if (!openedStreams.containsKey(streamId)) {
            throw new IllegalStateException("Stream " + streamId + " is not opened.");
        }
        Stream stream = openedStreams.get(streamId);
        return stream.startOffset();
    }

    @Override
    public long confirmOffset(long streamId) {
        Stream stream = openedStreams.get(streamId);
        return stream.nextOffset();
    }

    @Override
    public long nextOffset(long streamId) {
        if (!openedStreams.containsKey(streamId)) {
            throw new IllegalStateException("Stream " + streamId + " is not opened.");
        }
        Stream stream = openedStreams.get(streamId);
        return stream.nextOffset();
    }

    /**
     * Open the specified stream if not opened yet.
     *
     * @param streamId stream id.
     * @return the opened stream.
     */
    private Stream openStream(long streamId) {
        // Open the specified stream if not opened yet.
        if (openedStreams.containsKey(streamId)) {
            throw new IllegalStateException("Stream " + streamId + " already opened.");
        }
        return openedStreams.computeIfAbsent(streamId, id -> streamClient.openStream(id, null).join());
    }

    private void closeStream(long streamId) {
        Stream stream = openedStreams.remove(streamId);
        if (stream != null) {
            stream.close();
        }
    }

    @Override
    public void start() {

    }

    @Override
    public void shutdown() {

    }
}
