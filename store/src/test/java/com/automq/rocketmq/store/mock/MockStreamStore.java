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
        // TODO: Build a real OpenStreamOptions
        return openedStreams.computeIfAbsent(streamId, id -> streamClient.openStream(id, null).join());
    }

    private void closeStream(long streamId) {
        Stream stream = openedStreams.remove(streamId);
        if (stream != null) {
            stream.close();
        }
    }

    @Override
    public void start() throws Exception {

    }

    @Override
    public void shutdown() throws Exception {

    }
}
