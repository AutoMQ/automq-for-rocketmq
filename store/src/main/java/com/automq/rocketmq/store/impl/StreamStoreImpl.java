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

package com.automq.rocketmq.store.impl;

import com.automq.rocketmq.store.StreamStore;
import com.automq.rocketmq.stream.MemoryStreamClient;
import com.automq.rocketmq.stream.api.AppendResult;
import com.automq.rocketmq.stream.api.FetchResult;
import com.automq.rocketmq.stream.api.RecordBatch;
import com.automq.rocketmq.stream.api.Stream;
import com.automq.rocketmq.stream.api.StreamClient;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

public class StreamStoreImpl implements StreamStore {
    private final StreamClient streamClient;
    private final Map<Long, Stream> openedStreams = new ConcurrentHashMap<>();

    public StreamStoreImpl() {
        streamClient = new MemoryStreamClient();
    }

    @Override
    public CompletableFuture<FetchResult> fetch(long streamId, long startOffset, int maxCount) {
        Stream stream = openStream(streamId);
        return stream.fetch(startOffset, startOffset + maxCount, Integer.MAX_VALUE);
    }

    @Override
    public CompletableFuture<AppendResult> append(long streamId, RecordBatch recordBatch) {
        Stream stream = openStream(streamId);
        return stream.append(recordBatch);
    }

    @Override
    public long startOffset(long streamId) {
        Stream stream = openStream(streamId);
        return stream.startOffset();
    }

    @Override
    public long nextOffset(long streamId) {
        Stream stream = openStream(streamId);
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
        // TODO: Build a real OpenStreamOptions
        return openedStreams.computeIfAbsent(streamId, id -> streamClient.openStream(id, null).join());
    }
}
