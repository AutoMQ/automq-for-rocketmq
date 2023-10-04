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

import com.automq.rocketmq.common.config.S3StreamConfig;
import com.automq.rocketmq.metadata.api.StoreMetadataService;
import com.automq.rocketmq.store.api.StreamStore;
import com.automq.stream.api.AppendResult;
import com.automq.stream.api.FetchResult;
import com.automq.stream.api.OpenStreamOptions;
import com.automq.stream.api.RecordBatch;
import com.automq.stream.api.Stream;
import com.automq.stream.api.StreamClient;
import com.automq.stream.s3.Config;
import com.automq.stream.s3.S3Storage;
import com.automq.stream.s3.S3StreamClient;
import com.automq.stream.s3.Storage;
import com.automq.stream.s3.cache.DefaultS3BlockCache;
import com.automq.stream.s3.cache.S3BlockCache;
import com.automq.stream.s3.compact.CompactionManager;
import com.automq.stream.s3.objects.ObjectManager;
import com.automq.stream.s3.operator.DefaultS3Operator;
import com.automq.stream.s3.operator.S3Operator;
import com.automq.stream.s3.streams.StreamManager;
import com.automq.stream.s3.wal.BlockWALService;
import com.automq.stream.s3.wal.WriteAheadLog;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class S3StreamStore implements StreamStore {
    private static final Logger LOGGER = LoggerFactory.getLogger(S3StreamStore.class);
    private final Config s3Config;
    private final StreamClient streamClient;
    private final StoreMetadataService metadataService;
    private final StreamManager streamManager;
    private final ObjectManager objectManager;
    private final WriteAheadLog writeAheadLog;
    private final S3Operator operator;
    private final Storage storage;
    private final CompactionManager compactionManager;
    private final S3BlockCache blockCache;
    // TODO: Should clean the closed streams to avoid memory leak.
    private final Map<Long, Stream> openStreams = new ConcurrentHashMap<>();

    public S3StreamStore(S3StreamConfig streamConfig, StoreMetadataService metadataService) {
        this.s3Config = configFrom(streamConfig);

        // Build meta service and related manager
        this.metadataService = metadataService;
        this.streamManager = new S3StreamManager(metadataService);
        this.objectManager = new S3ObjectManager(metadataService);

        this.operator = new DefaultS3Operator(s3Config.s3Endpont(), s3Config.s3Region(), s3Config.s3Bucket(),
            s3Config.s3AccessKey(), s3Config.s3SecretKey());
        this.writeAheadLog = BlockWALService.builder(s3Config.s3WALPath(), s3Config.s3WALCapacity()).config(s3Config).build();
        this.blockCache = new DefaultS3BlockCache(s3Config.s3CacheSize(), objectManager, operator);

        // Build the s3 storage
        this.storage = new S3Storage(s3Config, writeAheadLog, streamManager, objectManager, blockCache, operator);

        // Build the compaction manager
        this.compactionManager = new CompactionManager(s3Config, objectManager, operator);

        this.streamClient = new S3StreamClient(streamManager, storage, objectManager, operator, s3Config);
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
    public CompletableFuture<Void> close(List<Long> streamIds) {
        // TODO: Close Stream
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Void> trim(long streamId, long newStartOffset) {
        Stream stream = openStream(streamId);
        return stream.trim(newStartOffset);
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
    @Override
    public void start() throws Exception {
        this.storage.startup();
        this.compactionManager.start();
    }

    @Override
    public void shutdown() throws Exception {
        this.storage.shutdown();
        this.compactionManager.shutdown();
        this.streamClient.shutdown();
    }

    @Override
    public CompletableFuture<Void> open(long streamId) {
        openStream(streamId);
        return CompletableFuture.completedFuture(null);
    }

    /**
     * Open the specified stream if not opened yet.
     *
     * @param streamId stream id.
     * @return the opened stream.
     */
    private Stream openStream(long streamId) {
        // Open the specified stream if not opened yet.
        // TODO: Reimplement the stream open logic.
        OpenStreamOptions options = OpenStreamOptions.newBuilder().epoch(3).build();
        return openStreams.computeIfAbsent(streamId, id -> streamClient.openStream(id, options).join());
    }

    private Config configFrom(S3StreamConfig streamConfig) {
        Config config = new Config();
        config.s3Endpoint(streamConfig.s3Endpoint());
        config.s3Region(streamConfig.s3Region());
        config.s3Bucket(streamConfig.s3Bucket());
        config.s3WALPath(streamConfig.s3WALPath());
        config.s3AccessKey(streamConfig.s3AccessKey());
        config.s3SecretKey(streamConfig.s3SecretKey());
        return config;
    }
}
