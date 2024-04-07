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

import com.automq.rocketmq.common.config.S3StreamConfig;
import com.automq.rocketmq.common.config.StoreConfig;
import com.automq.rocketmq.metadata.api.StoreMetadataService;
import com.automq.rocketmq.store.api.StreamStore;
import com.automq.rocketmq.store.model.StoreContext;
import com.automq.rocketmq.store.util.ContextUtil;
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
import com.automq.stream.s3.context.AppendContext;
import com.automq.stream.s3.context.FetchContext;
import com.automq.stream.s3.network.AsyncNetworkBandwidthLimiter;
import com.automq.stream.s3.objects.ObjectManager;
import com.automq.stream.s3.operator.DefaultS3Operator;
import com.automq.stream.s3.operator.S3Operator;
import com.automq.stream.s3.streams.StreamManager;
import com.automq.stream.s3.wal.BlockWALService;
import com.automq.stream.s3.wal.WriteAheadLog;
import com.automq.stream.utils.threads.S3StreamThreadPoolMonitor;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.instrumentation.annotations.SpanAttribute;
import io.opentelemetry.instrumentation.annotations.WithSpan;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.common.thread.ThreadPoolMonitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;

public class S3StreamStore implements StreamStore {
    private static final Logger LOGGER = LoggerFactory.getLogger(S3StreamStore.class);
    private final StreamClient streamClient;
    private final Storage storage;
    private final CompactionManager compactionManager;
    private final ThreadPoolExecutor storeWorkingThreadPool;

    public S3StreamStore(StoreConfig storeConfig, S3StreamConfig streamConfig, StoreMetadataService metadataService) {
        Config s3Config = configFrom(streamConfig);

        // Build meta service and related manager
        StreamManager streamManager = new S3StreamManager(metadataService);
        ObjectManager objectManager = new S3ObjectManager(metadataService);

        AsyncNetworkBandwidthLimiter networkInboundLimiter = null;
        AsyncNetworkBandwidthLimiter networkOutboundLimiter = null;

        if (s3Config.networkBaselineBandwidth() > 0 && s3Config.refillPeriodMs() > 0) {
            networkInboundLimiter = new AsyncNetworkBandwidthLimiter(
                AsyncNetworkBandwidthLimiter.Type.INBOUND,
                s3Config.networkBaselineBandwidth(),
                s3Config.refillPeriodMs(),
                s3Config.networkBaselineBandwidth()
            );
            networkOutboundLimiter = new AsyncNetworkBandwidthLimiter(
                AsyncNetworkBandwidthLimiter.Type.OUTBOUND,
                s3Config.networkBaselineBandwidth(),
                s3Config.refillPeriodMs(),
                s3Config.networkBaselineBandwidth()
            );
        }

        S3Operator defaultOperator = new DefaultS3Operator(streamConfig.s3Endpoint(), streamConfig.s3Region(), streamConfig.s3Bucket(),
            streamConfig.s3ForcePathStyle(), List.of(() -> AwsBasicCredentials.create(streamConfig.s3AccessKey(), streamConfig.s3SecretKey())),
            false, networkInboundLimiter, networkOutboundLimiter, true);

        WriteAheadLog writeAheadLog = BlockWALService.builder(s3Config.walPath(), s3Config.walCapacity()).config(s3Config).build();
        S3BlockCache blockCache = new DefaultS3BlockCache(s3Config, objectManager, defaultOperator);

        // Build the s3 storage
        this.storage = new S3Storage(s3Config, writeAheadLog, streamManager, objectManager, blockCache, defaultOperator);

        // Build the compaction manager
        S3Operator compactionOperator = new DefaultS3Operator(streamConfig.s3Endpoint(), streamConfig.s3Region(), streamConfig.s3Bucket(),
            streamConfig.s3ForcePathStyle(), List.of(() -> AwsBasicCredentials.create(streamConfig.s3AccessKey(), streamConfig.s3SecretKey())),
            false, networkInboundLimiter, networkOutboundLimiter, true);
        this.compactionManager = new CompactionManager(s3Config, objectManager, streamManager, compactionOperator);

        this.streamClient = new S3StreamClient(streamManager, storage, objectManager, defaultOperator, s3Config, networkInboundLimiter, networkOutboundLimiter);
        this.storeWorkingThreadPool = ThreadPoolMonitor.createAndMonitor(
            storeConfig.workingThreadPoolNums(),
            storeConfig.workingThreadQueueCapacity(),
            1,
            TimeUnit.MINUTES,
            "StoreWorkingThreadPool",
            storeConfig.workingThreadQueueCapacity()
        );
        S3StreamThreadPoolMonitor.init();
    }

    @Override
    @WithSpan(kind = SpanKind.SERVER)
    public CompletableFuture<FetchResult> fetch(StoreContext context, @SpanAttribute long streamId,
        @SpanAttribute long startOffset, @SpanAttribute int maxCount) {

        if (maxCount <= 0) {
            return CompletableFuture.completedFuture(new EmptyFetchResult());
        }

        Optional<Stream> stream = streamClient.getStream(streamId);
        if (stream.isEmpty()) {
            throw new IllegalStateException("Stream " + streamId + " is not opened.");
        }
        FetchContext fetchContext = new FetchContext(ContextUtil.buildStreamTraceContext(context));
        return stream.get().fetch(fetchContext, startOffset, startOffset + maxCount, Integer.MAX_VALUE)
            .thenApplyAsync(result -> {
                context.span().ifPresent(span -> {
                    span.setAttribute("messageCount", result.recordBatchList().size());
                    span.setAttribute("cacheAccess", result.getCacheAccessType().name().toLowerCase());
                });
                return result;
            }, storeWorkingThreadPool);
    }

    @Override
    @WithSpan(kind = SpanKind.SERVER)
    public CompletableFuture<AppendResult> append(StoreContext context, long streamId, RecordBatch recordBatch) {
        Optional<Stream> stream = streamClient.getStream(streamId);
        if (stream.isEmpty()) {
            throw new IllegalStateException("Stream " + streamId + " is not opened.");
        }

        context.span().ifPresent(span -> {
            span.setAttribute("streamId", streamId);
            span.setAttribute("recordCount", recordBatch.count());
            span.setAttribute("recordBytes", recordBatch.rawPayload().remaining());
        });

        AppendContext appendContext = new AppendContext(ContextUtil.buildStreamTraceContext(context));
        return stream.get().append(appendContext, recordBatch)
            .thenApplyAsync(result -> {
                context.span().ifPresent(span -> span.setAttribute("offset", result.baseOffset()));
                return result;
            }, storeWorkingThreadPool);
    }

    @Override
    public CompletableFuture<Void> close(List<Long> streamIds) {
        List<CompletableFuture<Void>> futureList = streamIds.stream()
            .map(streamId -> {
                Optional<Stream> stream = streamClient.getStream(streamId);
                return stream.map(Stream::close).orElse(null);
            })
            .filter(Objects::nonNull)
            .toList();
        return CompletableFuture.allOf(futureList.toArray(new CompletableFuture[0]))
            .thenApplyAsync(result -> result, storeWorkingThreadPool);
    }

    @Override
    public CompletableFuture<Void> trim(long streamId, long newStartOffset) {
        Optional<Stream> stream = streamClient.getStream(streamId);
        if (stream.isEmpty()) {
            throw new IllegalStateException("Stream " + streamId + " is not opened.");
        }
        return stream.get().trim(newStartOffset)
            .thenApplyAsync(result -> result, storeWorkingThreadPool);
    }

    @Override
    public long startOffset(long streamId) {
        Optional<Stream> stream = streamClient.getStream(streamId);
        if (stream.isEmpty()) {
            throw new IllegalStateException("Stream " + streamId + " is not opened.");
        }
        return stream.get().startOffset();
    }

    @Override
    public long confirmOffset(long streamId) {
        Optional<Stream> stream = streamClient.getStream(streamId);
        if (stream.isEmpty()) {
            throw new IllegalStateException("Stream " + streamId + " is not opened.");
        }
        return stream.get().confirmOffset();
    }

    @Override
    public long nextOffset(long streamId) {
        Optional<Stream> stream = streamClient.getStream(streamId);
        if (stream.isEmpty()) {
            throw new IllegalStateException("Stream " + streamId + " is not opened.");
        }
        return stream.get().nextOffset();
    }

    @Override
    public void start() throws Exception {
        this.storage.startup();
        this.compactionManager.start();
    }

    @Override
    public void shutdown() throws Exception {
        this.streamClient.shutdown();
        this.compactionManager.shutdown();
        this.storage.shutdown();
    }

    @Override
    public CompletableFuture<Void> open(long streamId, long epoch) {
        Optional<Stream> optionalStream = streamClient.getStream(streamId);
        if (optionalStream.isPresent()) {
            return CompletableFuture.completedFuture(null);
        }

        // Open the specified stream if not opened yet.
        OpenStreamOptions options = OpenStreamOptions.builder().epoch(epoch).build();
        return streamClient.openStream(streamId, options)
            .thenAccept(stream -> LOGGER.info("Stream {} opened", streamId))
            .exceptionally(throwable -> {
                LOGGER.error("Failed to open stream {}", streamId, throwable);
                return null;
            });
    }

    @Override
    public boolean isOpened(long streamId) {
        return streamClient.getStream(streamId).isPresent();
    }

    private Config configFrom(S3StreamConfig streamConfig) {
        Config config = new Config();
        config.endpoint(streamConfig.s3Endpoint());
        config.region(streamConfig.s3Region());
        config.bucket(streamConfig.s3Bucket());
        config.forcePathStyle(streamConfig.s3ForcePathStyle());
        config.walPath(streamConfig.s3WALPath());
        config.networkBaselineBandwidth(streamConfig.networkBaselineBandwidth());
        config.refillPeriodMs(streamConfig.refillPeriodMs());

        config.objectBlockSize(streamConfig.objectBlockSize());

        // Cache
        config.walCacheSize(streamConfig.walCacheSize());
        config.blockCacheSize(streamConfig.blockCacheSize());

        // Compaction config
        config.streamObjectCompactionIntervalMinutes(streamConfig.streamObjectCompactionIntervalMinutes());
        config.streamObjectCompactionMaxSizeBytes(streamConfig.streamObjectCompactionMaxSizeBytes());

        config.streamSetObjectCompactionInterval(streamConfig.streamSetObjectCompactionInterval());
        config.streamSetObjectCompactionCacheSize(streamConfig.streamSetObjectCompactionCacheSize());
        config.streamSetObjectCompactionUploadConcurrency(streamConfig.streamSetObjectCompactionUploadConcurrency());
        config.streamSetObjectCompactionMaxObjectNum(streamConfig.streamSetObjectCompactionMaxObjectNum());
        config.streamSetObjectCompactionForceSplitPeriod(streamConfig.streamSetObjectCompactionForceSplitPeriod());
        config.streamSetObjectCompactionStreamSplitSize(streamConfig.streamSetObjectCompactionStreamSplitSize());
        config.streamSplitSize(streamConfig.streamSplitSizeThreshold());
        return config;
    }
}
