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

package com.automq.rocketmq.metadata;

import apache.rocketmq.controller.v1.ConsumerGroup;
import apache.rocketmq.controller.v1.S3StreamObject;
import apache.rocketmq.controller.v1.S3StreamSetObject;
import apache.rocketmq.controller.v1.StreamMetadata;
import apache.rocketmq.controller.v1.StreamRole;
import apache.rocketmq.controller.v1.Topic;
import com.automq.rocketmq.common.config.ControllerConfig;
import com.automq.rocketmq.controller.MetadataStore;
import com.automq.rocketmq.metadata.api.StoreMetadataService;
import com.automq.rocketmq.metadata.service.S3MetadataService;
import com.google.common.util.concurrent.MoreExecutors;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultStoreMetadataService implements StoreMetadataService {

    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultStoreMetadataService.class);

    private final MetadataStore metadataStore;

    private final S3MetadataService s3MetadataService;

    public DefaultStoreMetadataService(MetadataStore metadataStore, S3MetadataService s3MetadataService) {
        this.metadataStore = metadataStore;
        this.s3MetadataService = s3MetadataService;
    }

    @Override
    public CompletableFuture<Topic> topicOf(String topicName) {
        return metadataStore.describeTopic(null, topicName);
    }

    @Override
    public CompletableFuture<Topic> topicOf(long topicId) {
        return metadataStore.describeTopic(topicId, null);
    }

    @Override
    public CompletableFuture<ConsumerGroup> consumerGroupOf(String groupName) {
        return metadataStore.describeGroup(null, groupName);
    }

    @Override
    public CompletableFuture<ConsumerGroup> consumerGroupOf(long consumerGroupId) {
        return metadataStore.describeGroup(consumerGroupId, null);
    }

    @Override
    public CompletableFuture<StreamMetadata> dataStreamOf(long topicId, int queueId) {
        AtomicBoolean loop = new AtomicBoolean(true);
        return Futures.loop(loop::get, () -> metadataStore.getStream(topicId, queueId, null, StreamRole.STREAM_ROLE_DATA)
            .thenApply(res -> {
                loop.set(false);
                return res;
            }), MoreExecutors.directExecutor());
    }

    @Override
    public CompletableFuture<StreamMetadata> operationStreamOf(long topicId, int queueId) {
        AtomicBoolean loop = new AtomicBoolean(true);
        return Futures.loop(loop::get, () -> metadataStore.getStream(topicId, queueId, null, StreamRole.STREAM_ROLE_OPS)
            .thenApply(res -> {
                loop.set(false);
                return res;
            }), MoreExecutors.directExecutor());
    }

    @Override
    public CompletableFuture<StreamMetadata> snapshotStreamOf(long topicId, int queueId) {
        AtomicBoolean loop = new AtomicBoolean(true);
        return Futures.loop(loop::get, () -> metadataStore.getStream(topicId, queueId, null, StreamRole.STREAM_ROLE_SNAPSHOT)
            .thenApply(res -> {
                loop.set(false);
                return res;
            }), MoreExecutors.directExecutor());
    }

    @Override
    public CompletableFuture<StreamMetadata> retryStreamOf(long consumerGroupId, long topicId, int queueId) {
        AtomicBoolean loop = new AtomicBoolean(true);
        return Futures.loop(loop::get, () -> metadataStore.getStream(topicId, queueId, consumerGroupId, StreamRole.STREAM_ROLE_RETRY)
            .thenApply(res -> {
                loop.set(false);
                return res;
            }), MoreExecutors.directExecutor());
    }

    @Override
    public CompletableFuture<Integer> maxDeliveryAttemptsOf(long consumerGroupId) {
        AtomicBoolean loop = new AtomicBoolean(true);
        return Futures.loop(loop::get, () -> metadataStore.describeGroup(consumerGroupId, null).thenApply((ConsumerGroup::getMaxDeliveryAttempt))
            .thenApply(res -> {
                loop.set(false);
                return res;
            }), MoreExecutors.directExecutor());
    }

    @Override
    public CompletableFuture<Void> trimStream(long streamId, long streamEpoch, long newStartOffset) {
        AtomicBoolean loop = new AtomicBoolean(true);
        return Futures.loop(loop::get, () -> s3MetadataService.trimStream(streamId, streamEpoch, newStartOffset)
            .thenApply(res -> {
                loop.set(false);
                return res;
            }), MoreExecutors.directExecutor());
    }

    @Override
    public CompletableFuture<StreamMetadata> openStream(long streamId, long streamEpoch) {

        AtomicBoolean loop = new AtomicBoolean(true);
        return Futures.loop(loop::get, () -> metadataStore.openStream(streamId, streamEpoch, metadataStore.config().nodeId())
            .thenApply(res -> {
                loop.set(false);
                s3MetadataService.onStreamOpen(streamId);
                LOGGER.info("Open Stream[stream-id={}, epoch={}] returns metadata: {}", streamId, streamEpoch, res);
                return res;
            }), MoreExecutors.directExecutor());
    }

    @Override
    public CompletableFuture<Void> closeStream(long streamId, long streamEpoch) {
        AtomicBoolean loop = new AtomicBoolean(true);
        return Futures.loop(loop::get, () -> metadataStore.closeStream(streamId, streamEpoch, metadataStore.config().nodeId())
            .thenApply(res -> {
                loop.set(false);
                s3MetadataService.onStreamClose(streamId);
                return res;
            }), MoreExecutors.directExecutor());
    }

    @Override
    public CompletableFuture<List<StreamMetadata>> listOpenStreams() {
        AtomicBoolean loop = new AtomicBoolean(true);
        return Futures.loop(loop::get, () -> metadataStore.listOpenStreams(metadataStore.config().nodeId())
            .thenApply(res -> {
                loop.set(false);
                return res;
            }), MoreExecutors.directExecutor());
    }

    @Override
    public CompletableFuture<Long> prepareS3Objects(int count, int ttlInMinutes) {
        AtomicBoolean loop = new AtomicBoolean(true);
        return Futures.loop(loop::get, () -> s3MetadataService.prepareS3Objects(count, ttlInMinutes)
            .thenApply(res -> {
                loop.set(false);
                return res;
            }), MoreExecutors.directExecutor());
    }

    @Override
    public CompletableFuture<Void> commitStreamSetObject(S3StreamSetObject streamSetObject, List<S3StreamObject> streamObjects,
        List<Long> compactedObjects) {
        // The underlying storage layer does not know the current node id when constructing the StreamSet object.
        // So we should fill it here.
        S3StreamSetObject newStreamSetObject = S3StreamSetObject.newBuilder(streamSetObject).setBrokerId(metadataStore.config().nodeId()).build();
        AtomicBoolean loop = new AtomicBoolean(true);
        return Futures.loop(loop::get,
            () -> s3MetadataService.commitStreamSetObject(newStreamSetObject, streamObjects, compactedObjects)
                .thenApply(res -> {
                    loop.set(false);
                    return res;
                }),
            MoreExecutors.directExecutor());
    }

    @Override
    public CompletableFuture<Void> compactStreamObject(S3StreamObject streamObject, List<Long> compactedObjects) {
        AtomicBoolean loop = new AtomicBoolean(true);
        return Futures.loop(loop::get,
            () -> s3MetadataService.commitStreamObject(streamObject, compactedObjects)
                .thenApply(res -> {
                    loop.set(false);
                    return res;
                }),
            MoreExecutors.directExecutor());
    }

    @Override
    public CompletableFuture<List<S3StreamSetObject>> listStreamSetObjects() {
        AtomicBoolean loop = new AtomicBoolean(true);
        return Futures.loop(loop::get, () -> s3MetadataService.listStreamSetObjects()
            .thenApply(res -> {
                loop.set(false);
                return res;
            }), MoreExecutors.directExecutor());
    }

    @Override
    public CompletableFuture<List<S3StreamSetObject>> listStreamSetObjects(long streamId, long startOffset, long endOffset,
        int limit) {
        AtomicBoolean loop = new AtomicBoolean(true);
        return Futures.loop(loop::get, () -> s3MetadataService.listStreamSetObjects(streamId, startOffset, endOffset, limit)
            .thenApply(res -> {
                loop.set(false);
                return res;
            }), MoreExecutors.directExecutor());
    }

    @Override
    public CompletableFuture<List<S3StreamObject>> listStreamObjects(long streamId, long startOffset, long endOffset,
        int limit) {
        AtomicBoolean loop = new AtomicBoolean(true);
        return Futures.loop(loop::get, () -> s3MetadataService.listStreamObjects(streamId, startOffset, endOffset, limit)
            .thenApply(res -> {
                loop.set(false);
                return res;
            }), MoreExecutors.directExecutor());
    }

    @Override
    public CompletableFuture<Pair<List<S3StreamObject>, List<S3StreamSetObject>>> listObjects(long streamId, long startOffset,
        long endOffset, int limit) {
        AtomicBoolean loop = new AtomicBoolean(true);
        return Futures.loop(loop::get, () -> s3MetadataService.listObjects(streamId, startOffset, endOffset, limit)
            .thenApply(res -> {
                loop.set(false);
                return res;
            }), MoreExecutors.directExecutor());
    }

    @Override
    public Optional<Integer> ownerNode(long topicId, int queueId) {
        return metadataStore.ownerNode(topicId, queueId);
    }

    @Override
    public ControllerConfig nodeConfig() {
        return metadataStore.config();
    }

    @Override
    public CompletableFuture<List<StreamMetadata>> getStreams(List<Long> streamIds) {
        if (streamIds == null || streamIds.isEmpty()) {
            return CompletableFuture.completedFuture(List.of());
        }

        AtomicBoolean loop = new AtomicBoolean(true);
        return Futures.loop(loop::get, () -> metadataStore.getStreams(streamIds)
            .thenApply(res -> {
                loop.set(false);
                return res;
            }), MoreExecutors.directExecutor());
    }
}
