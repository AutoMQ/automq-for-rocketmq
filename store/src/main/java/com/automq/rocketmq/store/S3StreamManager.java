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

import com.automq.rocketmq.metadata.api.StoreMetadataService;
import com.automq.stream.s3.metadata.StreamMetadata;
import com.automq.stream.s3.metadata.StreamState;
import com.automq.stream.s3.streams.StreamManager;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

public class S3StreamManager implements StreamManager {
    private final StoreMetadataService metaService;

    public S3StreamManager(StoreMetadataService metaService) {
        this.metaService = metaService;
    }

    @Override
    public CompletableFuture<List<StreamMetadata>> getOpeningStreams() {
        CompletableFuture<List<apache.rocketmq.controller.v1.StreamMetadata>> openStreams = metaService.listOpenStreams();
        // Convert to S3Stream model.
        return openStreams.thenApply(streams -> streams.stream().map(this::convertFrom).collect(Collectors.toList()));
    }

    @Override
    public CompletableFuture<List<StreamMetadata>> getStreams(List<Long> streamIds) {
        return metaService.getStreams(streamIds).thenApply(streams -> streams.stream().map(this::convertFrom).collect(Collectors.toList()));
    }

    @Override
    public CompletableFuture<Long> createStream() {
        // Don't support create stream, stream is created on demand.
        return CompletableFuture.failedFuture(new UnsupportedOperationException("Create stream is not supported."));
    }

    @Override
    public CompletableFuture<StreamMetadata> openStream(long streamId, long epoch) {
        return metaService.openStream(streamId, epoch).thenApply(this::convertFrom);
    }

    @Override
    public CompletableFuture<Void> trimStream(long streamId, long epoch, long newStartOffset) {
        return metaService.trimStream(streamId, epoch, newStartOffset).thenApply(v -> null);
    }

    @Override
    public CompletableFuture<Void> closeStream(long streamId, long epoch) {
        return metaService.closeStream(streamId, epoch).thenApply(v -> null);
    }

    @Override
    public CompletableFuture<Void> deleteStream(long streamId, long epoch) {
        return CompletableFuture.failedFuture(new UnsupportedOperationException("Delete stream is not supported."));
    }

    private StreamMetadata convertFrom(apache.rocketmq.controller.v1.StreamMetadata stream) {
        StreamMetadata metadata = new StreamMetadata();
        metadata.streamId(stream.getStreamId());
        metadata.epoch(stream.getEpoch());
        metadata.streamId(stream.getStartOffset());
        metadata.endOffset(stream.getEndOffset());
        if (stream.getState() == apache.rocketmq.controller.v1.StreamState.OPEN) {
            metadata.state(StreamState.OPENED);
        } else {
            // Treat all other states as closed.
            metadata.state(StreamState.CLOSED);
        }
        return metadata;
    }
}
