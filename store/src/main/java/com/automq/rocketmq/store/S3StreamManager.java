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
        //TODO: implement this
        return CompletableFuture.failedFuture(new UnsupportedOperationException("Get streams is not supported."));
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
        metadata.setStreamId(stream.getStreamId());
        metadata.setEpoch(stream.getEpoch());
        metadata.setStartOffset(stream.getStartOffset());
        metadata.setEndOffset(stream.getEndOffset());
        if (stream.getState() == apache.rocketmq.controller.v1.StreamState.OPEN) {
            metadata.setState(StreamState.OPENED);
        } else {
            // Treat all other states as closed.
            metadata.setState(StreamState.CLOSED);
        }
        return metadata;
    }
}
