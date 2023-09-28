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

package com.automq.rocketmq.metadata;

import apache.rocketmq.controller.v1.S3StreamObject;
import apache.rocketmq.controller.v1.S3WALObject;
import apache.rocketmq.controller.v1.StreamMetadata;
import com.automq.rocketmq.common.util.Pair;
import com.automq.rocketmq.controller.exception.ControllerException;
import com.automq.rocketmq.controller.metadata.MetadataStore;
import com.automq.rocketmq.controller.metadata.database.DefaultMetadataStore;
import com.automq.rocketmq.controller.metadata.database.dao.StreamRole;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultStoreMetadataService implements StoreMetadataService {

    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultMetadataStore.class);

    private final MetadataStore metadataStore;

    public DefaultStoreMetadataService(MetadataStore metadataStore) {
        this.metadataStore = metadataStore;
    }

    @Override
    public long getStreamId(long topicId, int queueId) {
        try {
            return metadataStore.streamIdOf(topicId, queueId, null, StreamRole.DATA);
        } catch (ControllerException e) {
            LOGGER.error("Failed to acquire data stream-id for topic-id={}, queue-id={}", topicId, queueId);
            return -1L;
        }
    }

    @Override
    public long getOperationLogStreamId(long topicId, int queueId) {
        try {
            return metadataStore.streamIdOf(topicId, queueId, null, StreamRole.OPS);
        } catch (ControllerException e) {
            LOGGER.error("Failed to acquire data stream-id for topic-id={}, queue-id={}", topicId, queueId);
            return -1L;
        }
    }

    @Override
    public long getRetryStreamId(long consumerGroupId, long topicId, int queueId) {
        try {
            return metadataStore.streamIdOf(topicId, queueId, consumerGroupId, StreamRole.RETRY);
        } catch (ControllerException e) {
            LOGGER.error("Failed to acquire data stream-id for topic-id={}, queue-id={}", topicId, queueId);
            return -1L;
        }
    }

    @Override
    public long getDeadLetterStreamId(long consumerGroupId, long topicId, int queueId) {
        throw new RuntimeException("Unsupported operation");
    }

    @Override
    public int getMaxRetryTimes(long consumerGroupId) {
        return 0;
    }

    @Override
    public CompletableFuture<StreamMetadata> dataStreamOf(long topicId, int queueId) {
        return null;
    }

    @Override
    public CompletableFuture<StreamMetadata> operationStreamOf(long topicId, int queueId) {
        return null;
    }

    @Override
    public CompletableFuture<StreamMetadata> retryStreamOf(long consumerGroupId, long topicId, int queueId) {
        return null;
    }

    @Override
    public CompletableFuture<List<StreamMetadata>> listStreamsManagedBy(long topicId, int queueId) {
        return null;
    }

    @Override
    public CompletableFuture<Integer> maxRetryTimesOf(long consumerGroupId) {
        return null;
    }

    @Override
    public CompletableFuture<Void> trimStream(long streamId, long streamEpoch,
        long newStartOffset) {
        return null;
    }

    @Override
    public CompletableFuture<StreamMetadata> openStream(long streamId, long streamEpoch) {
        return null;
    }

    @Override
    public CompletableFuture<Void> closeStream(long streamId, long streamEpoch) {
        return null;
    }

    @Override
    public CompletableFuture<List<StreamMetadata>> listOpenStreams() {
        return null;
    }

    @Override
    public CompletableFuture<Long> prepareS3Objects(int count, int ttlInMinutes) {
        return null;
    }

    @Override
    public CompletableFuture<Void> commitWalObject(S3WALObject walObject, List<S3StreamObject> streamObjects,
        List<Long> compactedObjects) {
        return null;
    }

    @Override
    public CompletableFuture<Void> commitStreamObject(S3StreamObject streamObject, List<Long> compactedObjects) {
        return null;
    }

    @Override
    public CompletableFuture<List<S3WALObject>> listWALObjects() {
        return null;
    }

    @Override
    public CompletableFuture<List<S3WALObject>> listWALObjects(long streamId, long startOffset, long endOffset,
        int limit) {
        return null;
    }

    @Override
    public CompletableFuture<List<S3StreamObject>> listStreamObjects(long streamId, long startOffset, long endOffset,
        int limit) {
        return null;
    }

    public CompletableFuture<Pair<List<S3StreamObject>, List<S3WALObject>>> listObjects(long streamId, long startOffset,
        long endOffset, int limit) {
        return null;
    }
}
