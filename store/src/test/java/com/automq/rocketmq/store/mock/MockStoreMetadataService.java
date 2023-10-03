/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.automq.rocketmq.store.mock;

import apache.rocketmq.controller.v1.S3StreamObject;
import apache.rocketmq.controller.v1.S3WALObject;
import apache.rocketmq.controller.v1.StreamMetadata;
import com.automq.rocketmq.common.util.Pair;
import com.automq.rocketmq.metadata.api.StoreMetadataService;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class MockStoreMetadataService implements StoreMetadataService {
    @Override
    public long getStreamId(long topicId, int queueId) {
        ByteBuffer buffer = ByteBuffer.allocate(8);
        // Mark the stream type as origin topic.
        buffer.putShort(0, (short) 0);
        buffer.putShort(2, (short) topicId);
        buffer.putShort(4, (short) queueId);
        return buffer.getLong(0);
    }

    @Override
    public long getOperationLogStreamId(long topicId, int queueId) {
        ByteBuffer buffer = ByteBuffer.allocate(8);
        // Mark the stream type as operation log.
        buffer.putShort(0, (short) 1);
        buffer.putShort(2, (short) topicId);
        buffer.putShort(4, (short) queueId);
        return buffer.getLong(0);
    }

    @Override
    public long getRetryStreamId(long consumerGroupId, long topicId, int queueId) {
        ByteBuffer buffer = ByteBuffer.allocate(8);
        // Mark the stream type as retry topic.
        buffer.putShort(0, (short) 2);
        buffer.putShort(2, (short) consumerGroupId);
        buffer.putShort(4, (short) topicId);
        buffer.putShort(6, (short) queueId);
        return buffer.getLong(0);
    }

    @Override
    public long getDeadLetterStreamId(long consumerGroupId, long topicId, int queueId) {
        ByteBuffer buffer = ByteBuffer.allocate(8);
        // Mark the stream type as dead letter topic.
        buffer.putShort(0, (short) 3);
        buffer.putShort(2, (short) consumerGroupId);
        buffer.putShort(4, (short) topicId);
        buffer.putShort(6, (short) queueId);
        return buffer.getLong(0);
    }

    @Override
    public int getMaxDeliveryAttempts(long consumerGroupId) {
        return 1;
    }

    @Override
    public CompletableFuture<StreamMetadata> dataStreamOf(long topicId, int queueId) {
        ByteBuffer buffer = ByteBuffer.allocate(8);
        // Mark the stream type as origin topic.
        buffer.putShort(0, (short) 0);
        buffer.putShort(2, (short) topicId);
        buffer.putShort(4, (short) queueId);
        long id = buffer.getLong(0);
        return CompletableFuture.completedFuture(StreamMetadata.newBuilder().setStreamId(id).build());
    }

    @Override
    public CompletableFuture<StreamMetadata> operationStreamOf(long topicId, int queueId) {
        ByteBuffer buffer = ByteBuffer.allocate(8);
        // Mark the stream type as operation log.
        buffer.putShort(0, (short) 1);
        buffer.putShort(2, (short) topicId);
        buffer.putShort(4, (short) queueId);
        long id = buffer.getLong(0);
        return CompletableFuture.completedFuture(StreamMetadata.newBuilder().setStreamId(id).build());
    }

    @Override
    public CompletableFuture<StreamMetadata> snapshotStreamOf(long topicId, int queueId) {
        ByteBuffer buffer = ByteBuffer.allocate(8);
        // Mark the stream type as snapshot.
        buffer.putShort(0, (short) 4);
        buffer.putShort(2, (short) topicId);
        buffer.putShort(4, (short) queueId);
        long id = buffer.getLong(0);
        return CompletableFuture.completedFuture(StreamMetadata.newBuilder().setStreamId(id).build());
    }

    @Override
    public CompletableFuture<StreamMetadata> retryStreamOf(long consumerGroupId, long topicId, int queueId) {
        ByteBuffer buffer = ByteBuffer.allocate(8);
        // Mark the stream type as retry topic.
        buffer.putShort(0, (short) 2);
        buffer.putShort(2, (short) consumerGroupId);
        buffer.putShort(4, (short) topicId);
        buffer.putShort(6, (short) queueId);
        long id = buffer.getLong(0);
        return CompletableFuture.completedFuture(StreamMetadata.newBuilder().setStreamId(id).build());
    }

    @Override
    public CompletableFuture<List<StreamMetadata>> listStreamsManagedBy(long topicId, int queueId) {
        return null;
    }

    @Override
    public CompletableFuture<Integer> maxDeliveryAttemptsOf(long consumerGroupId) {
        return CompletableFuture.completedFuture(2);
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

    @Override
    public CompletableFuture<Pair<List<S3StreamObject>, List<S3WALObject>>> listObjects(long streamId, long startOffset,
        long endOffset, int limit) {
        return null;
    }
}
