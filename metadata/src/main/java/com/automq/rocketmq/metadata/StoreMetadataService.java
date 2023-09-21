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

package com.automq.rocketmq.metadata;

import apache.rocketmq.controller.v1.S3StreamObject;
import apache.rocketmq.controller.v1.S3WALObject;
import apache.rocketmq.controller.v1.StreamMetadata;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public interface StoreMetadataService {
    long getStreamId(long topicId, int queueId);

    long getOperationLogStreamId(long topicId, int queueId);

    long getRetryStreamId(long consumerGroupId, long topicId, int queueId);

    long getDeadLetterStreamId(long consumerGroupId, long topicId, int queueId);

    int getMaxRetryTimes(long consumerGroupId);

    /**
     * Trim stream to new start offset. The old data will be deleted or marked as deleted.
     *
     * @param streamId stream id.
     * @param streamEpoch stream epoch.
     * @param newStartOffset new start offset.
     * @return {@link CompletableFuture} of trim operation.
     */
    CompletableFuture<Void> trimStream(long streamId, long streamEpoch, long newStartOffset);

    /**
     * Open stream with newer epoch. The controller will:
     * 1. update stream epoch to fence old stream writer to commit object.
     * 2. calculate the last range endOffset.
     *
     * @param streamId stream id.
     * @param streamEpoch stream epoch.
     * @return {@link StreamMetadata}
     */
    CompletableFuture<StreamMetadata> openStream(long streamId, long streamEpoch);

    /**
     * Mark the specified stream as closed.
     *
     * @param streamId stream id.
     * @param streamEpoch stream epoch.
     * @return {@link CompletableFuture} of close operation.
     */
    CompletableFuture<Void> closeStream(long streamId, long streamEpoch);

    /**
     * List the open streams of current server.
     *
     * @return list of {@link StreamMetadata}
     */
    CompletableFuture<List<StreamMetadata>> listOpenStreams();

    /**
     * Request to prepare S3 objects before uploading.
     * <p>
     * The prepare and commit APIs follow the 2-phase commit manner to avoid leaving garbage in S3.
     *
     * @param count number of objects to prepare.
     * @param ttlInMinutes time to live in minutes. The uncommitted objects will be deleted after ttl.
     * @return the first object id.
     */
    CompletableFuture<Long> prepareS3Objects(int count, int ttlInMinutes);

    /**
     * Commit an uploaded or compacted S3 WAL object.
     * <p>
     * This operation will be triggered in upload or compaction process.
     *
     * @param walObject the new WAL object.
     * @param streamObjects the stream objects that split from the compaction process.
     * @param compactedObjects the compacted objects that should be deleted.
     * @return {@link CompletableFuture} of commit operation.
     */
    CompletableFuture<Void> commitWalObject(S3WALObject walObject, List<S3StreamObject> streamObjects, List<Long> compactedObjects);

    /**
     * Commit a compacted S3 stream object.
     * <p>
     * This operation will only be triggered in stream object compaction process.
     *
     * @param streamObject the new stream object.
     * @param compactedObjects the compacted objects that should be deleted.
     * @return {@link CompletableFuture} of commit operation.
     */
    CompletableFuture<Void> commitStreamObject(S3StreamObject streamObject, List<Long> compactedObjects);

    /**
     * List the WAL objects served by the current server.
     *
     * @return list of {@link S3WALObject}
     */
    CompletableFuture<List<S3WALObject>> listWALObjects();

    /**
     * List stream objects by a specified stream range with a limit count.
     *
     * @param streamId the specified stream id
     * @param startOffset the start offset of the specified stream range.
     * @param endOffset the end offset of the specified stream range. NOOP_OFFSET(-1) represent endOffset is unlimited.
     * @param limit the limit count of the returned stream objects.
     * @return list of {@link S3StreamObject}
     */
    CompletableFuture<List<S3StreamObject>> listStreamObjects(long streamId, long startOffset, long endOffset, int limit);
}
