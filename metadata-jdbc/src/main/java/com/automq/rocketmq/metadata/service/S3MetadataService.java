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

package com.automq.rocketmq.metadata.service;

import apache.rocketmq.controller.v1.S3StreamObject;
import apache.rocketmq.controller.v1.S3StreamSetObject;
import java.io.Closeable;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.commons.lang3.tuple.Pair;

/**
 * Localized accessor to S3 metadata.
 */
public interface S3MetadataService extends Closeable {

    public void start();

    CompletableFuture<Long> prepareS3Objects(int count, int ttlInMinutes);

    CompletableFuture<Void> commitStreamSetObject(S3StreamSetObject walObject, List<S3StreamObject> streamObjects,
        List<Long> compactedObjects);


    CompletableFuture<Void> commitStreamObject(S3StreamObject streamObject,
        List<Long> compactedObjects);

    CompletableFuture<List<S3StreamSetObject>> listStreamSetObjects();

    CompletableFuture<List<S3StreamSetObject>> listStreamSetObjects(long streamId, long startOffset, long endOffset, int limit);

    CompletableFuture<List<S3StreamObject>> listStreamObjects(long streamId, long startOffset, long endOffset, int limit);

    CompletableFuture<Pair<List<S3StreamObject>, List<S3StreamSetObject>>> listObjects(long streamId, long startOffset,
        long endOffset, int limit);

    CompletableFuture<Void> trimStream(long streamId, long streamEpoch, long newStartOffset);

    void onStreamOpen(long streamId);

    void onStreamClose(long streamId);

    long streamDataSize(long streamId);

    long streamStartTime(long streamId);
}
