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

import apache.rocketmq.controller.v1.S3StreamObject;
import apache.rocketmq.controller.v1.S3WALObject;
import apache.rocketmq.controller.v1.SubStream;
import com.automq.rocketmq.metadata.StoreMetadataService;
import com.automq.stream.s3.metadata.S3ObjectMetadata;
import com.automq.stream.s3.metadata.S3ObjectType;
import com.automq.stream.s3.metadata.S3StreamConstant;
import com.automq.stream.s3.metadata.StreamOffsetRange;
import com.automq.stream.s3.objects.CommitStreamObjectRequest;
import com.automq.stream.s3.objects.CommitWALObjectRequest;
import com.automq.stream.s3.objects.CommitWALObjectResponse;
import com.automq.stream.s3.objects.ObjectManager;
import com.automq.stream.s3.objects.ObjectStreamRange;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class S3ObjectManager implements ObjectManager {
    private static final Logger LOGGER = LoggerFactory.getLogger(S3ObjectManager.class);
    private final StoreMetadataService metaService;

    public S3ObjectManager(StoreMetadataService metaService) {
        this.metaService = metaService;
    }

    @Override
    public CompletableFuture<Long> prepareObject(int count, long ttl) {
        // Covert ttl from milliseconds to minutes
        int minutes = (int) TimeUnit.MILLISECONDS.toMinutes(ttl);
        return metaService.prepareS3Objects(count, minutes);
    }

    @Override
    public CompletableFuture<CommitWALObjectResponse> commitWALObject(CommitWALObjectRequest request) {
        // Build S3WALObject
        S3WALObject.Builder builder = S3WALObject.newBuilder();
        builder.setObjectId(request.getObjectId());
        builder.setSequenceId(request.getOrderId());
        builder.setObjectSize(request.getObjectSize());

        // Collect SubStream from request
        Map<Long, SubStream> subStreams = request
            .getStreamRanges()
            .stream()
            .collect(Collectors.toMap(ObjectStreamRange::getStreamId,
                range -> {
                    SubStream.Builder subStreamBuilder = SubStream.newBuilder();
                    subStreamBuilder.setStreamId(range.getStreamId());
                    subStreamBuilder.setStartOffset(range.getStartOffset());
                    subStreamBuilder.setEndOffset(range.getEndOffset());
                    return subStreamBuilder.build();
                }));

        builder.putAllSubStreams(subStreams);
        S3WALObject walObject = builder.build();

        // Build stream objects
        List<S3StreamObject> streamObjects = request.getStreamObjects().stream().map(streamObject -> {
            S3StreamObject.Builder streamObjectBuilder = S3StreamObject.newBuilder();
            streamObjectBuilder.setStreamId(streamObject.getStreamId());
            streamObjectBuilder.setStartOffset(streamObject.getStartOffset());
            streamObjectBuilder.setEndOffset(streamObject.getEndOffset());
            streamObjectBuilder.setObjectId(streamObject.getObjectId());
            streamObjectBuilder.setObjectSize(streamObject.getObjectSize());
            return streamObjectBuilder.build();
        }).toList();

        LOGGER.info("Commit WAL object: {}, along with split stream objects: {}, compacted objects: {}",
            walObject, streamObjects, request.getCompactedObjectIds());

        // Build compacted objects
        return metaService.commitWalObject(walObject, streamObjects, request.getCompactedObjectIds()).thenApply(resp -> new CommitWALObjectResponse());
    }

    @Override
    public CompletableFuture<Void> commitStreamObject(CommitStreamObjectRequest request) {
        // Build S3StreamObject
        S3StreamObject.Builder builder = S3StreamObject.newBuilder();
        builder.setStreamId(request.getStreamId());
        builder.setStartOffset(request.getStartOffset());
        builder.setEndOffset(request.getEndOffset());
        builder.setObjectId(request.getObjectId());
        builder.setObjectSize(request.getObjectSize());
        S3StreamObject object = builder.build();

        LOGGER.info("Commit stream object: {}, compacted objects: {}", object, request.getSourceObjectIds());

        return metaService.commitStreamObject(object, request.getSourceObjectIds());
    }

    @Override
    public CompletableFuture<List<S3ObjectMetadata>> getObjects(long streamId, long startOffset, long endOffset,
        int limit) {
        CompletableFuture<List<S3StreamObject>> sf = metaService.listStreamObjects(streamId, startOffset, endOffset, limit);
        CompletableFuture<List<S3WALObject>> wf = metaService.listWALObjects(streamId, startOffset, endOffset, limit);

        // Retrieve S3ObjectMetadata from stream objects and wal objects
        return sf.thenCombine(wf, (streamObjects, walObjects) -> {
            // Sort the streamObjects in ascending order of startOffset
            streamObjects.sort((o1, o2) -> (int) (o1.getStartOffset() - o2.getStartOffset()));
            // Sort the walObjects in ascending order of startOffset
            walObjects.sort((o1, o2) -> (int) (o1.getSubStreamsMap().get(streamId).getStartOffset() - o2.getSubStreamsMap().get(streamId).getStartOffset()));
            // Merge sort the two object lists
            List<S3ObjectMetadata> objectMetadatas = new ArrayList<>();
            long startOffsetPtr = startOffset;
            int sIndex = 0, wIndex = 0;
            while (true) {
                boolean moved = false;
                // Stream object has higher priority
                if (sIndex < streamObjects.size()) {
                    S3StreamObject streamObject = streamObjects.get(sIndex);
                    if (streamObject.getStartOffset() <= startOffsetPtr && streamObject.getEndOffset() > startOffsetPtr) {
                        objectMetadatas.add(convertFrom(streamObject));
                        startOffsetPtr = streamObject.getEndOffset();
                        sIndex++;
                        moved = true;
                    } else if (streamObject.getEndOffset() <= startOffsetPtr) {
                        // Don't need this stream object.
                        sIndex++;
                        moved = true;
                    }
                }

                if (wIndex < walObjects.size()) {
                    S3WALObject walObject = walObjects.get(wIndex);
                    long walStart = walObject.getSubStreamsMap().get(streamId).getStartOffset();
                    long walEnd = walObject.getSubStreamsMap().get(streamId).getEndOffset();
                    if (walStart <= startOffsetPtr && walEnd > startOffsetPtr) {
                        objectMetadatas.add(convertFrom(walObject));
                        startOffsetPtr = walEnd;
                        wIndex++;
                        moved = true;
                    } else if (walEnd <= startOffsetPtr) {
                        // Don't need this wal object.
                        wIndex++;
                        moved = true;
                    }
                }

                if (!moved) {
                    // No object can be added, break the loop
                    // Consider we need [100, 300), but the first object is [200, 300), then we need to break the loop
                    break;
                }

                if (objectMetadatas.size() >= limit) {
                    break;
                }

                if (sIndex >= streamObjects.size() && wIndex >= walObjects.size()) {
                    break;
                }
            }
            LOGGER.trace("Get objects from streamId: {}, startOffset: {}, endOffset: {}, limit: {}, result: {}",
                streamId, startOffset, endOffset, limit, objectMetadatas);
            return objectMetadatas;
        });
    }

    @Override
    public CompletableFuture<List<S3ObjectMetadata>> getServerObjects() {
        CompletableFuture<List<S3WALObject>> walObjects = metaService.listWALObjects();
        // Covert to the list of S3ObjectMetadata
        return walObjects.thenApply(objects -> objects.stream().map(this::convertFrom).toList());
    }

    @Override
    public CompletableFuture<List<S3ObjectMetadata>> getStreamObjects(long streamId, long startOffset, long endOffset,
        int limit) {
        CompletableFuture<List<S3StreamObject>> streamObjects = metaService.listStreamObjects(streamId, startOffset, endOffset, limit);
        return streamObjects.thenApply(objects -> objects.stream().map(this::convertFrom).toList());
    }

    private S3ObjectMetadata convertFrom(S3StreamObject object) {
        StreamOffsetRange offsetRange = new StreamOffsetRange(object.getStreamId(), object.getStartOffset(), object.getEndOffset());
        return new S3ObjectMetadata(object.getObjectId(), S3ObjectType.STREAM, List.of(offsetRange), object.getBaseDataTimestamp(),
            object.getCommittedTimestamp(), object.getObjectSize(), S3StreamConstant.INVALID_ORDER_ID);
    }

    private S3ObjectMetadata convertFrom(S3WALObject object) {
        List<StreamOffsetRange> offsetRanges = object.getSubStreamsMap().values()
            .stream()
            .map(subStream -> new StreamOffsetRange(subStream.getStreamId(), subStream.getStartOffset(), subStream.getEndOffset())).toList();
        return new S3ObjectMetadata(object.getObjectId(), S3ObjectType.WAL, offsetRanges, object.getBaseDataTimestamp(),
            object.getCommittedTimestamp(), object.getObjectSize(), object.getSequenceId());
    }
}