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

import apache.rocketmq.controller.v1.S3StreamObject;
import apache.rocketmq.controller.v1.S3StreamSetObject;
import apache.rocketmq.controller.v1.SubStream;
import apache.rocketmq.controller.v1.SubStreams;
import com.automq.rocketmq.metadata.api.StoreMetadataService;
import com.automq.stream.s3.metadata.S3ObjectMetadata;
import com.automq.stream.s3.metadata.S3ObjectType;
import com.automq.stream.s3.objects.CompactStreamObjectRequest;
import com.automq.stream.s3.objects.CommitStreamSetObjectRequest;
import com.automq.stream.s3.objects.ObjectStreamRange;
import com.automq.stream.s3.objects.StreamObject;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class S3ObjectManagerTest {
    @Mock
    private StoreMetadataService metadataService;
    @Captor
    ArgumentCaptor<S3StreamSetObject> streamSetObjectCaptor;
    @Captor
    ArgumentCaptor<S3StreamObject> streamObjectCaptor;
    @Captor
    ArgumentCaptor<List<S3StreamObject>> streamObjectsCaptor;

    private S3ObjectManager objectManager;

    @BeforeEach
    void setUp() {
        objectManager = new S3ObjectManager(metadataService);
    }

    @Test
    void prepareObject() {
        when(metadataService.prepareS3Objects(anyInt(), anyInt())).thenReturn(CompletableFuture.completedFuture(100L));
        // 2 Minutes retention
        CompletableFuture<Long> preparedObject = objectManager.prepareObject(10, 1000 * 60 * 2);
        assertEquals(100L, preparedObject.join());
        verify(metadataService).prepareS3Objects(10, Math.max(2, S3ObjectManager.MIN_OBJECT_TTL_MINUTES));

        preparedObject = objectManager.prepareObject(10, 1000 * 60 * 40);
        assertEquals(100L, preparedObject.join());
        verify(metadataService).prepareS3Objects(10, Math.max(40, S3ObjectManager.MIN_OBJECT_TTL_MINUTES));
    }

    @Test
    void commitStreamSetObject() {
        when(metadataService.commitStreamSetObject(any(), any(), any())).thenReturn(CompletableFuture.completedFuture(null));
        // Build a CommitStreamSetObjectRequest for testing
        CommitStreamSetObjectRequest streamSetObjectRequest = new CommitStreamSetObjectRequest();
        streamSetObjectRequest.setObjectId(100L);
        streamSetObjectRequest.setOrderId(200L);
        streamSetObjectRequest.setObjectSize(300L);
        List<Long> compactedObjectIds = Collections.singletonList(400L);
        streamSetObjectRequest.setCompactedObjectIds(compactedObjectIds);
        List<ObjectStreamRange> streamRanges = Collections.singletonList(
            new ObjectStreamRange(1, 1, 100, 200, 300));
        streamSetObjectRequest.setStreamRanges(streamRanges);

        StreamObject streamObject = new StreamObject();
        streamObject.setObjectId(2);
        streamObject.setStreamId(1);
        streamObject.setStartOffset(1000);
        streamObject.setEndOffset(2000);
        streamSetObjectRequest.setStreamObjects(Collections.singletonList(streamObject));

        objectManager.commitStreamSetObject(streamSetObjectRequest);
        verify(metadataService).commitStreamSetObject(streamSetObjectCaptor.capture(), streamObjectsCaptor.capture(), eq(compactedObjectIds));

        S3StreamSetObject streamSetObject = streamSetObjectCaptor.getValue();
        assertEquals(100L, streamSetObject.getObjectId());
        assertEquals(200L, streamSetObject.getSequenceId());
        assertEquals(300L, streamSetObject.getObjectSize());

        // Bellow fields are not set in commitStreamSetObject method
        assertEquals(0, streamSetObject.getBrokerId());
        assertEquals(0, streamSetObject.getCommittedTimestamp());
        assertEquals(0, streamSetObject.getBaseDataTimestamp());

        List<S3StreamObject> streamObjects = streamObjectsCaptor.getValue();
        assertEquals(1, streamObjects.size());
        S3StreamObject sObject = streamObjects.get(0);
        assertEquals(1, sObject.getStreamId());
        assertEquals(1000, sObject.getStartOffset());
        assertEquals(2000, sObject.getEndOffset());
    }

    @Test
    void commitStreamObject() {
        CompactStreamObjectRequest request = new CompactStreamObjectRequest(1L, 1000, 2000, 0L, 100, 1000, List.of(10L));
        when(metadataService.compactStreamObject(any(), any())).thenReturn(CompletableFuture.completedFuture(null));

        objectManager.compactStreamObject(request);

        verify(metadataService).compactStreamObject(streamObjectCaptor.capture(), eq(List.of(10L)));

        S3StreamObject streamObject = streamObjectCaptor.getValue();
        assertEquals(1L, streamObject.getObjectId());
        assertEquals(100, streamObject.getStartOffset());
        assertEquals(1000, streamObject.getEndOffset());
        assertEquals(2000, streamObject.getStreamId());
        assertEquals(1000, streamObject.getObjectSize());

        assertEquals(0, streamObject.getBaseDataTimestamp());
        assertEquals(0, streamObject.getCommittedTimestamp());
    }

    @Test
    void getObjects_OffsetTooMin() {
        // StreamSetObject contains stream range [20, 100)
        S3StreamSetObject.Builder builder = S3StreamSetObject.newBuilder();
        var subStreams = Collections.singletonMap(1L,
            SubStream.newBuilder()
                .setStartOffset(20)
                .setEndOffset(100)
                .build());
        builder.setSubStreams(SubStreams.newBuilder().putAllSubStreams(subStreams));

        // StreamSetObject contains stream range [20, 100) and no stream object
        when(metadataService.listStreamSetObjects(anyLong(), anyLong(), anyLong(), anyInt()))
            .thenReturn(CompletableFuture.completedFuture(new ArrayList<>(Collections.singletonList(builder.build()))));
        when(metadataService.listStreamObjects(anyLong(), anyLong(), anyLong(), anyInt()))
            .thenReturn(CompletableFuture.completedFuture(new ArrayList<>()));
        when(metadataService.listObjects(anyLong(), anyLong(), anyLong(), anyInt()))
            .thenAnswer(ink -> {
                long streamId = ink.getArgument(0);
                long startOffset = ink.getArgument(1);
                long endOffset = ink.getArgument(2);
                int limit = ink.getArgument(3);
                return metadataService.listStreamObjects(streamId, startOffset, endOffset, limit).thenCombine(
                    metadataService.listStreamSetObjects(streamId, startOffset, endOffset, limit),
                    (sObjects, wObjects) -> {
                        return new ImmutablePair(sObjects, wObjects);
                    }
                );
            });

        CompletableFuture<List<S3ObjectMetadata>> objects = objectManager.getObjects(1, 10, 100, 10);
        // Assert a runtime exception is thrown
        objects.handle((rst, ex) -> {
            assertTrue(ex.getCause() instanceof RuntimeException);
            return null;
        }).join();

        // Stream object contains stream range [20, 100) and no StreamSet object
        S3StreamObject.Builder soBuilder = S3StreamObject.newBuilder();
        soBuilder.setStreamId(1);
        soBuilder.setStartOffset(20);
        soBuilder.setEndOffset(100);
        soBuilder.setObjectId(1);
        when(metadataService.listStreamObjects(anyLong(), anyLong(), anyLong(), anyInt()))
            .thenReturn(CompletableFuture.completedFuture(new ArrayList<>(Collections.singletonList(soBuilder.build()))));
        when(metadataService.listStreamSetObjects(anyLong(), anyLong(), anyLong(), anyInt()))
            .thenReturn(CompletableFuture.completedFuture(new ArrayList<>()));

        objects = objectManager.getObjects(1, 10, 100, 10);
        objects.handle((rst, ex) -> {
            assertTrue(ex.getCause() instanceof RuntimeException);
            return null;
        }).join();
    }

    @Test
    void getObjects_Normal() {
        // StreamSetObject1 contains stream ranges:
        // (1, [100, 200))
        // (2, [200, 300))
        // (3, [300, 400))
        // StreamSetObject2 contains stream ranges:
        // (1, [400, 500))
        // StreamObject contains stream range:
        // (1, [200, 400))

        List<S3StreamSetObject> streamSetObjects = new ArrayList<>();
        // Build StreamSetObject1
        S3StreamSetObject.Builder builder = S3StreamSetObject.newBuilder();
        List<SubStream> subStreams = new ArrayList<>();
        subStreams.add(SubStream.newBuilder().setStreamId(1L).setStartOffset(100).setEndOffset(200).build());
        subStreams.add(SubStream.newBuilder().setStreamId(2L).setStartOffset(200).setEndOffset(300).build());
        subStreams.add(SubStream.newBuilder().setStreamId(3L).setStartOffset(300).setEndOffset(400).build());
        builder.setSubStreams(SubStreams.newBuilder()
            .putAllSubStreams(subStreams.stream()
                .collect(Collectors.toMap(SubStream::getStreamId, s -> s)))
            .build());
        builder.setObjectId(1);

        streamSetObjects.add(builder.build());

        builder.clearSubStreams();
        subStreams.clear();
        subStreams.add(SubStream.newBuilder().setStreamId(1L).setStartOffset(400).setEndOffset(500).build());
        builder.setObjectId(2);
        builder.setSubStreams(SubStreams.newBuilder().putAllSubStreams(subStreams
                .stream()
                .collect(Collectors.toMap(SubStream::getStreamId, s -> s)))
            .build());
        streamSetObjects.add(builder.build());

        when(metadataService.listStreamSetObjects(anyLong(), anyLong(), anyLong(), anyInt()))
            .thenReturn(CompletableFuture.completedFuture(List.copyOf(streamSetObjects)));

        // Build StreamObject
        S3StreamObject.Builder soBuilder = S3StreamObject.newBuilder();
        soBuilder.setStreamId(1);
        soBuilder.setStartOffset(200);
        soBuilder.setEndOffset(400);
        soBuilder.setObjectId(3);
        List<S3StreamObject> streamObjects = new ArrayList<>(Collections.singletonList(soBuilder.build()));
        when(metadataService.listStreamObjects(anyLong(), anyLong(), anyLong(), anyInt()))
            .thenReturn(CompletableFuture.completedFuture(List.copyOf(streamObjects)));
        when(metadataService.listObjects(anyLong(), anyLong(), anyLong(), anyInt()))
            .thenAnswer(ink -> {
                long streamId = ink.getArgument(0);
                long startOffset = ink.getArgument(1);
                long endOffset = ink.getArgument(2);
                int limit = ink.getArgument(3);
                return metadataService.listStreamObjects(streamId, startOffset, endOffset, limit).thenCombine(
                    metadataService.listStreamSetObjects(streamId, startOffset, endOffset, limit),
                    (sObjects, wObjects) -> new ImmutablePair(sObjects, wObjects)
                );
            });
        objectManager.getObjects(1, 100, 500, 3).thenAccept(objects -> {
            assertEquals(3, objects.size());
            S3ObjectMetadata obj1 = objects.get(0);
            assertEquals(1, obj1.objectId());
            S3ObjectMetadata obj2 = objects.get(1);
            assertEquals(3, obj2.objectId());
            S3ObjectMetadata obj3 = objects.get(2);
            assertEquals(2, obj3.objectId());

            assertEquals(S3ObjectType.STREAM_SET, obj1.getType());
            assertEquals(S3ObjectType.STREAM, obj2.getType());
            assertEquals(S3ObjectType.STREAM_SET, obj3.getType());

            assertEquals(100, obj1.startOffset());
            assertEquals(3, obj1.getOffsetRanges().size());
            assertEquals(200, obj2.startOffset());
            assertEquals(400, obj2.endOffset());
            assertEquals(400, obj3.startOffset());
            assertEquals(1, obj3.getOffsetRanges().size());
        }).join();

        objectManager.getObjects(1, 100, 500, 2).thenAccept(objects -> {
            assertEquals(2, objects.size());
            assertEquals(1, objects.get(0).objectId());
            assertEquals(3, objects.get(1).objectId());
        }).join();

        objectManager.getObjects(1, 150, 450, 3).thenAccept(objects -> {
            assertEquals(3, objects.size());
            assertEquals(1, objects.get(0).objectId());
            assertEquals(3, objects.get(1).objectId());
            assertEquals(2, objects.get(2).objectId());
        }).join();
    }
}