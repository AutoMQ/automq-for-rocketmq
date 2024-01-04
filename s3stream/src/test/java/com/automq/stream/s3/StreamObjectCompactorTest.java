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

package com.automq.stream.s3;

import com.automq.stream.s3.metadata.S3ObjectMetadata;
import com.automq.stream.s3.metadata.S3ObjectType;
import com.automq.stream.s3.metadata.StreamOffsetRange;
import com.automq.stream.s3.model.StreamRecordBatch;
import com.automq.stream.s3.objects.CompactStreamObjectRequest;
import com.automq.stream.s3.objects.ObjectManager;
import com.automq.stream.s3.operator.MemoryS3Operator;
import com.automq.stream.s3.operator.S3Operator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class StreamObjectCompactorTest {

    private ObjectManager objectManager;
    private S3Operator s3Operator;
    private S3Stream stream;
    private final long streamId = 233L;

    @BeforeEach
    void setUp() {
        objectManager = Mockito.mock(ObjectManager.class);
        s3Operator = new MemoryS3Operator();
        stream = Mockito.mock(S3Stream.class);
    }

    @Test
    public void testCompact() throws ExecutionException, InterruptedException {
        // prepare object
        List<S3ObjectMetadata> objects = new LinkedList<>();
        {
            // object-1: offset 10~15
            ObjectWriter writer = ObjectWriter.writer(1, s3Operator, Integer.MAX_VALUE, Integer.MAX_VALUE);
            writer.write(233L, List.of(
                    newRecord(10L, 1, 1024),
                    newRecord(11L, 1, 1024),
                    newRecord(12L, 1, 1024)
            ));
            writer.write(233L, List.of(
                    newRecord(13L, 1, 1024),
                    newRecord(14L, 1, 1024),
                    newRecord(15L, 1, 1024)
            ));
            writer.close().get();
            objects.add(new S3ObjectMetadata(1, S3ObjectType.STREAM, List.of(new StreamOffsetRange(streamId, 10, 16)),
                    System.currentTimeMillis(), System.currentTimeMillis(), writer.size(), 1));
        }
        {
            // object-2: offset 16~17
            ObjectWriter writer = ObjectWriter.writer(2, s3Operator, Integer.MAX_VALUE, Integer.MAX_VALUE);
            writer.write(233L, List.of(
                    newRecord(16L, 1, 1024)
            ));
            writer.write(233L, List.of(
                    newRecord(17L, 1, 1024)
            ));
            writer.close().get();
            objects.add(new S3ObjectMetadata(2, S3ObjectType.STREAM, List.of(new StreamOffsetRange(streamId, 16, 18)),
                    System.currentTimeMillis(), System.currentTimeMillis(), writer.size(), 2));
        }
        {
            // object-3: offset 30
            ObjectWriter writer = ObjectWriter.writer(3, s3Operator, Integer.MAX_VALUE, Integer.MAX_VALUE);
            writer.write(233L, List.of(
                    newRecord(30L, 1, 1024)
            ));
            writer.close().get();
            objects.add(new S3ObjectMetadata(3, S3ObjectType.STREAM, List.of(new StreamOffsetRange(streamId, 30, 31)),
                    System.currentTimeMillis(), System.currentTimeMillis(), writer.size(), 3));
        }
        {
            // object-4: offset 31-32
            ObjectWriter writer = ObjectWriter.writer(4, s3Operator, Integer.MAX_VALUE, Integer.MAX_VALUE);
            writer.write(233L, List.of(
                    newRecord(31L, 1, 1024)
            ));
            writer.write(233L, List.of(
                    newRecord(32L, 1, 1024)
            ));
            writer.close().get();
            objects.add(new S3ObjectMetadata(4, S3ObjectType.STREAM, List.of(new StreamOffsetRange(streamId, 31, 33)),
                    System.currentTimeMillis(), System.currentTimeMillis(), writer.size(), 4));
        }
        when(objectManager.getStreamObjects(eq(streamId), eq(14L), eq(32L), eq(Integer.MAX_VALUE)))
                .thenReturn(CompletableFuture.completedFuture(objects));
        AtomicLong nextObjectId = new AtomicLong(5);
        doAnswer(invocationOnMock -> CompletableFuture.completedFuture(nextObjectId.getAndIncrement())).when(objectManager).prepareObject(anyInt(), anyLong());
        when(objectManager.compactStreamObject(any())).thenReturn(CompletableFuture.completedFuture(null));
        when(stream.streamId()).thenReturn(streamId);
        when(stream.startOffset()).thenReturn(14L);
        when(stream.confirmOffset()).thenReturn(32L);

        StreamObjectCompactor task = StreamObjectCompactor.builder().objectManager(objectManager).s3Operator(s3Operator)
                .maxStreamObjectSize(1024 * 1024 * 1024).stream(stream).build();
        task.compact();

        ArgumentCaptor<CompactStreamObjectRequest> ac = ArgumentCaptor.forClass(CompactStreamObjectRequest.class);
        verify(objectManager, times(2)).compactStreamObject(ac.capture());

        // verify compact request
        List<CompactStreamObjectRequest> requests = ac.getAllValues();
        CompactStreamObjectRequest req1 = requests.get(0);
        assertEquals(5, req1.getObjectId());
        assertEquals(233L, req1.getStreamId());
        assertEquals(13L, req1.getStartOffset());
        assertEquals(18L, req1.getEndOffset());
        assertEquals(List.of(1L, 2L), req1.getSourceObjectIds());

        CompactStreamObjectRequest req2 = requests.get(1);
        assertEquals(6, req2.getObjectId());
        assertEquals(233L, req2.getStreamId());
        assertEquals(30L, req2.getStartOffset());
        assertEquals(33L, req2.getEndOffset());
        assertEquals(List.of(3L, 4L), req2.getSourceObjectIds());

        // verify compacted object record
        {
            ObjectReader objectReader = new ObjectReader(new S3ObjectMetadata(5, req1.getObjectSize(), S3ObjectType.STREAM), s3Operator);
            assertEquals(3, objectReader.basicObjectInfo().get().blockCount());
            ObjectReader.FindIndexResult rst = objectReader.find(streamId, 13L, 18L).get();
            assertEquals(3, rst.streamDataBlocks().size());
            ObjectReader.DataBlock dataBlock1 = objectReader.read(rst.streamDataBlocks().get(0).dataBlockIndex()).get();
            try (dataBlock1) {
                assertEquals(3, dataBlock1.recordCount());
                Iterator<StreamRecordBatch> it = dataBlock1.iterator();
                assertEquals(13L, it.next().getBaseOffset());
                assertEquals(14L, it.next().getBaseOffset());
                assertEquals(15L, it.next().getBaseOffset());
                assertFalse(it.hasNext());
            }
            ObjectReader.DataBlock dataBlock2 = objectReader.read(rst.streamDataBlocks().get(1).dataBlockIndex()).get();
            try (dataBlock2) {
                assertEquals(1, dataBlock2.recordCount());
                Iterator<StreamRecordBatch> it = dataBlock2.iterator();
                assertEquals(16L, it.next().getBaseOffset());
            }
            ObjectReader.DataBlock dataBlock3 = objectReader.read(rst.streamDataBlocks().get(2).dataBlockIndex()).get();
            try (dataBlock3) {
                assertEquals(1, dataBlock3.recordCount());
                Iterator<StreamRecordBatch> it = dataBlock3.iterator();
                assertEquals(17L, it.next().getBaseOffset());
            }
            objectReader.close();
        }
        {
            ObjectReader objectReader = new ObjectReader(new S3ObjectMetadata(6, req2.getObjectSize(), S3ObjectType.STREAM), s3Operator);
            assertEquals(3, objectReader.basicObjectInfo().get().blockCount());
            ObjectReader.FindIndexResult rst = objectReader.find(streamId, 30L, 33L).get();
            assertEquals(3, rst.streamDataBlocks().size());
            ObjectReader.DataBlock dataBlock1 = objectReader.read(rst.streamDataBlocks().get(0).dataBlockIndex()).get();
            try (dataBlock1) {
                assertEquals(1, dataBlock1.recordCount());
                Iterator<StreamRecordBatch> it = dataBlock1.iterator();
                assertEquals(30L, it.next().getBaseOffset());
                assertFalse(it.hasNext());
            }
            ObjectReader.DataBlock dataBlock2 = objectReader.read(rst.streamDataBlocks().get(1).dataBlockIndex()).get();
            try (dataBlock2) {
                assertEquals(1, dataBlock2.recordCount());
                Iterator<StreamRecordBatch> it = dataBlock2.iterator();
                assertEquals(31L, it.next().getBaseOffset());
            }
            ObjectReader.DataBlock dataBlock3 = objectReader.read(rst.streamDataBlocks().get(2).dataBlockIndex()).get();
            try (dataBlock3) {
                assertEquals(1, dataBlock3.recordCount());
                Iterator<StreamRecordBatch> it = dataBlock3.iterator();
                assertEquals(32L, it.next().getBaseOffset());
            }
            objectReader.close();
        }
    }

    @Test
    public void testGroup() {
        List<S3ObjectMetadata> objects = List.of(
                new S3ObjectMetadata(2, S3ObjectType.STREAM, List.of(new StreamOffsetRange(streamId, 16, 18)),
                        System.currentTimeMillis(), System.currentTimeMillis(), 1024, 2),

                new S3ObjectMetadata(3, S3ObjectType.STREAM, List.of(new StreamOffsetRange(streamId, 18, 19)),
                        System.currentTimeMillis(), System.currentTimeMillis(), 1, 3),
                new S3ObjectMetadata(4, S3ObjectType.STREAM, List.of(new StreamOffsetRange(streamId, 19, 20)),
                        System.currentTimeMillis(), System.currentTimeMillis(), 1, 4),

                new S3ObjectMetadata(5, S3ObjectType.STREAM, List.of(new StreamOffsetRange(streamId, 30, 31)),
                        System.currentTimeMillis(), System.currentTimeMillis(), 1, 5),
                new S3ObjectMetadata(6, S3ObjectType.STREAM, List.of(new StreamOffsetRange(streamId, 31, 32)),
                        System.currentTimeMillis(), System.currentTimeMillis(), 1, 6)
        );
        List<List<S3ObjectMetadata>> groups = StreamObjectCompactor.group0(objects, 512);
        assertEquals(3, groups.size());
        assertEquals(List.of(2L), groups.get(0).stream().map(S3ObjectMetadata::objectId).collect(Collectors.toList()));
        assertEquals(List.of(3L, 4L), groups.get(1).stream().map(S3ObjectMetadata::objectId).collect(Collectors.toList()));
        assertEquals(List.of(5L, 6L), groups.get(2).stream().map(S3ObjectMetadata::objectId).collect(Collectors.toList()));
    }


    StreamRecordBatch newRecord(long offset, int count, int payloadSize) {
        return new StreamRecordBatch(streamId, 0, offset, count, TestUtils.random(payloadSize));
    }
}