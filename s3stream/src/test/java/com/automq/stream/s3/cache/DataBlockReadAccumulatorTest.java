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

package com.automq.stream.s3.cache;

import com.automq.stream.s3.DataBlockIndex;
import com.automq.stream.s3.ObjectReader;
import com.automq.stream.s3.StreamDataBlock;
import com.automq.stream.s3.TestUtils;
import com.automq.stream.s3.model.StreamRecordBatch;
import com.automq.stream.utils.CloseableIterator;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class DataBlockReadAccumulatorTest {

    private static StreamRecordBatch newRecord(long streamId, long offset, int count, int size) {
        return new StreamRecordBatch(streamId, 0, offset, count, TestUtils.random(size));
    }

    @Test
    public void test() throws ExecutionException, InterruptedException, TimeoutException {
        DataBlockReadAccumulator accumulator = new DataBlockReadAccumulator();

        ObjectReader reader = mock(ObjectReader.class);
        DataBlockIndex dataBlockIndex = new DataBlockIndex(10, 0, 12, 2, 10, 100);
        StreamDataBlock streamDataBlock = new StreamDataBlock(1, dataBlockIndex);
        CompletableFuture<ObjectReader.DataBlockGroup> readerCf = new CompletableFuture<>();
        when(reader.read(eq(dataBlockIndex))).thenReturn(readerCf);

        List<DataBlockReadAccumulator.ReserveResult> reserveResults = accumulator.reserveDataBlock(List.of(new ImmutablePair<>(reader, streamDataBlock)));
        Assertions.assertEquals(1, reserveResults.size());
        Assertions.assertEquals(100, reserveResults.get(0).reserveSize());

        List<DataBlockReadAccumulator.ReserveResult> reserveResults2 = accumulator.reserveDataBlock(List.of(new ImmutablePair<>(reader, streamDataBlock)));
        Assertions.assertEquals(1, reserveResults2.size());
        Assertions.assertEquals(0, reserveResults2.get(0).reserveSize());

        accumulator.readDataBlock(reader, dataBlockIndex);

        ObjectReader.DataBlockGroup dataBlockGroup = mock(ObjectReader.DataBlockGroup.class);
        List<StreamRecordBatch> records = List.of(
            newRecord(10, 10, 2, 1),
            newRecord(10, 12, 2, 1)
        );
        when(dataBlockGroup.recordCount()).thenReturn(2);
        when(dataBlockGroup.iterator()).thenAnswer(args -> {
            Iterator<StreamRecordBatch> it = records.iterator();
            return new CloseableIterator<StreamRecordBatch>() {

                @Override
                public boolean hasNext() {
                    return it.hasNext();
                }

                @Override
                public StreamRecordBatch next() {
                    return it.next();
                }

                @Override
                public void close() {

                }
            };
        });
        when(dataBlockGroup.recordCount()).thenReturn(2);
        readerCf.complete(dataBlockGroup);

        verify(reader, times(1)).read(any());

        CompletableFuture<DataBlockRecords> dataBlockCf1 = reserveResults.get(0).cf();
        CompletableFuture<DataBlockRecords> dataBlockCf2 = reserveResults2.get(0).cf();
        assertEquals(2, dataBlockCf1.get(1, TimeUnit.SECONDS).records().size());
        assertEquals(12, dataBlockCf1.get(1, TimeUnit.SECONDS).records().get(1).getBaseOffset());
        dataBlockCf1.get().release();
        assertEquals(2, dataBlockCf2.get(1, TimeUnit.SECONDS).records().size());
        dataBlockCf2.get().release();

        // next round read, expected new read
        List<DataBlockReadAccumulator.ReserveResult> reserveResults3 = accumulator.reserveDataBlock(List.of(new ImmutablePair<>(reader, streamDataBlock)));
        Assertions.assertEquals(1, reserveResults3.size());
        Assertions.assertEquals(100, reserveResults3.get(0).reserveSize());
        accumulator.readDataBlock(reader, dataBlockIndex);
        verify(reader, times(2)).read(any());
        reserveResults3.get(0).cf().get().release();
    }

}
