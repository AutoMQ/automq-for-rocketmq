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

package com.automq.stream.s3.wal;


import com.automq.stream.s3.wal.util.WALUtil;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static com.automq.stream.s3.wal.WriteAheadLog.AppendResult;

/**
 * A WriteBlockTask contains multiple records, and will be written to the WAL in one batch.
 */
public interface WriteBlockTask {
    /**
     * The start offset of this block.
     * Align to {@link WALUtil#BLOCK_SIZE}
     */
    long startOffset();

    /**
     * Append a record to this block.
     *
     * @param record The record including the header.
     * @param future The future of this record, which will be completed when the record is written to the WAL.
     * @return The start offset of this record.
     * @throws BlockFullException If the size of this block exceeds the limit.
     */
    long addRecord(ByteBuffer record, CompletableFuture<AppendResult.CallbackResult> future);

    /**
     * Futures of all records in this task.
     */
    List<CompletableFuture<AppendResult.CallbackResult>> futures();

    /**
     * The content of this block, which contains multiple records.
     */
    ByteBuffer data();

    void flushWALHeader(long windowMaxLength) throws IOException;

    class BlockFullException extends RuntimeException {
        public BlockFullException(String message) {
            super(message);
        }
    }
}
