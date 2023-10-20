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

import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class WriteBlockTaskImpl implements WriteBlockTask {

    /**
     * The soft limit of block size. (128 KiB)
     * TODO make it configurable
     */
    private static final long SOFT_BLOCK_SIZE_LIMIT = 1 << 17;

    private final long startOffset;
    /**
     * The max size of this block.
     * Any try to add a record to this block will fail and throw {@link BlockFullException} if the size of this block
     * exceeds this limit.
     */
    private final long maxSize;
    private final Flusher flusher;

    /**
     * The next offset to write in this block.
     * Align to {@link WALUtil#BLOCK_SIZE}
     */
    private long nextOffset = 0;
    private ByteBuffer data = ByteBuffer.allocate(0);
    private final List<CompletableFuture<WriteAheadLog.AppendResult.CallbackResult>> futures = new LinkedList<>();

    public WriteBlockTaskImpl(long startOffset, long maxSize, Flusher flusher) {
        this.startOffset = startOffset;
        this.maxSize = maxSize;
        this.flusher = flusher;
    }

    @Override
    public long startOffset() {
        return startOffset;
    }

    @Override
    public long addRecord(ByteBuffer record, CompletableFuture<WriteAheadLog.AppendResult.CallbackResult> future) throws BlockFullException {
        // TODO no need to align to block size
        long requiredSize = WALUtil.alignLargeByBlockSize(record.remaining());
        long requiredCapacity = nextOffset + requiredSize;

        if (requiredCapacity > maxSize) {
            throw new BlockFullException("The block is full. Required capacity: " + requiredCapacity + ", max size: " + maxSize);
        }
        // if there is no record in this block, we can write a record larger than SOFT_BLOCK_SIZE_LIMIT
        if (requiredCapacity > SOFT_BLOCK_SIZE_LIMIT && !futures.isEmpty()) {
            throw new BlockFullException("The block is almost full. Required capacity: " + requiredCapacity + ", max size: " + maxSize);
        }

        // TODO use composite buffer
        if (requiredCapacity > data.capacity()) {
            int newCapacity = Math.max(data.capacity() * 2, (int) requiredCapacity);
            ByteBuffer newData = ByteBuffer.allocate(newCapacity);
            data.flip();
            newData.put(data);
            data = newData;
        }

        long offset = nextOffset;
        nextOffset += requiredSize;
        data.position((int) offset);
        data.put(record);

        futures.add(future);
        return offset;
    }

    @Override
    public List<CompletableFuture<WriteAheadLog.AppendResult.CallbackResult>> futures() {
        return futures;
    }

    @Override
    public ByteBuffer data() {
        return data;
    }

    @Override
    public Flusher flusher() {
        return flusher;
    }
}
