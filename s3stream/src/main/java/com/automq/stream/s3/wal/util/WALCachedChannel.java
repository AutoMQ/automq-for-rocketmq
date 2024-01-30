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

package com.automq.stream.s3.wal.util;

import com.automq.stream.s3.DirectByteBufAlloc;
import io.netty.buffer.ByteBuf;
import java.io.IOException;

/**
 * A wrapper of {@link WALChannel} that caches for read to reduce I/O.
 */
public class WALCachedChannel implements WALChannel {

    private static final int DEFAULT_CACHE_SIZE = 1 << 20;

    private final WALChannel channel;
    private final int cacheSizeWant;

    private ByteBuf cache;
    private long cachePosition = -1;

    private WALCachedChannel(WALChannel channel, int cacheSizeWant) {
        this.channel = channel;
        this.cacheSizeWant = cacheSizeWant;
    }

    public static WALCachedChannel of(WALChannel channel) {
        return new WALCachedChannel(channel, DEFAULT_CACHE_SIZE);
    }

    public static WALCachedChannel of(WALChannel channel, int cacheSize) {
        return new WALCachedChannel(channel, cacheSize);
    }

    /**
     * As we use a common cache for all threads, we need to synchronize the read.
     */
    @Override
    public synchronized int read(ByteBuf dst, long position) throws IOException {
        long start = position;
        int length = dst.writableBytes();
        long end = position + length;
        ByteBuf cache = getCache();
        boolean fallWithinCache = cachePosition >= 0 && cachePosition <= start && end <= cachePosition + cache.readableBytes();
        if (!fallWithinCache) {
            cache.clear();
            cachePosition = start;
            if (cachePosition + cache.writableBytes() > channel.capacity()) {
                // The cache is larger than the channel capacity.
                cachePosition = channel.capacity() - cache.writableBytes();
                assert cachePosition >= 0;
            }
            channel.read(cache, cachePosition);
        }
        // Now the cache is ready.
        int relativePosition = (int) (start - cachePosition);
        dst.writeBytes(cache, relativePosition, length);
        return length;
    }

    @Override
    public void close() {
        releaseCache();
        this.channel.close();
    }

    /**
     * Release the cache if it is not null.
     * This method should be called when no more {@link #read}s will be called to release the allocated memory.
     */
    public synchronized void releaseCache() {
        if (this.cache != null) {
            this.cache.release();
            this.cache = null;
        }
        this.cachePosition = -1;
    }

    /**
     * Get the cache. If the cache is not initialized, initialize it.
     * Should be called under synchronized.
     */
    private ByteBuf getCache() {
        if (this.cache == null) {
            // Make sure the cache size is not larger than the channel capacity.
            int cacheSize = (int) Math.min(this.cacheSizeWant, this.channel.capacity());
            this.cache = DirectByteBufAlloc.byteBuffer(cacheSize);
        }
        return this.cache;
    }

    @Override
    public void open(CapacityReader reader) throws IOException {
        this.channel.open(reader);
    }

    @Override
    public long capacity() {
        return this.channel.capacity();
    }

    @Override
    public String path() {
        return this.channel.path();
    }

    @Override
    public void write(ByteBuf src, long position) throws IOException {
        this.channel.write(src, position);
    }

    @Override
    public void flush() throws IOException {
        this.channel.flush();
    }
}
