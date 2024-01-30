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

import io.netty.buffer.ByteBuf;
import java.io.IOException;

/**
 * A wrapper of {@link WALChannel} that caches for read to reduce I/O.
 */
public class WALCachedChannel implements WALChannel {

    private static final int DEFAULT_CACHE_SIZE = 1 << 20;

    private final WALChannel channel;
    private final int cacheSize;

    private WALCachedChannel(WALChannel channel, int cacheSize) {
        this.channel = channel;
        this.cacheSize = cacheSize;
    }

    public static WALCachedChannel of(WALChannel channel) {
        return new WALCachedChannel(channel, DEFAULT_CACHE_SIZE);
    }

    public static WALCachedChannel of(WALChannel channel, int cacheSize) {
        return new WALCachedChannel(channel, cacheSize);
    }

    @Override
    public int read(ByteBuf dst, long position) throws IOException {
        // TODO
        return this.channel.read(dst, position);
    }

    @Override
    public void open(CapacityReader reader) throws IOException {
        this.channel.open(reader);
    }

    @Override
    public void close() {
        // TODO: maybe release cache
        this.channel.close();
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
