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

import com.automq.stream.s3.TestUtils;
import com.automq.stream.utils.ThreadUtils;
import com.automq.stream.utils.Threads;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("S3Unit")
@EnabledOnOs(OS.LINUX)
public class WALBlockDeviceChannelTest {

    @Test
    public void testSingleThreadWriteBasic() throws IOException {
        final int size = 4096 + 1;
        final int count = 100;
        final long capacity = WALUtil.alignLargeByBlockSize(size) * count;

        WALBlockDeviceChannel channel = new WALBlockDeviceChannel(TestUtils.tempFilePath(), capacity);
        channel.open();

        for (int i = 0; i < count; i++) {
            ByteBuf data = TestUtils.random(size);
            long pos = WALUtil.alignLargeByBlockSize(size) * i;
            channel.writeAndFlush(data, pos);
        }

        channel.close();
    }

    @Test
    public void testSingleThreadWriteComposite() throws IOException {
        final int maxSize = 4096 * 4;
        final int count = 100;
        final int batch = 10;
        final long capacity = WALUtil.alignLargeByBlockSize(maxSize) * count;

        WALBlockDeviceChannel channel = new WALBlockDeviceChannel(TestUtils.tempFilePath(), capacity);
        channel.open();

        for (int i = 0; i < count; i += batch) {
            CompositeByteBuf data = Unpooled.compositeBuffer();
            for (int j = 0; j < batch; j++) {
                int size = ThreadLocalRandom.current().nextInt(1, maxSize);
                data.addComponent(true, TestUtils.random(size));
            }
            long pos = WALUtil.alignLargeByBlockSize(maxSize) * i;
            channel.writeAndFlush(data, pos);
        }

        channel.close();
    }

    @Test
    public void testMultiThreadWrite() throws IOException, InterruptedException {
        final int size = 4096 + 1;
        final int count = 100;
        final int threads = 8;
        final long capacity = WALUtil.alignLargeByBlockSize(size) * count;

        WALBlockDeviceChannel channel = new WALBlockDeviceChannel(TestUtils.tempFilePath(), capacity);
        channel.open();

        ExecutorService executor = Threads.newFixedThreadPool(threads,
                ThreadUtils.createThreadFactory("test-block-device-channel-write-%d", false), null);
        for (int i = 0; i < count; i++) {
            final int index = i;
            executor.submit(() -> {
                ByteBuf data = TestUtils.random(size);
                long pos = WALUtil.alignLargeByBlockSize(size) * index;
                try {
                    channel.writeAndFlush(data, pos);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
        }
        executor.shutdown();
        assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));

        channel.close();
    }

    @Test
    public void testWriteInvalidBufferSize() throws IOException {
        WALBlockDeviceChannel channel = new WALBlockDeviceChannel(TestUtils.tempFilePath(), 1 << 20);
        channel.open();

        ByteBuf data = TestUtils.random(42);
        assertThrows(AssertionError.class, () -> channel.writeAndFlush(data, 0));

        channel.close();
    }

    @Test
    public void testWriteInvalidPosition() throws IOException {
        WALBlockDeviceChannel channel = new WALBlockDeviceChannel(TestUtils.tempFilePath(), 1 << 20);
        channel.open();

        ByteBuf data = TestUtils.random(4096);
        assertThrows(AssertionError.class, () -> channel.writeAndFlush(data, 42));

        channel.close();
    }

    @Test
    public void testWriteOutOfBound() throws IOException {
        WALBlockDeviceChannel channel = new WALBlockDeviceChannel(TestUtils.tempFilePath(), 4096);
        channel.open();

        ByteBuf data = TestUtils.random(4096);
        // TODO
        assertThrows(AssertionError.class, () -> channel.writeAndFlush(data, 8192));

        channel.close();
    }
}
