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

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

public class WALFileChannel implements WALChannel {
    final String filePath;
    long fileCapacityWant;
    long fileCapacityFact = 0;
    RandomAccessFile randomAccessFile;
    FileChannel fileChannel;
    MappedByteBuffer mappedByteBuffer;

    public WALFileChannel(String filePath, long fileCapacityWant) {
        this.filePath = filePath;
        this.fileCapacityWant = fileCapacityWant;
        if (this.fileCapacityWant > 1 << 30) {
            this.fileCapacityWant = 1 << 30;
        }
    }

    @Override
    public void open() throws IOException {
        File file = new File(filePath);
        if (file.exists()) {
            randomAccessFile = new RandomAccessFile(file, "rw");
            fileCapacityFact = randomAccessFile.length();
            if (fileCapacityFact != fileCapacityWant) {
                throw new IOException("file " + filePath + " capacity " + fileCapacityFact + " not equal to requested " + fileCapacityWant);
            }
        } else {
            if (!file.getParentFile().exists()) {
                if (!file.getParentFile().mkdirs()) {
                    throw new IOException("mkdirs " + file.getParentFile() + " fail");
                }
            }
            if (!file.createNewFile()) {
                throw new IOException("create " + filePath + " fail");
            }
            if (!file.setWritable(true)) {
                throw new IOException("set " + filePath + " writable fail");
            }
            randomAccessFile = new RandomAccessFile(file, "rw");
            randomAccessFile.setLength(fileCapacityWant);
            fileCapacityFact = fileCapacityWant;
        }

        fileChannel = randomAccessFile.getChannel();
        mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, fileCapacityFact);
    }

    @Override
    public void close() {
        try {
            fileChannel.close();
            randomAccessFile.close();
        } catch (IOException ignored) {
        }
    }

    @Override
    public long capacity() {
        return fileCapacityFact;
    }

    @Override
    public void write(ByteBuf src, long position) throws IOException {
        assert src.readableBytes() + position <= fileCapacityFact;
        ByteBuffer[] nioBuffers = src.nioBuffers();
        int pos = (int) position;
        for (ByteBuffer nioBuffer : nioBuffers) {
            mappedByteBuffer.put(pos, nioBuffer, nioBuffer.position(), nioBuffer.remaining());
            pos += nioBuffer.remaining();
        }
        mappedByteBuffer.force();
    }

    @Override
    public int read(ByteBuf dst, long position) throws IOException {
        ByteBuffer nioBuffer = dst.nioBuffer(dst.writerIndex(), dst.writableBytes());
        int bytesRead = read(nioBuffer, position);
        dst.writerIndex(dst.writerIndex() + bytesRead);
        return bytesRead;
    }

    private int read(ByteBuffer dst, long position) throws IOException {
        int bytesRead = 0;
        while (dst.hasRemaining()) {
            int read = fileChannel.read(dst, position + bytesRead);
            if (read == -1) {
                // EOF
                break;
            }
            bytesRead += read;
        }
        return bytesRead;
    }
}
