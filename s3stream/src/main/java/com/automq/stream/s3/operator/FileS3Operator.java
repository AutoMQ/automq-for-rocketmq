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

package com.automq.stream.s3.operator;

import com.automq.stream.s3.network.ThrottleStrategy;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import software.amazon.awssdk.services.s3.model.CompletedPart;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class FileS3Operator implements S3Operator {

    private final RandomAccessFile file;

    public FileS3Operator(String filePath) throws FileNotFoundException {
        this.file = new RandomAccessFile(filePath, "r");
    }

    @Override
    public void close() {
        try {
            this.file.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public CompletableFuture<ByteBuf> rangeRead(String path, long start, long end, ThrottleStrategy throttleStrategy) {
        int size = (int) (end - start);
        byte[] bytes = new byte[size];
        try {
            this.file.seek(start);
            this.file.read(bytes);
        } catch (IOException e) {
            return CompletableFuture.failedFuture(e);
        }
        ByteBuf byteBuf = PooledByteBufAllocator.DEFAULT.buffer(size);
        byteBuf.writeBytes(bytes);
        return CompletableFuture.completedFuture(byteBuf);
    }

    @Override
    public CompletableFuture<Void> write(String path, ByteBuf data, ThrottleStrategy throttleStrategy) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Writer writer(String path, ThrottleStrategy throttleStrategy) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<Void> delete(String path) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<List<String>> delete(List<String> objectKeys) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<String> createMultipartUpload(String path) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<CompletedPart> uploadPart(String path, String uploadId, int partNumber, ByteBuf data, ThrottleStrategy throttleStrategy) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<CompletedPart> uploadPartCopy(String sourcePath, String path, long start, long end, String uploadId, int partNumber) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<Void> completeMultipartUpload(String path, String uploadId, List<CompletedPart> parts) {
        throw new UnsupportedOperationException();
    }
}
