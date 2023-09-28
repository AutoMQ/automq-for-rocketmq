/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.automq.rocketmq.metadata;

import apache.rocketmq.controller.v1.Code;
import apache.rocketmq.controller.v1.StreamMetadata;
import apache.rocketmq.controller.v1.StreamRole;
import com.automq.rocketmq.controller.exception.ControllerException;
import com.automq.rocketmq.controller.metadata.ControllerConfig;
import com.automq.rocketmq.controller.metadata.MetadataStore;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

class DefaultStoreMetadataServiceTest {

    private final ControllerConfig config;

    public DefaultStoreMetadataServiceTest() {
        this.config = Mockito.mock(ControllerConfig.class);
    }

    @Test
    public void testGetStreamId() {
        CompletableFuture<StreamMetadata> future = new CompletableFuture<>();
        StreamMetadata metadata = StreamMetadata.newBuilder()
            .setStreamId(1L).build();
        future.complete(metadata);
        MetadataStore metadataStore = Mockito.mock(MetadataStore.class);
        Mockito.when(metadataStore.getStream(ArgumentMatchers.anyLong(), ArgumentMatchers.anyInt(),
                ArgumentMatchers.nullable(Long.class), ArgumentMatchers.eq(StreamRole.STREAM_ROLE_DATA)))
            .thenReturn(future);

        ControllerConfig config = Mockito.mock(ControllerConfig.class);

        DefaultStoreMetadataService service = new DefaultStoreMetadataService(metadataStore, config);
        Assertions.assertEquals(1L, service.getStreamId(1L, 2));
    }

    @Test
    public void testGetStreamId_throws() {
        MetadataStore metadataStore = Mockito.mock(MetadataStore.class);
        CompletableFuture<StreamMetadata> future = new CompletableFuture<>();
        future.completeExceptionally(new ControllerException(Code.NOT_FOUND_VALUE, "not found"));
        Mockito.when(metadataStore.getStream(ArgumentMatchers.anyLong(), ArgumentMatchers.anyInt(),
                ArgumentMatchers.nullable(Long.class), ArgumentMatchers.eq(StreamRole.STREAM_ROLE_DATA)))
            .thenReturn(future);
        DefaultStoreMetadataService service = new DefaultStoreMetadataService(metadataStore, config);
        Assertions.assertEquals(-1L, service.getStreamId(1L, 2));
    }

    @Test
    public void testGetOperationLogStreamId() {
        MetadataStore metadataStore = Mockito.mock(MetadataStore.class);
        CompletableFuture<StreamMetadata> future = new CompletableFuture<>();
        StreamMetadata metadata = StreamMetadata.newBuilder()
            .setStreamId(1L).build();
        future.complete(metadata);
        Mockito.when(metadataStore.getStream(ArgumentMatchers.anyLong(), ArgumentMatchers.anyInt(),
                ArgumentMatchers.nullable(Long.class), ArgumentMatchers.eq(StreamRole.STREAM_ROLE_OPS)))
            .thenReturn(future);
        DefaultStoreMetadataService service = new DefaultStoreMetadataService(metadataStore, config);
        Assertions.assertEquals(1L, service.getOperationLogStreamId(1L, 2));
    }

    @Test
    public void testGetOperationLogStreamId_throws() {
        MetadataStore metadataStore = Mockito.mock(MetadataStore.class);
        CompletableFuture<StreamMetadata> future = new CompletableFuture<>();
        future.completeExceptionally(new ControllerException(Code.NOT_FOUND_VALUE, "not found"));
        Mockito.when(metadataStore.getStream(ArgumentMatchers.anyLong(), ArgumentMatchers.anyInt(),
                ArgumentMatchers.nullable(Long.class), ArgumentMatchers.eq(StreamRole.STREAM_ROLE_OPS)))
            .thenReturn(future);
        DefaultStoreMetadataService service = new DefaultStoreMetadataService(metadataStore, config);
        Assertions.assertEquals(-1L, service.getOperationLogStreamId(1L, 2));
    }

    @Test
    public void testGetRetryStreamId() {
        MetadataStore metadataStore = Mockito.mock(MetadataStore.class);
        CompletableFuture<StreamMetadata> future = new CompletableFuture<>();
        StreamMetadata metadata = StreamMetadata.newBuilder()
            .setStreamId(1L).build();
        future.complete(metadata);
        Mockito.when(metadataStore.getStream(ArgumentMatchers.anyLong(), ArgumentMatchers.anyInt(),
                ArgumentMatchers.nullable(Long.class), ArgumentMatchers.eq(StreamRole.STREAM_ROLE_RETRY)))
            .thenReturn(future);
        DefaultStoreMetadataService service = new DefaultStoreMetadataService(metadataStore, config);
        Assertions.assertEquals(1L, service.getRetryStreamId(3L, 1L, 2));
    }

    @Test
    public void testGetRetryStreamId_throws() {
        MetadataStore metadataStore = Mockito.mock(MetadataStore.class);
        CompletableFuture<StreamMetadata> future = new CompletableFuture<>();
        future.completeExceptionally(new ControllerException(Code.NOT_FOUND_VALUE, "not found"));
        Mockito.when(metadataStore.getStream(ArgumentMatchers.anyLong(), ArgumentMatchers.anyInt(),
                ArgumentMatchers.nullable(Long.class), ArgumentMatchers.eq(StreamRole.STREAM_ROLE_RETRY)))
            .thenReturn(future);
        DefaultStoreMetadataService service = new DefaultStoreMetadataService(metadataStore, config);
        Assertions.assertEquals(-1L, service.getRetryStreamId(0L, 1L, 2));
    }

}