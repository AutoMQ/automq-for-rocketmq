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

import apache.rocketmq.common.v1.Code;
import apache.rocketmq.controller.v1.S3StreamObject;
import apache.rocketmq.controller.v1.S3WALObject;
import apache.rocketmq.controller.v1.StreamMetadata;
import apache.rocketmq.controller.v1.StreamRole;
import com.automq.rocketmq.common.config.ControllerConfig;
import com.automq.rocketmq.common.exception.ControllerException;
import com.automq.rocketmq.controller.MetadataStore;
import com.automq.rocketmq.metadata.api.StoreMetadataService;
import com.automq.rocketmq.metadata.service.S3MetadataService;
import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class DefaultStoreMetadataServiceTest {

    @Mock
    private ControllerConfig config;

    @Mock
    private MetadataStore metadataStore;

    @Mock
    private S3MetadataService s3MetadataService;

    @Test
    public void testCommitWalObject() {
        DefaultStoreMetadataService service = new DefaultStoreMetadataService(metadataStore, s3MetadataService);
        S3WALObject walObject = S3WALObject.newBuilder().setObjectId(1L).setBrokerId(10).build();
        int nodeId = 100;
        when(metadataStore.config()).thenReturn(config);
        when(config.nodeId()).thenReturn(nodeId);
        when(s3MetadataService.commitWalObject(ArgumentMatchers.any(), ArgumentMatchers.anyList(), ArgumentMatchers.anyList()))
            .thenReturn(CompletableFuture.completedFuture(null));
        service.commitWalObject(walObject, new ArrayList<>(), new ArrayList<>());
        // Verify the arguments passed to metadataStore.commitWalObject().
        S3WALObject newWal = S3WALObject.newBuilder(walObject).setBrokerId(nodeId).build();
        Mockito.verify(s3MetadataService).commitWalObject(ArgumentMatchers.eq(newWal), ArgumentMatchers.anyList(), ArgumentMatchers.anyList());
    }

    @Test
    public void testGetStreamId() {
        CompletableFuture<StreamMetadata> future = new CompletableFuture<>();
        StreamMetadata metadata = StreamMetadata.newBuilder()
            .setStreamId(1L).build();
        future.complete(metadata);
        when(metadataStore.getStream(ArgumentMatchers.anyLong(), ArgumentMatchers.anyInt(),
            ArgumentMatchers.nullable(Long.class), ArgumentMatchers.eq(StreamRole.STREAM_ROLE_DATA)))
            .thenReturn(future);

        DefaultStoreMetadataService service = new DefaultStoreMetadataService(metadataStore, s3MetadataService);
        Assertions.assertEquals(1L, service.dataStreamOf(1L, 2).join().getStreamId());
    }

    @Test
    public void testGetStreamId_throws() {
        CompletableFuture<StreamMetadata> future = new CompletableFuture<>();
        future.completeExceptionally(new ControllerException(Code.NOT_FOUND_VALUE, "not found"));
        when(metadataStore.getStream(ArgumentMatchers.anyLong(), ArgumentMatchers.anyInt(),
            ArgumentMatchers.nullable(Long.class), ArgumentMatchers.eq(StreamRole.STREAM_ROLE_DATA)))
            .thenReturn(future);
        DefaultStoreMetadataService service = new DefaultStoreMetadataService(metadataStore, s3MetadataService);
        CompletableFuture<StreamMetadata> streamCf = service.dataStreamOf(1L, 2);
        // Assert exception thrown
        ControllerException exception = (ControllerException) Assertions.assertThrows(ExecutionException.class, streamCf::get).getCause();
        Assertions.assertEquals(Code.NOT_FOUND_VALUE, exception.getErrorCode());
    }

    @Test
    public void testGetOperationLogStreamId() {
        CompletableFuture<StreamMetadata> future = new CompletableFuture<>();
        StreamMetadata metadata = StreamMetadata.newBuilder()
            .setStreamId(1L).build();
        future.complete(metadata);
        when(metadataStore.getStream(ArgumentMatchers.anyLong(), ArgumentMatchers.anyInt(),
            ArgumentMatchers.nullable(Long.class), ArgumentMatchers.eq(StreamRole.STREAM_ROLE_OPS)))
            .thenReturn(future);
        DefaultStoreMetadataService service = new DefaultStoreMetadataService(metadataStore, s3MetadataService);
        Assertions.assertEquals(1L, service.operationStreamOf(1L, 2).join().getStreamId());
    }

    @Test
    public void testGetOperationLogStreamId_throws() {
        CompletableFuture<StreamMetadata> future = new CompletableFuture<>();
        future.completeExceptionally(new ControllerException(Code.NOT_FOUND_VALUE, "not found"));
        when(metadataStore.getStream(ArgumentMatchers.anyLong(), ArgumentMatchers.anyInt(),
            ArgumentMatchers.nullable(Long.class), ArgumentMatchers.eq(StreamRole.STREAM_ROLE_OPS)))
            .thenReturn(future);
        DefaultStoreMetadataService service = new DefaultStoreMetadataService(metadataStore, s3MetadataService);
        CompletableFuture<StreamMetadata> streamCf = service.operationStreamOf(1L, 2);
        // Assert exception thrown
        ControllerException exception = (ControllerException) Assertions.assertThrows(ExecutionException.class, streamCf::get).getCause();
        Assertions.assertEquals(Code.NOT_FOUND_VALUE, exception.getErrorCode());
    }

    @Test
    public void testGetRetryStreamId() {
        CompletableFuture<StreamMetadata> future = new CompletableFuture<>();
        StreamMetadata metadata = StreamMetadata.newBuilder()
            .setStreamId(1L).build();
        future.complete(metadata);
        when(metadataStore.getStream(ArgumentMatchers.anyLong(), ArgumentMatchers.anyInt(),
            ArgumentMatchers.nullable(Long.class), ArgumentMatchers.eq(StreamRole.STREAM_ROLE_RETRY)))
            .thenReturn(future);
        DefaultStoreMetadataService service = new DefaultStoreMetadataService(metadataStore, s3MetadataService);
        Assertions.assertEquals(1L, service.retryStreamOf(3L, 1L, 2).join().getStreamId());
    }

    @Test
    public void testGetRetryStreamId_throws() {
        CompletableFuture<StreamMetadata> future = new CompletableFuture<>();
        future.completeExceptionally(new ControllerException(Code.NOT_FOUND_VALUE, "not found"));
        when(metadataStore.getStream(ArgumentMatchers.anyLong(), ArgumentMatchers.anyInt(),
            ArgumentMatchers.nullable(Long.class), ArgumentMatchers.eq(StreamRole.STREAM_ROLE_RETRY)))
            .thenReturn(future);
        DefaultStoreMetadataService service = new DefaultStoreMetadataService(metadataStore, s3MetadataService);

        CompletableFuture<StreamMetadata> streamCf = service.retryStreamOf(0L, 1L, 2);
        // Assert exception thrown
        ControllerException exception = (ControllerException) Assertions.assertThrows(ExecutionException.class, streamCf::get).getCause();
        Assertions.assertEquals(Code.NOT_FOUND_VALUE, exception.getErrorCode());
    }

    @Test
    public void testRetryableCommitWalObject() {
        when(metadataStore.config()).thenReturn(config);
        when(config.nodeId()).thenReturn(1);
        S3MetadataService metadataService = Mockito.mock(S3MetadataService.class);
        Mockito.when(metadataService.commitWalObject(ArgumentMatchers.any(), ArgumentMatchers.anyList(), ArgumentMatchers.anyList()))
            .thenReturn(CompletableFuture.failedFuture(new ControllerException(Code.MOCK_FAILURE_VALUE, "Mocked Failure")))
            .thenReturn(CompletableFuture.completedFuture(null));
        StoreMetadataService svc = new DefaultStoreMetadataService(metadataStore, metadataService);
        Assertions.assertDoesNotThrow(() -> svc.commitWalObject(S3WALObject.newBuilder().build(),
            new ArrayList<>(), new ArrayList<>()).join());
    }

    @Test
    public void testRetryableCommitStreamObject() {
        S3MetadataService metadataService = Mockito.mock(S3MetadataService.class);
        Mockito.when(metadataService.commitStreamObject(ArgumentMatchers.any(), ArgumentMatchers.anyList()))
            .thenReturn(CompletableFuture.failedFuture(new ControllerException(Code.MOCK_FAILURE_VALUE, "Mocked Failure")))
            .thenReturn(CompletableFuture.completedFuture(null));
        StoreMetadataService svc = new DefaultStoreMetadataService(metadataStore, metadataService);
        Assertions.assertDoesNotThrow(() -> svc.commitStreamObject(S3StreamObject.newBuilder().build(),
            new ArrayList<>()).join());
    }

}