/*
 * Copyright 2024, AutoMQ CO.,LTD.
 *
 * Use of this software is governed by the Business Source License
 * included in the file BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

package com.automq.rocketmq.metadata;

import apache.rocketmq.common.v1.Code;
import apache.rocketmq.controller.v1.S3StreamObject;
import apache.rocketmq.controller.v1.S3StreamSetObject;
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
        S3StreamSetObject streamSetObject = S3StreamSetObject.newBuilder().setObjectId(1L).setBrokerId(10).build();
        int nodeId = 100;
        when(metadataStore.config()).thenReturn(config);
        when(config.nodeId()).thenReturn(nodeId);
        when(s3MetadataService.commitStreamSetObject(ArgumentMatchers.any(), ArgumentMatchers.anyList(), ArgumentMatchers.anyList()))
            .thenReturn(CompletableFuture.completedFuture(null));
        service.commitStreamSetObject(streamSetObject, new ArrayList<>(), new ArrayList<>());
        // Verify the arguments passed to metadataStore.commitWalObject().
        S3StreamSetObject newWal = S3StreamSetObject.newBuilder(streamSetObject).setBrokerId(nodeId).build();
        Mockito.verify(s3MetadataService).commitStreamSetObject(ArgumentMatchers.eq(newWal), ArgumentMatchers.anyList(), ArgumentMatchers.anyList());
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
        Mockito.when(metadataService.commitStreamSetObject(ArgumentMatchers.any(), ArgumentMatchers.anyList(), ArgumentMatchers.anyList()))
            .thenReturn(CompletableFuture.failedFuture(new ControllerException(Code.MOCK_FAILURE_VALUE, "Mocked Failure")))
            .thenReturn(CompletableFuture.completedFuture(null));
        StoreMetadataService svc = new DefaultStoreMetadataService(metadataStore, metadataService);
        Assertions.assertDoesNotThrow(() -> svc.commitStreamSetObject(S3StreamSetObject.newBuilder().build(),
            new ArrayList<>(), new ArrayList<>()).join());
    }

    @Test
    public void testRetryableCommitStreamObject() {
        S3MetadataService metadataService = Mockito.mock(S3MetadataService.class);
        Mockito.when(metadataService.commitStreamObject(ArgumentMatchers.any(), ArgumentMatchers.anyList()))
            .thenReturn(CompletableFuture.failedFuture(new ControllerException(Code.MOCK_FAILURE_VALUE, "Mocked Failure")))
            .thenReturn(CompletableFuture.completedFuture(null));
        StoreMetadataService svc = new DefaultStoreMetadataService(metadataStore, metadataService);
        Assertions.assertDoesNotThrow(() -> svc.compactStreamObject(S3StreamObject.newBuilder().build(),
            new ArrayList<>()).join());
    }

}