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
import com.automq.rocketmq.controller.exception.ControllerException;
import com.automq.rocketmq.controller.metadata.MetadataStore;
import com.automq.rocketmq.controller.metadata.database.dao.StreamRole;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

class DefaultStoreMetadataServiceTest {

    @Test
    public void testGetStreamId() throws ControllerException {
        MetadataStore metadataStore = Mockito.mock(MetadataStore.class);
        Mockito.when(metadataStore.streamIdOf(ArgumentMatchers.anyLong(), ArgumentMatchers.anyInt(),
                ArgumentMatchers.nullable(Long.class), ArgumentMatchers.eq(StreamRole.DATA)))
            .thenReturn(1L);
        DefaultStoreMetadataService service = new DefaultStoreMetadataService(metadataStore);
        Assertions.assertEquals(1L, service.getStreamId(1L, 2));
    }

    @Test
    public void testGetStreamId_throws() throws ControllerException {
        MetadataStore metadataStore = Mockito.mock(MetadataStore.class);
        Mockito.when(metadataStore.streamIdOf(ArgumentMatchers.anyLong(), ArgumentMatchers.anyInt(),
                ArgumentMatchers.nullable(Long.class), ArgumentMatchers.eq(StreamRole.DATA)))
            .thenThrow(new ControllerException(Code.NOT_FOUND_VALUE, "not found"));
        DefaultStoreMetadataService service = new DefaultStoreMetadataService(metadataStore);
        Assertions.assertEquals(-1L, service.getStreamId(1L, 2));
    }

    @Test
    public void testGetOperationLogStreamId() throws ControllerException {
        MetadataStore metadataStore = Mockito.mock(MetadataStore.class);
        Mockito.when(metadataStore.streamIdOf(ArgumentMatchers.anyLong(), ArgumentMatchers.anyInt(),
                ArgumentMatchers.nullable(Long.class), ArgumentMatchers.eq(StreamRole.OPS)))
            .thenReturn(1L);
        DefaultStoreMetadataService service = new DefaultStoreMetadataService(metadataStore);
        Assertions.assertEquals(1L, service.getStreamId(1L, 2));
    }

    @Test
    public void testGetOperationLogStreamId_throws() throws ControllerException {
        MetadataStore metadataStore = Mockito.mock(MetadataStore.class);
        Mockito.when(metadataStore.streamIdOf(ArgumentMatchers.anyLong(), ArgumentMatchers.anyInt(),
                ArgumentMatchers.nullable(Long.class), ArgumentMatchers.eq(StreamRole.OPS)))
            .thenThrow(new ControllerException(Code.NOT_FOUND_VALUE, "not found"));
        DefaultStoreMetadataService service = new DefaultStoreMetadataService(metadataStore);
        Assertions.assertEquals(-1L, service.getStreamId(1L, 2));
    }

    @Test
    public void testGetRetryStreamId() throws ControllerException {
        MetadataStore metadataStore = Mockito.mock(MetadataStore.class);
        Mockito.when(metadataStore.streamIdOf(ArgumentMatchers.anyLong(), ArgumentMatchers.anyInt(),
                ArgumentMatchers.nullable(Long.class), ArgumentMatchers.eq(StreamRole.RETRY)))
            .thenReturn(1L);
        DefaultStoreMetadataService service = new DefaultStoreMetadataService(metadataStore);
        Assertions.assertEquals(1L, service.getStreamId(1L, 2));
    }

    @Test
    public void testGetRetryStreamId_throws() throws ControllerException {
        MetadataStore metadataStore = Mockito.mock(MetadataStore.class);
        Mockito.when(metadataStore.streamIdOf(ArgumentMatchers.anyLong(), ArgumentMatchers.anyInt(),
                ArgumentMatchers.nullable(Long.class), ArgumentMatchers.eq(StreamRole.RETRY)))
            .thenThrow(new ControllerException(Code.NOT_FOUND_VALUE, "not found"));
        DefaultStoreMetadataService service = new DefaultStoreMetadataService(metadataStore);
        Assertions.assertEquals(-1L, service.getStreamId(1L, 2));
    }

}