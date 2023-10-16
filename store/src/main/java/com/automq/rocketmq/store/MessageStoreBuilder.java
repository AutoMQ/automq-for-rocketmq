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

package com.automq.rocketmq.store;

import com.automq.rocketmq.common.config.S3StreamConfig;
import com.automq.rocketmq.common.config.StoreConfig;
import com.automq.rocketmq.metadata.api.StoreMetadataService;
import com.automq.rocketmq.store.api.DLQSender;
import com.automq.rocketmq.store.api.LogicQueueManager;
import com.automq.rocketmq.store.api.S3ObjectOperator;
import com.automq.rocketmq.store.api.StreamStore;
import com.automq.rocketmq.store.exception.StoreException;
import com.automq.rocketmq.store.queue.DefaultLogicQueueManager;
import com.automq.rocketmq.store.service.InflightService;
import com.automq.rocketmq.store.service.ReviveService;
import com.automq.rocketmq.store.service.RocksDBKVService;
import com.automq.rocketmq.store.service.SnapshotService;
import com.automq.rocketmq.store.service.StreamOperationLogService;
import com.automq.rocketmq.store.service.api.KVService;
import com.automq.rocketmq.store.service.api.OperationLogService;
import com.automq.stream.s3.operator.DefaultS3Operator;
import com.automq.stream.s3.operator.S3Operator;

import static com.automq.rocketmq.store.MessageStoreImpl.KV_NAMESPACE_CHECK_POINT;
import static com.automq.rocketmq.store.MessageStoreImpl.KV_NAMESPACE_TIMER_TAG;

public class MessageStoreBuilder {
    public static MessageStoreImpl build(StoreConfig storeConfig, S3StreamConfig s3StreamConfig,
        StoreMetadataService metadataService, DLQSender dlqSender) throws StoreException {
        S3Operator operator = new DefaultS3Operator(s3StreamConfig.s3Endpoint(), s3StreamConfig.s3Region(), s3StreamConfig.s3Bucket(),
            s3StreamConfig.s3ForcePathStyle(), s3StreamConfig.s3AccessKey(), s3StreamConfig.s3SecretKey());
        StreamStore streamStore = new S3StreamStore(s3StreamConfig, metadataService, operator);
        KVService kvService = new RocksDBKVService(storeConfig.kvPath());
        InflightService inflightService = new InflightService();
        SnapshotService snapshotService = new SnapshotService(streamStore, kvService);
        OperationLogService operationLogService = new StreamOperationLogService(streamStore, snapshotService, storeConfig);
        LogicQueueManager logicQueueManager = new DefaultLogicQueueManager(storeConfig, streamStore, kvService,
            metadataService, operationLogService, inflightService);
        ReviveService reviveService = new ReviveService(KV_NAMESPACE_CHECK_POINT, KV_NAMESPACE_TIMER_TAG,
            kvService, metadataService, inflightService, logicQueueManager, dlqSender);
        S3ObjectOperator objectOperator = new S3ObjectOperatorImpl(operator);

        return new MessageStoreImpl(storeConfig, streamStore, metadataService, kvService, inflightService, snapshotService, logicQueueManager, reviveService, objectOperator);
    }
}
