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

package com.automq.rocketmq.store;

import com.automq.rocketmq.common.config.S3StreamConfig;
import com.automq.rocketmq.common.config.StoreConfig;
import com.automq.rocketmq.metadata.api.StoreMetadataService;
import com.automq.rocketmq.store.api.DeadLetterSender;
import com.automq.rocketmq.store.api.LogicQueueManager;
import com.automq.rocketmq.store.api.S3ObjectOperator;
import com.automq.rocketmq.store.api.StreamStore;
import com.automq.rocketmq.store.exception.StoreException;
import com.automq.rocketmq.store.queue.DefaultLogicQueueManager;
import com.automq.rocketmq.store.service.InflightService;
import com.automq.rocketmq.store.service.MessageArrivalNotificationService;
import com.automq.rocketmq.store.service.ReviveService;
import com.automq.rocketmq.store.service.RocksDBKVService;
import com.automq.rocketmq.store.service.SnapshotService;
import com.automq.rocketmq.store.service.StreamOperationLogService;
import com.automq.rocketmq.store.service.StreamReclaimService;
import com.automq.rocketmq.store.service.TimerService;
import com.automq.rocketmq.store.service.TransactionService;
import com.automq.rocketmq.store.service.api.KVService;
import com.automq.rocketmq.store.service.api.OperationLogService;
import com.automq.stream.s3.metadata.ObjectUtils;
import com.automq.stream.s3.operator.DefaultS3Operator;
import com.automq.stream.s3.operator.S3Operator;
import java.util.List;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;

import static com.automq.rocketmq.store.MessageStoreImpl.KV_NAMESPACE_CHECK_POINT;

public class MessageStoreBuilder {
    public static MessageStoreImpl build(StoreConfig storeConfig, S3StreamConfig s3StreamConfig,
        StoreMetadataService metadataService, DeadLetterSender deadLetterSender) throws StoreException {
        // set S3 namespace
        ObjectUtils.setNamespace(s3StreamConfig.s3Namespace());
        StreamStore streamStore = new S3StreamStore(storeConfig, s3StreamConfig, metadataService);
        KVService kvService = new RocksDBKVService(storeConfig.kvPath());
        InflightService inflightService = new InflightService();
        SnapshotService snapshotService = new SnapshotService(streamStore, kvService);
        OperationLogService operationLogService = new StreamOperationLogService(streamStore, snapshotService, storeConfig);
        StreamReclaimService streamReclaimService = new StreamReclaimService(streamStore);
        // TODO: We may have multiple timer service in the future.
        TimerService timerService = new TimerService("timer_0", kvService);
        LogicQueueManager logicQueueManager = new DefaultLogicQueueManager(storeConfig, streamStore, kvService, timerService,
            metadataService, operationLogService, inflightService, streamReclaimService);
        MessageArrivalNotificationService messageArrivalNotificationService = new MessageArrivalNotificationService();
        ReviveService reviveService = new ReviveService(KV_NAMESPACE_CHECK_POINT, kvService, timerService,
            metadataService, messageArrivalNotificationService, logicQueueManager, deadLetterSender);

        // S3 object manager, such as trim expired messages, etc.
        S3Operator operator = new DefaultS3Operator(s3StreamConfig.s3Endpoint(), s3StreamConfig.s3Region(), s3StreamConfig.s3Bucket(),
            s3StreamConfig.s3ForcePathStyle(), List.of(() -> AwsBasicCredentials.create(s3StreamConfig.s3AccessKey(), s3StreamConfig.s3SecretKey())), false);
        S3ObjectOperator objectOperator = new S3ObjectOperatorImpl(operator);

        TransactionService transactionService = new TransactionService(storeConfig, timerService);

        return new MessageStoreImpl(storeConfig, streamStore, metadataService, kvService, timerService, inflightService,
            snapshotService, logicQueueManager, reviveService, objectOperator, messageArrivalNotificationService, transactionService);
    }
}
