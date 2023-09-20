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

package com.automq.rocketmq.metadata;

import apache.rocketmq.controller.v1.S3StreamObject;
import apache.rocketmq.controller.v1.S3WALObject;
import apache.rocketmq.controller.v1.StreamMetadata;
import com.automq.rocketmq.common.exception.RocketMQException;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public interface StoreMetadataService {
    long getStreamId(long topicId, int queueId);

    long getOperationLogStreamId(long topicId, int queueId);

    long getRetryStreamId(long consumerGroupId, long topicId, int queueId);

    long getDeadLetterStreamId(long consumerGroupId, long topicId, int queueId);

    int getMaxRetryTimes(long consumerGroupId);

    CompletableFuture<Void> trimStream(long streamId, long streamEpoch, long newStartOffset) throws RocketMQException;

    CompletableFuture<StreamMetadata> openStream(long streamId, long streamEpoch);

    CompletableFuture<Void> closeStream(long streamId, long streamEpoch);

    CompletableFuture<List<StreamMetadata>> listOpenStreams(int brokerId, long brokerEpoch);

    CompletableFuture<Long> prepareS3Objects(int count, int ttlInMinutes);

    CompletableFuture<Void> commitWalObject(S3WALObject walObject, List<S3StreamObject> streamObjects, List<Long> compactedObjects);

    CompletableFuture<Void> commitStreamObject(S3StreamObject streamObject);
}
