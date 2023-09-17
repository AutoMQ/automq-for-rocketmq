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

public interface StoreMetadataService {
    long getStreamId(long topicId, long queueId);

    long getOperationLogStreamId(long topicId, long queueId);

    long getRetryStreamId(long consumerGroupId, long topicId, long queueId);

    long getDeadLetterStreamId(long consumerGroupId, long topicId, long queueId);

    // Each time pop will advance the consumer offset by batch size.
    // Metadata service will cache the consumer offset in memory, and periodically commit to Controller.
    void advanceConsumeOffset(long consumerGroupId, long topicId, long queueId, long offset);
}
