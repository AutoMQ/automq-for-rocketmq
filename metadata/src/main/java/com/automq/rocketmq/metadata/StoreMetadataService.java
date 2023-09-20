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

import apache.rocketmq.controller.v1.Stream;
import com.automq.rocketmq.common.exception.RocketMQException;
import java.util.List;

public interface StoreMetadataService {
    long getStreamId(long topicId, int queueId);

    long getOperationLogStreamId(long topicId, int queueId);

    long getRetryStreamId(long consumerGroupId, long topicId, int queueId);

    long getDeadLetterStreamId(long consumerGroupId, long topicId, int queueId);

    int getMaxRetryTimes(long consumerGroupId);

    StreamOffset openStream(long streamId, long streamEpoch, int brokerId, long brokerEpoch) throws RocketMQException;

    void closeStream(long streamId, long streamEpoch, int brokerId, long brokerEpoch) throws RocketMQException;

    /**
     * @param streamId
     * @param streamEpoch
     * @param brokerId
     * @param brokerEpoch
     * @param offset      The new start offset of the stream
     */
    void trimStream(long streamId, long streamEpoch, int brokerId, long brokerEpoch,
        long offset) throws RocketMQException;

    List<Stream> listOpenStreams(int brokerId, long brokerEpoch) throws RocketMQException;


    long prepareS3Objects(int count, int ttlInMinutes) throws RocketMQException;

    void commitWalObject(WalObject walObject) throws RocketMQException;

    void commitStreamObject(StreamObject streamObject) throws RocketMQException;
}
