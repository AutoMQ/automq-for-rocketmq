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

import apache.rocketmq.controller.v1.Stream;
import com.automq.rocketmq.common.exception.RocketMQException;
import com.automq.rocketmq.controller.metadata.MetadataStore;
import java.util.List;

public class DefaultStoreMetadataService implements StoreMetadataService {

    private final MetadataStore metadataStore;

    public DefaultStoreMetadataService(MetadataStore metadataStore) {
        this.metadataStore = metadataStore;
    }

    @Override
    public long getStreamId(long topicId, int queueId) {
        return 0;
    }

    @Override
    public long getOperationLogStreamId(long topicId, int queueId) {
        return 0;
    }

    @Override
    public long getRetryStreamId(long consumerGroupId, long topicId, int queueId) {
        return 0;
    }

    @Override
    public long getDeadLetterStreamId(long consumerGroupId, long topicId, int queueId) {
        return 0;
    }

    @Override
    public int getMaxRetryTimes(long consumerGroupId) {
        return 0;
    }

    @Override
    public StreamOffset openStream(long streamId, long streamEpoch, int brokerId,
        long brokerEpoch) throws RocketMQException {
        return null;
    }

    @Override
    public void closeStream(long streamId, long streamEpoch, int brokerId, long brokerEpoch) throws RocketMQException {

    }

    @Override
    public void trimStream(long streamId, long streamEpoch, int brokerId, long brokerEpoch,
        long offset) throws RocketMQException {

    }

    @Override
    public List<Stream> listOpenStreams(int brokerId, long brokerEpoch) throws RocketMQException {
        return null;
    }

    @Override
    public long prepareS3Objects(int count, int ttlInMinutes) throws RocketMQException {
        return 0;
    }

    @Override
    public void commitWalObject(WalObject walObject) throws RocketMQException {

    }

    @Override
    public void commitStreamObject(StreamObject streamObject) throws RocketMQException {

    }
}
