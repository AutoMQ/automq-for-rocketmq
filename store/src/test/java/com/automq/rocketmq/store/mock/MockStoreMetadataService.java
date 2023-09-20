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

package com.automq.rocketmq.store.mock;

import apache.rocketmq.controller.v1.Stream;
import com.automq.rocketmq.metadata.StoreMetadataService;
import com.automq.rocketmq.metadata.StreamObject;
import com.automq.rocketmq.metadata.StreamOffset;
import com.automq.rocketmq.metadata.WalObject;
import java.nio.ByteBuffer;
import java.util.List;

public class MockStoreMetadataService implements StoreMetadataService {
    @Override
    public long getStreamId(long topicId, int queueId) {
        ByteBuffer buffer = ByteBuffer.allocate(8);
        // Mark the stream type as origin topic.
        buffer.putShort(0, (short) 0);
        buffer.putShort(2, (short) topicId);
        buffer.putShort(4, (short) queueId);
        return buffer.getLong(0);
    }

    @Override
    public long getOperationLogStreamId(long topicId, int queueId) {
        ByteBuffer buffer = ByteBuffer.allocate(8);
        // Mark the stream type as operation log.
        buffer.putShort(0, (short) 1);
        buffer.putShort(2, (short) topicId);
        buffer.putShort(4, (short) queueId);
        return buffer.getLong(0);
    }

    @Override
    public long getRetryStreamId(long consumerGroupId, long topicId, int queueId) {
        ByteBuffer buffer = ByteBuffer.allocate(8);
        // Mark the stream type as retry topic.
        buffer.putShort(0, (short) 2);
        buffer.putShort(2, (short) consumerGroupId);
        buffer.putShort(4, (short) topicId);
        buffer.putShort(6, (short) queueId);
        return buffer.getLong(0);
    }

    @Override
    public long getDeadLetterStreamId(long consumerGroupId, long topicId, int queueId) {
        ByteBuffer buffer = ByteBuffer.allocate(8);
        // Mark the stream type as dead letter topic.
        buffer.putShort(0, (short) 3);
        buffer.putShort(2, (short) consumerGroupId);
        buffer.putShort(4, (short) topicId);
        buffer.putShort(6, (short) queueId);
        return buffer.getLong(0);
    }

    @Override
    public int getMaxRetryTimes(long consumerGroupId) {
        return 1;
    }

    @Override
    public StreamOffset openStream(long streamId, long streamEpoch, int brokerId, long brokerEpoch) {
        return null;
    }

    @Override
    public void closeStream(long streamId, long streamEpoch, int brokerId, long brokerEpoch) {

    }

    @Override
    public void trimStream(long streamId, long streamEpoch, int brokerId, long brokerEpoch, long offset) {

    }

    @Override
    public List<Stream> listOpenStreams(int brokerId, long brokerEpoch) {
        return null;
    }

    @Override
    public long prepareS3Objects(int count, int ttlInMinutes) {
        return 0;
    }

    @Override
    public void commitWalObject(WalObject walObject) {

    }

    @Override
    public void commitStreamObject(StreamObject streamObject) {

    }
}
