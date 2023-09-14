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

package com.automq.rocketmq.store.impl;

import com.automq.rocketmq.common.model.Message;
import com.automq.rocketmq.store.MessageStore;
import com.automq.rocketmq.store.StreamStore;
import com.automq.rocketmq.store.model.generated.CheckPoint;
import com.automq.rocketmq.store.model.message.AckResult;
import com.automq.rocketmq.store.model.message.ChangeInvisibleDurationResult;
import com.automq.rocketmq.store.model.message.PopResult;
import com.automq.rocketmq.store.service.KVService;
import com.google.flatbuffers.FlatBufferBuilder;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.rocksdb.RocksDBException;

public class MessageStoreImpl implements MessageStore {
    protected static final String KV_PARTITION_CHECK_POINT = "check_point";
    protected static final String KV_PARTITION_TIMER_TAG = "timer_tag";

    private final StreamStore streamStore;
    private final KVService kvService;

    private final AtomicLong fakeSerialNumberGenerator = new AtomicLong();

    public MessageStoreImpl(StreamStore streamStore, KVService kvService) {
        this.streamStore = streamStore;
        this.kvService = kvService;
    }

    //<groupId><topicId><queueId><offset>
    protected static byte[] buildCheckPointKey(long consumeGroupId, long topicId, int queueId, long offset) {
        ByteBuffer buffer = ByteBuffer.allocate(28);
        buffer.putLong(0, consumeGroupId);
        buffer.putLong(8, topicId);
        buffer.putInt(16, queueId);
        buffer.putLong(20, offset);
        return buffer.array();
    }

    private static byte[] buildCheckPointValue(long serialNumber, long deliveryTimestamp, long invisibleDuration,
        long topicId,
        int queueId, long offset, long consumeGroupId) {
        FlatBufferBuilder builder = new FlatBufferBuilder();
        int root = CheckPoint.createCheckPoint(builder, serialNumber, deliveryTimestamp, invisibleDuration, topicId, queueId, offset, consumeGroupId);
        builder.finish(root);
        return builder.sizedByteArray();
    }

    // <invisibleTime><groupId><topicId><queueId><offset>
    protected static byte[] buildTimerTagKey(long invisibleTime, long consumeGroupId, long topicId, int queueId,
        long offset) {
        ByteBuffer buffer = ByteBuffer.allocate(36);
        buffer.putLong(0, invisibleTime);
        buffer.putLong(8, consumeGroupId);
        buffer.putLong(16, topicId);
        buffer.putInt(24, queueId);
        buffer.putLong(28, offset);
        return buffer.array();
    }

    @Override
    public PopResult pop(long consumeGroupId, long topicId, int queueId, long offset, int maxCount, boolean isOrder,
        long invisibleDuration) {
        // TODO: write this request to operation log and get the serial number
        // serial number should be monotonically increasing
        long serialNumber = fakeSerialNumberGenerator.getAndIncrement();

        long deliveryTimestamp = System.nanoTime();

        // TODO: fetch message from stream store
        List<Message> messageList = new ArrayList<>();

        // add mock message
        messageList.add(new Message(0));

        // insert check point and timer tag into KVService
        messageList.forEach(message -> {
            try {
                // put check point
                kvService.put(KV_PARTITION_CHECK_POINT,
                    buildCheckPointKey(consumeGroupId, topicId, queueId, message.offset()),
                    buildCheckPointValue(serialNumber, deliveryTimestamp, invisibleDuration, topicId, queueId, message.offset(), consumeGroupId));
                // put timer tag
                kvService.put(KV_PARTITION_TIMER_TAG,
                    buildTimerTagKey(invisibleDuration, consumeGroupId, topicId, queueId, message.offset()), new byte[0]);
            } catch (RocksDBException e) {
                throw new RuntimeException(e);
            }
        });

        if (!isOrder) {
            // TODO: commit consumer offset
        }

        return new PopResult(0, deliveryTimestamp, messageList);
    }

    @Override
    public AckResult ack(String receiptHandle) {
        // write this request to operation log

        // delete check point and timer tag according to receiptHandle

        return null;
    }

    @Override
    public ChangeInvisibleDurationResult changeInvisibleDuration(String receiptHandle, int invisibleDuration) {
        // write this request to operation log

        // change invisibleTime in check point info and regenerate timer tag

        return null;
    }

    @Override
    public int getInflightStatsByQueue(long topicId, int queueId) {
        // get check point count of specified topic and queue
        return 0;
    }

    @Override
    public boolean cleanMetadata(long topicId, int queueId) {
        // clean all check points and timer tags of specified topic and queue
        return false;
    }
}
