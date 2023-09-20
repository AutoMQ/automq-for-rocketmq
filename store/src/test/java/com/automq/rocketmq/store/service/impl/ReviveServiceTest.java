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

package com.automq.rocketmq.store.service.impl;

import com.automq.rocketmq.common.model.MessageExt;
import com.automq.rocketmq.metadata.StoreMetadataService;
import com.automq.rocketmq.store.StreamStore;
import com.automq.rocketmq.store.exception.StoreException;
import com.automq.rocketmq.store.impl.StreamStoreImpl;
import com.automq.rocketmq.store.mock.MockStoreMetadataService;
import com.automq.rocketmq.store.model.generated.TimerTag;
import com.automq.rocketmq.store.model.stream.SingleRecord;
import com.automq.rocketmq.store.service.KVService;
import com.automq.rocketmq.store.util.MessageUtil;
import com.automq.rocketmq.store.util.SerializeUtil;
import com.automq.rocketmq.stream.api.FetchResult;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static com.automq.rocketmq.store.mock.MockMessageUtil.buildMessage;
import static org.junit.jupiter.api.Assertions.assertEquals;

class ReviveServiceTest {
    private static final String PATH = "/tmp/test_revive_service/";
    protected static final String KV_NAMESPACE_CHECK_POINT = "check_point";
    protected static final String KV_NAMESPACE_TIMER_TAG = "timer_tag";

    private static KVService kvService;
    private static StoreMetadataService metadataService;
    private static StreamStore streamStore;
    private static ReviveService reviveService;

    @BeforeEach
    public void setUp() throws StoreException {
        kvService = new RocksDBKVService(PATH);
        metadataService = new MockStoreMetadataService();
        streamStore = new StreamStoreImpl();
        reviveService = new ReviveService(KV_NAMESPACE_CHECK_POINT, KV_NAMESPACE_TIMER_TAG, kvService, metadataService, streamStore);
    }

    @AfterEach
    public void tearDown() throws StoreException {
        kvService.destroy();
    }

    @Test
    void tryRevive() throws StoreException, InterruptedException {
        // Append mock message.
        long streamId = metadataService.getStreamId(32, 32);
        streamStore.append(streamId, new SingleRecord(new HashMap<>(), buildMessage(32, 32, ""))).join();

        // Append mock check point and timer tag.
        kvService.put(KV_NAMESPACE_CHECK_POINT, SerializeUtil.buildCheckPointKey(32, 32, 0, Long.MAX_VALUE), new byte[0]);

        kvService.put(KV_NAMESPACE_TIMER_TAG, SerializeUtil.buildTimerTagKey(0, 32, 32, 0, Long.MAX_VALUE),
            SerializeUtil.buildTimerTagValue(0, 32, 32, 32, streamId, 0, Long.MAX_VALUE));
        kvService.put(KV_NAMESPACE_TIMER_TAG, SerializeUtil.buildTimerTagKey(Long.MAX_VALUE, 0, 0, 0, 0),
            SerializeUtil.buildTimerTagValue(Long.MAX_VALUE, 0, 0, 0, 0, 0, 0));

        reviveService.tryRevive();

        long retryStreamId = metadataService.getRetryStreamId(32, 32, 32);
        FetchResult fetchResult = streamStore.fetch(retryStreamId, 0, 100).join();
        assertEquals(1, fetchResult.recordBatchList().size());

        MessageExt messageExt = MessageUtil.transferToMessageExt(fetchResult.recordBatchList().get(0));
        assertEquals(1, messageExt.reconsumeCount());
        assertEquals(32, messageExt.message().topicId());
        assertEquals(32, messageExt.message().queueId());
        assertEquals(0, messageExt.offset());

        AtomicInteger checkPointCount = new AtomicInteger();
        kvService.iterate(KV_NAMESPACE_CHECK_POINT, (key, value) -> checkPointCount.getAndIncrement());
        assertEquals(0, checkPointCount.get());

        AtomicInteger timerTagCount = new AtomicInteger();
        kvService.iterate(KV_NAMESPACE_TIMER_TAG, (key, value) -> timerTagCount.getAndIncrement());
        assertEquals(1, timerTagCount.get());

        // Append timer tag of retry message.
        kvService.put(KV_NAMESPACE_TIMER_TAG, SerializeUtil.buildTimerTagKey(System.currentTimeMillis() - 100, 32, 32, 0, Long.MAX_VALUE),
            SerializeUtil.buildTimerTagValue(System.currentTimeMillis() - 100, 32, 32, 32, retryStreamId, 0, Long.MAX_VALUE));
        reviveService.tryRevive();

        long deadLetterStreamId = metadataService.getDeadLetterStreamId(32, 32, 32);
        fetchResult = streamStore.fetch(deadLetterStreamId, 0, 100).join();
        assertEquals(1, fetchResult.recordBatchList().size());

        messageExt = MessageUtil.transferToMessageExt(fetchResult.recordBatchList().get(0));
        assertEquals(2, messageExt.reconsumeCount());
        assertEquals(32, messageExt.message().topicId());
        assertEquals(32, messageExt.message().queueId());
        assertEquals(0, messageExt.offset());

        kvService.flush(true);
        Set<TimerTag> timerTagSet = new HashSet<>();
        kvService.iterate(KV_NAMESPACE_TIMER_TAG, (key, value) -> timerTagSet.add(TimerTag.getRootAsTimerTag(ByteBuffer.wrap(value))));
        for (TimerTag tag : timerTagSet) {
            System.out.printf("topic: %d, queue: %d, offset: %d, time %d%n", tag.originTopicId(), tag.originQueueId(), tag.offset(), tag.nextVisibleTimestamp());
        }
        assertEquals(1, timerTagSet.size());
    }
}