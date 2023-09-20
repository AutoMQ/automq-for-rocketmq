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
import com.automq.rocketmq.store.model.generated.TimerTag;
import com.automq.rocketmq.store.model.kv.BatchDeleteRequest;
import com.automq.rocketmq.store.model.stream.SingleRecord;
import com.automq.rocketmq.store.service.KVService;
import com.automq.rocketmq.store.util.MessageUtil;
import com.automq.rocketmq.store.util.SerializeUtil;
import com.automq.rocketmq.stream.api.FetchResult;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicBoolean;

public class ReviveService implements Runnable {
    private Thread thread;
    private final AtomicBoolean started = new AtomicBoolean(false);

    protected volatile boolean stopped = false;

    private final String checkPointNamespace;
    private final String timerTagNamespace;
    private final KVService kvService;
    private final StoreMetadataService metadataService;
    private final StreamStore streamStore;

    public ReviveService(String checkPointNamespace, String timerTagNamespace, KVService kvService,
        StoreMetadataService metadataService,
        StreamStore streamStore) {
        this.checkPointNamespace = checkPointNamespace;
        this.timerTagNamespace = timerTagNamespace;
        this.kvService = kvService;
        this.metadataService = metadataService;
        this.streamStore = streamStore;
    }

    public String getServiceName() {
        return "ReviveService";
    }

    public void start() {
        if (!started.compareAndSet(false, true)) {
            return;
        }
        stopped = false;
        this.thread = new Thread(this, getServiceName());
        this.thread.setDaemon(true);
        this.thread.start();

    }

    public void shutdown() {
        this.stopped = true;
        this.thread.interrupt();
    }

    @Override
    public void run() {
        while (!stopped) {
            try {
                tryRevive();
                Thread.sleep(1000);
            } catch (StoreException e) {
                // TODO: log exception
                e.printStackTrace();
            } catch (InterruptedException ignored) {
            }
        }
    }

    protected void tryRevive() throws StoreException {
        byte[] start = ByteBuffer.allocate(8).putLong(0).array();
        byte[] end = ByteBuffer.allocate(8).putLong(System.currentTimeMillis() - 1).array();

        // Iterate timer tag until now to find messages need to reconsume.
        kvService.iterate(timerTagNamespace, null, start, end, (key, value) -> {
            // Fetch the origin message from stream store.
            // TODO: async revive retry message
            TimerTag timerTag = TimerTag.getRootAsTimerTag(ByteBuffer.wrap(value));
            // The timer tag may belong to origin topic or retry topic.
            FetchResult result = streamStore.fetch(timerTag.streamId(), timerTag.offset(), 1).join();
            // TODO: log exception
            assert result.recordBatchList().size() <= 1;
            // Message has already been deleted.
            if (result.recordBatchList().isEmpty()) {
                // TODO: log the probable bug.
                return;
            }

            // Build the retry message and append it to retry stream or dead letter stream.
            MessageExt messageExt = MessageUtil.transferToMessageExt(result.recordBatchList().get(0));
            messageExt.setReconsumeCount(messageExt.reconsumeCount() + 1);
            if (messageExt.reconsumeCount() <= metadataService.getMaxRetryTimes(timerTag.consumerGroupId())) {
                long retryStreamId = metadataService.getRetryStreamId(timerTag.consumerGroupId(), timerTag.originTopicId(), timerTag.originQueueId());
                streamStore.append(retryStreamId, new SingleRecord(messageExt.systemProperties(), messageExt.message().getByteBuffer())).join();
            } else {
                long deadLetterStreamId = metadataService.getDeadLetterStreamId(timerTag.consumerGroupId(), timerTag.originTopicId(), timerTag.originQueueId());
                streamStore.append(deadLetterStreamId, new SingleRecord(messageExt.systemProperties(), messageExt.message().getByteBuffer())).join();
            }

            // Delete checkpoint and timer tag
            try {
                BatchDeleteRequest deleteCheckPointRequest = new BatchDeleteRequest(checkPointNamespace,
                    SerializeUtil.buildCheckPointKey(timerTag.originTopicId(), timerTag.originQueueId(), timerTag.offset(), timerTag.operationId()));

                BatchDeleteRequest deleteTimerTagRequest = new BatchDeleteRequest(timerTagNamespace,
                    SerializeUtil.buildTimerTagKey(timerTag.nextVisibleTimestamp(), timerTag.originTopicId(), timerTag.originQueueId(), timerTag.offset(), timerTag.operationId()));
                kvService.batch(deleteCheckPointRequest, deleteTimerTagRequest);
            } catch (StoreException e) {
                // TODO: log exception
            }
        });
    }
}
