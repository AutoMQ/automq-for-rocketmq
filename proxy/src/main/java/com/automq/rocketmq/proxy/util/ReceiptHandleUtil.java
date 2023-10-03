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

package com.automq.rocketmq.proxy.util;

import com.automq.rocketmq.proxy.model.VirtualQueue;
import com.automq.rocketmq.store.model.generated.ReceiptHandle;
import com.automq.rocketmq.store.util.SerializeUtil;
import org.apache.rocketmq.common.message.MessageConst;

import static org.apache.rocketmq.common.consumer.ReceiptHandle.decode;

public class ReceiptHandleUtil {
    public static String encodeReceiptHandle(String rawReceiptHandle, long invisibleTime) {
        ReceiptHandle rawHandle = SerializeUtil.decodeReceiptHandle(rawReceiptHandle);

        VirtualQueue virtualQueue = new VirtualQueue(rawHandle.topicId(), rawHandle.queueId());
        ReceiptHandleBuilder rmqHandle = new ReceiptHandleBuilder()
            .startOffset(rawHandle.messageOffset())
            .retrieveTime(System.currentTimeMillis())
            .invisibleTime(invisibleTime)
            .reviveQueueId(0) // No revive queue in S3RocketMQ
            .topicType(rawReceiptHandle) // S3RocketMQ occupies the topic type field for store raw receipt handle
            .brokerName(virtualQueue.brokerName())
            .queueId(rawHandle.queueId())
            .offset(rawHandle.messageOffset()); // Each message has a unique receipt handle, so offset is the same as start offset

        return rmqHandle.build();
    }

    public static String decodeReceiptHandle(String receiptHandle) {
        // The real receipt handle is stored in the topic type field
        return decode(receiptHandle).getTopicType();
    }

    public static class ReceiptHandleBuilder {
        private long startOffset;
        private long retrieveTime;
        private long invisibleTime;
        private int reviveQueueId;
        private String topicType;
        private String brokerName;
        private int queueId;
        private long offset;

        ReceiptHandleBuilder() {
        }

        public ReceiptHandleBuilder startOffset(final long startOffset) {
            this.startOffset = startOffset;
            return this;
        }

        public ReceiptHandleBuilder retrieveTime(final long retrieveTime) {
            this.retrieveTime = retrieveTime;
            return this;
        }

        public ReceiptHandleBuilder invisibleTime(final long invisibleTime) {
            this.invisibleTime = invisibleTime;
            return this;
        }

        public ReceiptHandleBuilder reviveQueueId(final int reviveQueueId) {
            this.reviveQueueId = reviveQueueId;
            return this;
        }

        public ReceiptHandleBuilder topicType(final String topicType) {
            this.topicType = topicType;
            return this;
        }

        public ReceiptHandleBuilder brokerName(final String brokerName) {
            this.brokerName = brokerName;
            return this;
        }

        public ReceiptHandleBuilder queueId(final int queueId) {
            this.queueId = queueId;
            return this;
        }

        public ReceiptHandleBuilder offset(final long offset) {
            this.offset = offset;
            return this;
        }

        public String build() {
            return startOffset
                + MessageConst.KEY_SEPARATOR + retrieveTime + MessageConst.KEY_SEPARATOR + invisibleTime
                + MessageConst.KEY_SEPARATOR + reviveQueueId + MessageConst.KEY_SEPARATOR + topicType
                + MessageConst.KEY_SEPARATOR + brokerName + MessageConst.KEY_SEPARATOR + queueId
                + MessageConst.KEY_SEPARATOR + offset;
        }
    }
}
