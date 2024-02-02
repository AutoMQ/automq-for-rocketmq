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
            .startOffset(rawHandle.operationId())
            .retrieveTime(System.currentTimeMillis())
            .invisibleTime(invisibleTime)
            .reviveQueueId(0) // No revive queue in S3RocketMQ
            .topicType(rawReceiptHandle) // S3RocketMQ occupies the topic type field for store raw receipt handle
            .brokerName(virtualQueue.brokerName())
            .queueId(rawHandle.queueId())
            .offset(rawHandle.operationId()); // Each message has a unique receipt handle, so offset is the same as start offset

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
