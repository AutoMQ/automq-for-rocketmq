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

package com.automq.rocketmq.store.model.metadata;

import java.util.Objects;
import java.util.concurrent.ConcurrentSkipListMap;

public class ConsumerGroupMetadata {
    private final long consumerGroupId;
    private long consumeOffset;
    private long ackOffset;
    private long retryConsumeOffset;
    private long retryAckOffset;
    private final ConcurrentSkipListMap<Long/*offset*/, Integer/*times*/> consumeTimes;

    public ConsumerGroupMetadata(long consumerGroupId) {
        this.consumerGroupId = consumerGroupId;
        this.consumeTimes = new ConcurrentSkipListMap<>();
    }

    public ConsumerGroupMetadata(long consumerGroupId, long consumeOffset, long ackOffset, long retryConsumeOffset,
        long retryAckOffset, ConcurrentSkipListMap<Long, Integer> consumeTimes) {
        this.consumerGroupId = consumerGroupId;
        this.consumeOffset = consumeOffset;
        this.ackOffset = ackOffset;
        this.retryConsumeOffset = retryConsumeOffset;
        this.retryAckOffset = retryAckOffset;
        this.consumeTimes = consumeTimes;
    }

    public long getConsumeOffset() {
        return consumeOffset;
    }

    public long getAckOffset() {
        return ackOffset;
    }

    public long getRetryAckOffset() {
        return retryAckOffset;
    }

    public long getRetryConsumeOffset() {
        return retryConsumeOffset;
    }

    public long getConsumerGroupId() {
        return consumerGroupId;
    }

    public void setConsumeOffset(long consumeOffset) {
        this.consumeOffset = consumeOffset;
    }

    public void setAckOffset(long ackOffset) {
        this.ackOffset = ackOffset;
        // when ack offset is updated, we should clear the consume times
        this.consumeTimes.subMap(0L, ackOffset).clear();
    }

    public void setRetryConsumeOffset(long retryConsumeOffset) {
        this.retryConsumeOffset = retryConsumeOffset;
    }

    public void setRetryAckOffset(long retryAckOffset) {
        this.retryAckOffset = retryAckOffset;
    }

    public ConcurrentSkipListMap<Long, Integer> getConsumeTimes() {
        return consumeTimes;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        ConsumerGroupMetadata metadata = (ConsumerGroupMetadata) o;
        return consumerGroupId == metadata.consumerGroupId && consumeOffset == metadata.consumeOffset && ackOffset == metadata.ackOffset && retryConsumeOffset == metadata.retryConsumeOffset && retryAckOffset == metadata.retryAckOffset && Objects.equals(consumeTimes, metadata.consumeTimes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(consumerGroupId, consumeOffset, ackOffset, retryConsumeOffset, retryAckOffset, consumeTimes);
    }
}
