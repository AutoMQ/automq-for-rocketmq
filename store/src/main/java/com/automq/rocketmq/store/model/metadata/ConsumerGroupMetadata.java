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

package com.automq.rocketmq.store.model.metadata;

import java.util.Objects;

public class ConsumerGroupMetadata {
    private final long consumerGroupId;
    private long consumeOffset;
    private long ackOffset;
    private long retryConsumeOffset;
    private long retryAckOffset;
    private final long version;

    public ConsumerGroupMetadata(long consumerGroupId) {
        this.consumerGroupId = consumerGroupId;
        this.version = 0;
    }

    public ConsumerGroupMetadata(long consumerGroupId, long consumeOffset, long ackOffset, long retryConsumeOffset,
        long retryAckOffset, long version) {
        this.consumerGroupId = consumerGroupId;
        this.consumeOffset = consumeOffset;
        this.ackOffset = ackOffset;
        this.retryConsumeOffset = retryConsumeOffset;
        this.retryAckOffset = retryAckOffset;
        this.version = version;
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

    public void advanceAckOffset(long ackOffset) {
        if (ackOffset > this.ackOffset) {
            this.ackOffset = ackOffset;
        }
    }

    public void advanceRetryConsumeOffset(long retryConsumeOffset) {
        if (retryConsumeOffset > this.retryConsumeOffset) {
            this.retryConsumeOffset = retryConsumeOffset;
        }
    }

    public void advanceRetryAckOffset(long retryAckOffset) {
        if (retryAckOffset > this.retryAckOffset) {
            this.retryAckOffset = retryAckOffset;
        }
    }

    public long getVersion() {
        return version;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        ConsumerGroupMetadata metadata = (ConsumerGroupMetadata) o;
        return consumerGroupId == metadata.consumerGroupId && consumeOffset == metadata.consumeOffset && ackOffset == metadata.ackOffset && retryConsumeOffset == metadata.retryConsumeOffset && retryAckOffset == metadata.retryAckOffset && version == metadata.version;
    }

    @Override
    public int hashCode() {
        return Objects.hash(consumerGroupId, consumeOffset, ackOffset, retryConsumeOffset, retryAckOffset, version);
    }
}
