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

package com.automq.rocketmq.store.metrics;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

public record LagRecord(
    long topicId,
    int queueId,
    long consumerGroupId,
    boolean retry,
    long lag,
    long LagLatency,
    long inflight,
    long queueingLatency,
    long ready
) {
    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        LagRecord record = (LagRecord) o;

        return new EqualsBuilder().append(topicId, record.topicId).append(queueId, record.queueId).append(consumerGroupId, record.consumerGroupId).append(retry, record.retry).isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37).append(topicId).append(queueId).append(consumerGroupId).append(retry).toHashCode();
    }
}
