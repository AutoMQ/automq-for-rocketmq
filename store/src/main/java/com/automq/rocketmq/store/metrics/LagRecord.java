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
