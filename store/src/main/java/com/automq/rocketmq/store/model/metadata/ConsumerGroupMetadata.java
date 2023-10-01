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

import java.util.HashMap;
import java.util.Map;

public class ConsumerGroupMetadata {
    private long consumerGroupId;
    private long consumeOffset;
    private long ackOffset;
    private long retryOffset;
    private Map<Long/*offset*/, Integer/*retryTimes*/> retryTimesMap;
    private Map<Long/*offset*/, Integer/*consumeTimes*/> consumeTimesMap;

    public ConsumerGroupMetadata(long consumerGroupId) {
        this.consumerGroupId = consumerGroupId;
        this.retryTimesMap = new HashMap<>();
        this.consumeTimesMap = new HashMap<>();
    }

    public ConsumerGroupMetadata(long consumerGroupId, long consumeOffset, long ackOffset,
        long retryOffset,
        Map<Long, Integer> retryTimesMap, Map<Long, Integer> consumeTimesMap) {
        this.consumerGroupId = consumerGroupId;
        this.consumeOffset = consumeOffset;
        this.ackOffset = ackOffset;
        this.retryOffset = retryOffset;
        this.retryTimesMap = retryTimesMap;
        this.consumeTimesMap = consumeTimesMap;
    }

    public long getConsumeOffset() {
        return consumeOffset;
    }

    public long getAckOffset() {
        return ackOffset;
    }

    public long getRetryOffset() {
        return retryOffset;
    }

    public Map<Long, Integer> getRetryTimesMap() {
        return retryTimesMap;
    }

    public Map<Long, Integer> getConsumeTimesMap() {
        return consumeTimesMap;
    }

    public long getConsumerGroupId() {
        return consumerGroupId;
    }

    public void setConsumeOffset(long consumeOffset) {
        this.consumeOffset = consumeOffset;
    }

    public void setAckOffset(long ackOffset) {
        this.ackOffset = ackOffset;
    }

    public void setRetryOffset(long retryOffset) {
        this.retryOffset = retryOffset;
    }
}
