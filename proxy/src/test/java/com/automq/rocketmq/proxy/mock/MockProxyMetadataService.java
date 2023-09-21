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

package com.automq.rocketmq.proxy.mock;

import com.automq.rocketmq.metadata.ProxyMetadataService;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class MockProxyMetadataService implements ProxyMetadataService {
    Map<Long, Long> offsetMap = new HashMap<>();

    @Override
    public long queryTopicId(String name) {
        return 2;
    }

    @Override
    public Set<Integer> queryAssignmentQueueSet(long topicId) {
        return Set.of(0, 2, 4);
    }

    @Override
    public long queryConsumerGroupId(String name) {
        return 8;
    }

    @Override
    public long queryConsumerOffset(long consumerGroupId, long topicId, int queueId) {
        if (offsetMap.containsKey(consumerGroupId + topicId + queueId)) {
            return offsetMap.get(consumerGroupId + topicId + queueId);
        }
        return 0;
    }

    @Override
    public void updateConsumerOffset(long consumerGroupId, long topicId, int queueId, long offset, boolean retry) {
        offsetMap.put(consumerGroupId + topicId + queueId, offset);
    }
}
