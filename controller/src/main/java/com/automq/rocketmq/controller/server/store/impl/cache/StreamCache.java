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

package com.automq.rocketmq.controller.server.store.impl.cache;

import apache.rocketmq.controller.v1.StreamState;
import com.automq.rocketmq.metadata.dao.Stream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class StreamCache {

    private final ConcurrentMap<Long, Stream> streams;

    public StreamCache() {
        streams = new ConcurrentHashMap<>();
    }

    public int streamNumOfNode(int nodeId) {
        int count = 0;
        for (Map.Entry<Long, Stream> entry : streams.entrySet()) {
            if (entry.getValue().getDstNodeId() == nodeId) {
                count++;
            }
        }
        return count;
    }

    public List<Long> streamsOf(long topicId, int queueId) {
        List<Long> result = new ArrayList<>();
        for (Map.Entry<Long, Stream> entry : streams.entrySet()) {
            if (entry.getValue().getTopicId() == topicId && entry.getValue().getQueueId() == queueId) {
                result.add(entry.getKey());
            }
        }
        return result;
    }

    public void apply(Collection<Stream> streams) {
        for (Stream stream : streams) {
            cacheItem(stream);
        }
    }

    private void cacheItem(Stream stream) {
        if (stream.getState() == StreamState.DELETED) {
            streams.remove(stream.getId());
        } else {
            streams.put(stream.getId(), stream);
        }
    }

    public int streamQuantity() {
        return streams.size();
    }
}
