/*
 * Copyright 2024, AutoMQ HK Limited.
 *
 * Use of this software is governed by the Business Source License
 * included in the file BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
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
