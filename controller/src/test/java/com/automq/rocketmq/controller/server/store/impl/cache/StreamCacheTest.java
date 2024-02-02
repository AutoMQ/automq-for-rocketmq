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

package com.automq.rocketmq.controller.server.store.impl.cache;

import apache.rocketmq.controller.v1.StreamRole;
import apache.rocketmq.controller.v1.StreamState;
import com.automq.rocketmq.metadata.dao.Stream;
import java.util.List;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class StreamCacheTest {

    @Test
    public void testApply() {
        StreamCache cache = new StreamCache();
        Stream stream = new Stream();
        stream.setStreamRole(StreamRole.STREAM_ROLE_DATA);
        stream.setRangeId(1);
        stream.setTopicId(2L);
        stream.setQueueId(3);
        stream.setId(4L);
        stream.setEpoch(5L);
        stream.setSrcNodeId(6);
        stream.setDstNodeId(7);
        stream.setState(StreamState.DELETED);

        cache.apply(List.of(stream));

        Assertions.assertEquals(0, cache.streamQuantity());
        stream.setState(StreamState.OPEN);
        cache.apply(List.of(stream));
        Assertions.assertEquals(1, cache.streamQuantity());
        Assertions.assertEquals(1, cache.streamNumOfNode(7));
        Assertions.assertEquals(0, cache.streamNumOfNode(6));
    }

    @Test
    public void testStreamsOf() {
        StreamCache cache = new StreamCache();
        Stream stream = new Stream();
        stream.setStreamRole(StreamRole.STREAM_ROLE_DATA);
        stream.setRangeId(1);
        stream.setTopicId(2L);
        stream.setQueueId(3);
        stream.setId(4L);
        stream.setEpoch(5L);
        stream.setSrcNodeId(6);
        stream.setDstNodeId(7);
        stream.setState(StreamState.OPEN);
        cache.apply(List.of(stream));

        List<Long> streams = cache.streamsOf(2, 3);
        Assertions.assertFalse(streams.isEmpty());
    }

}