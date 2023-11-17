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