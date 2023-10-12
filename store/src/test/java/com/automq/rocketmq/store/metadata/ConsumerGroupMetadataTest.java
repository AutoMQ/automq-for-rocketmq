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

package com.automq.rocketmq.store.metadata;

import com.automq.rocketmq.store.model.metadata.ConsumerGroupMetadata;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ConsumerGroupMetadataTest {

    private static final long CONSUMER_GROUP_ID = 1L;

    @Test
    public void ack() {
        ConsumerGroupMetadata metadata = new ConsumerGroupMetadata(CONSUMER_GROUP_ID);
        assertEquals(0L, metadata.getAckOffset());
        metadata.setAckOffset(1L);
        assertEquals(1L, metadata.getAckOffset());

        // add consume times
        metadata.getConsumeTimes().put(0L, 1);
        metadata.getConsumeTimes().put(1L, 2);
        metadata.getConsumeTimes().put(2L, 3);
        metadata.getConsumeTimes().put(4L, 5);
        metadata.getConsumeTimes().put(5L, 6);
        metadata.getConsumeTimes().put(6L, 7);

        metadata.setAckOffset(3L);
        assertEquals(3L, metadata.getAckOffset());
        assertEquals(3, metadata.getConsumeTimes().size());
        assertEquals(5, metadata.getConsumeTimes().get(4L));
        assertEquals(6, metadata.getConsumeTimes().get(5L));
        assertEquals(7, metadata.getConsumeTimes().get(6L));

        metadata.setAckOffset(6L);
        assertEquals(6L, metadata.getAckOffset());
        assertEquals(1, metadata.getConsumeTimes().size());
        assertEquals(7, metadata.getConsumeTimes().get(6L));

    }


}

