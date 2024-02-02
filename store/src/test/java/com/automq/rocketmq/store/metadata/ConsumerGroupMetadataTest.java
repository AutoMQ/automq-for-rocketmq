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
        metadata.advanceAckOffset(1L);
        assertEquals(1L, metadata.getAckOffset());

        metadata.advanceAckOffset(3L);
        assertEquals(3L, metadata.getAckOffset());

        metadata.advanceAckOffset(6L);
        assertEquals(6L, metadata.getAckOffset());
    }


}

