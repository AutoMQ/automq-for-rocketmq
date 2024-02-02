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

package com.automq.rocketmq.common.model;

import com.automq.rocketmq.common.model.generated.FlatMessage;
import com.automq.rocketmq.common.util.FlatMessageUtil;
import java.nio.ByteBuffer;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FlatMessageExtTest {
    @Test
    void build() {
        assertThrowsExactly(IllegalArgumentException.class, () -> FlatMessageExt.Builder.builder().build());
        assertThrowsExactly(IllegalArgumentException.class, () ->
            FlatMessageExt.Builder.builder().message(FlatMessage.getRootAsFlatMessage(ByteBuffer.allocate(32))).build());

        FlatMessageExt messageExt = FlatMessageExt.Builder.builder()
            .message(FlatMessageUtil.mockFlatMessage())
            .offset(100)
            .receiptHandle("receiptHandle")
            .build();
        assertEquals(100, messageExt.offset());
        assertTrue(messageExt.receiptHandle().isPresent());
        assertEquals("receiptHandle", messageExt.receiptHandle().get());

        assertEquals(0, messageExt.deliveryAttempts());
        messageExt.setDeliveryAttempts(10);
        assertEquals(10, messageExt.deliveryAttempts());
    }
}