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

package com.automq.rocketmq.store.util;

import com.automq.rocketmq.common.model.FlatMessageExt;
import com.automq.rocketmq.store.mock.MemoryStreamClient;
import com.automq.rocketmq.store.mock.MockMessageUtil;
import com.automq.rocketmq.store.model.stream.SingleRecord;
import java.nio.ByteBuffer;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class MessageUtilTest {

    @Test
    void transferToMessageExt() {
        ByteBuffer messageBuffer = MockMessageUtil.buildMessage(1, 0, "TagA");
        FlatMessageExt messageExt = FlatMessageUtil.transferToMessageExt(
            new MemoryStreamClient.RecordBatchWithContextWrapper(
                new SingleRecord(messageBuffer),
                100));
        assertNotNull(messageExt);
        assertNotNull(messageExt.message());
        assertEquals(100, messageExt.offset());
        assertEquals(1, messageExt.deliveryAttempts());
        assertFalse(messageExt.receiptHandle().isPresent());
    }
}