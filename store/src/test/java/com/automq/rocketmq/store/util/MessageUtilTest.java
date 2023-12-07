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