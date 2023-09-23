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

import com.automq.rocketmq.common.model.MessageExt;
import com.automq.rocketmq.common.model.generated.Message;
import com.automq.rocketmq.store.model.stream.SingleRecord;
import com.automq.rocketmq.store.mock.MemoryStreamClient;
import java.nio.ByteBuffer;
import java.util.HashMap;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class MessageUtilTest {

    @Test
    void transferToMessageExt() {
        MessageExt messageExt = MessageUtil.transferToMessageExt(
            new MemoryStreamClient.RecordBatchWithContextWrapper(
                new SingleRecord(new HashMap<>(), ByteBuffer.allocate(10)),
                100));
        assertNotNull(messageExt);
        assertNotNull(messageExt.message());
        assertEquals(100, messageExt.offset());
        assertEquals(0, messageExt.reconsumeCount());
        assertFalse(messageExt.receiptHandle().isPresent());
    }

    @Test
    void transferToMessage() {
        Message message = MessageUtil.transferToMessage(1, 2, "tag", new HashMap<>(), new byte[] {});
        assertNotNull(message);
        assertEquals(1, message.topicId());
        assertEquals(2, message.queueId());
        assertEquals("tag", message.tag());
    }
}