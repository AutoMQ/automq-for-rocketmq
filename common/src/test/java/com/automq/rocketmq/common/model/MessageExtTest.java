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

package com.automq.rocketmq.common.model;

import com.automq.rocketmq.common.model.generated.Message;
import java.nio.ByteBuffer;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MessageExtTest {
    @Test
    void build() {
        assertThrowsExactly(IllegalArgumentException.class, () -> MessageExt.Builder.builder().build());
        assertThrowsExactly(IllegalArgumentException.class, () ->
            MessageExt.Builder.builder().message(Message.getRootAsMessage(ByteBuffer.allocate(32))).build());

        MessageExt messageExt = MessageExt.Builder.builder()
            .message(Message.getRootAsMessage(ByteBuffer.allocate(32)))
            .offset(100)
            .receiptHandle("receiptHandle")
            .build();
        assertEquals(100, messageExt.offset());
        assertTrue(messageExt.receiptHandle().isPresent());
        assertEquals("receiptHandle", messageExt.receiptHandle().get());

        assertEquals(0, messageExt.reconsumeCount());
        messageExt.setReconsumeCount(1);
        assertEquals(1, messageExt.reconsumeCount());
    }
}