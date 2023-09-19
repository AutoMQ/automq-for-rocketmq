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
import com.google.common.base.Strings;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class MessageExt {
    private static final String SYSTEM_PROPERTY_RECONSUME_COUNT = "RECONSUME_COUNT";

    private Message message;

    // Only system properties can be mutated. The rest of the variables are immutable.
    private Map<String, String> systemProperties;
    private String receiptHandle;

    // The offset of the message in the queue
    private long offset = -1;

    private MessageExt() {

    }

    public static class Builder {
        private final MessageExt messageExt;

        private Builder() {
            messageExt = new MessageExt();
        }

        public static Builder builder() {
            return new Builder();
        }

        public Builder message(Message message) {
            messageExt.message = message;
            return this;
        }

        public Builder systemProperties(Map<String, String> systemProperties) {
            messageExt.systemProperties = systemProperties;
            return this;
        }

        public Builder receiptHandle(String receiptHandle) {
            messageExt.receiptHandle = receiptHandle;
            return this;
        }

        public Builder offset(long offset) {
            messageExt.offset = offset;
            return this;
        }

        public MessageExt build() {
            if (messageExt.message == null) {
                throw new IllegalArgumentException("Message not set");
            }
            if (messageExt.offset < 0) {
                throw new IllegalArgumentException("Message offset not set or invalid");
            }
            if (messageExt.systemProperties == null) {
                messageExt.systemProperties = new HashMap<>();
            }
            return messageExt;
        }
    }

    public Map<String, String> systemProperties() {
        return systemProperties;
    }

    public Message message() {
        return message;
    }

    public Optional<String> receiptHandle() {
        return Optional.ofNullable(receiptHandle);
    }

    public long offset() {
        return offset;
    }

    public int reconsumeCount() {
        String reconsumeCount = systemProperties.get(SYSTEM_PROPERTY_RECONSUME_COUNT);
        if (Strings.isNullOrEmpty(reconsumeCount)) {
            return 0;
        }
        return Integer.parseInt(reconsumeCount);
    }

    public void setReconsumeCount(int reconsumeCount) {
        systemProperties.put(SYSTEM_PROPERTY_RECONSUME_COUNT, String.valueOf(reconsumeCount));
    }
}
