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
import java.util.Map;
import java.util.Optional;

public class MessageExt {
    private static final String SYSTEM_PROPERTY_RECONSUME_COUNT = "RECONSUME_COUNT";

    private final Message message;

    private final Map<String, String> systemProperties;
    private String receiptHandle;

    // The offset of the message in the queue
    private long offset;

    public MessageExt(Message message, Map<String, String> systemProperties, long offset) {
        this.message = message;
        this.systemProperties = systemProperties;
        this.offset = offset;
    }

    public int getReconsumeCount() {
        String reconsumeCount = systemProperties.get(SYSTEM_PROPERTY_RECONSUME_COUNT);
        if (Strings.isNullOrEmpty(reconsumeCount)) {
            return 0;
        }
        return Integer.parseInt(reconsumeCount);
    }

    public void setReconsumeCount(int reconsumeCount) {
        systemProperties.put(SYSTEM_PROPERTY_RECONSUME_COUNT, String.valueOf(reconsumeCount));
    }

    public Map<String, String> getSystemProperties() {
        return systemProperties;
    }

    public Message getMessage() {
        return message;
    }

    public Optional<String> getReceiptHandle() {
        return Optional.ofNullable(receiptHandle);
    }

    public void setReceiptHandle(String receiptHandle) {
        this.receiptHandle = receiptHandle;
    }

    public long getOffset() {
        return offset;
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }
}
