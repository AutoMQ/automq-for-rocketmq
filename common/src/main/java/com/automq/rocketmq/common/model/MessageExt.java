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

    private final Message message;

    private final Map<String, String> systemPropertyMap;
    private String receiptHandle;

    public MessageExt(Message message, Map<String, String> systemPropertyMap) {
        this.message = message;
        this.systemPropertyMap = systemPropertyMap;
    }

    public MessageExt(Message message, String receiptHandle) {
        this(message, new HashMap<>());
        this.receiptHandle = receiptHandle;
    }

    public int getReconsumeCount() {
        String reconsumeCount = systemPropertyMap.get(SYSTEM_PROPERTY_RECONSUME_COUNT);
        if (Strings.isNullOrEmpty(reconsumeCount)) {
            return 0;
        }
        return Integer.parseInt(reconsumeCount);
    }

    public void setReconsumeCount(int reconsumeCount) {
        systemPropertyMap.put(SYSTEM_PROPERTY_RECONSUME_COUNT, String.valueOf(reconsumeCount));
    }

    public Map<String, String> systemPropertyMap() {
        return systemPropertyMap;
    }

    public Message message() {
        return message;
    }

    public Optional<String> receiptHandle() {
        return Optional.ofNullable(receiptHandle);
    }
}
