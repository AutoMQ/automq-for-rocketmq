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

package com.automq.rocketmq.common.util;

import com.automq.rocketmq.common.model.generated.FlatMessage;
import com.automq.rocketmq.common.model.generated.FlatMessageT;
import com.automq.rocketmq.common.model.generated.KeyValueT;
import com.automq.rocketmq.common.model.generated.SystemPropertiesT;
import com.google.flatbuffers.FlatBufferBuilder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class FlatMessageUtil {
    public static FlatMessage mockFlatMessage() {
        FlatMessageT flatMessageT = new FlatMessageT();
        flatMessageT.setTopicId(1);
        flatMessageT.setQueueId(0);
        flatMessageT.setPayload("Hello, AutoMQ Message".getBytes(StandardCharsets.UTF_8));
        flatMessageT.setMessageGroup("test-group");
        flatMessageT.setKeys("keys");
        flatMessageT.setTag("TagA");

        List<KeyValueT> userProperties = new ArrayList<>();
        KeyValueT keyValueT = new KeyValueT();
        keyValueT.setKey("key");
        keyValueT.setValue("value");
        userProperties.add(keyValueT);
        flatMessageT.setUserProperties(userProperties.toArray(new KeyValueT[0]));

        SystemPropertiesT systemPropertiesT = new SystemPropertiesT();
        systemPropertiesT.setBornTimestamp(System.currentTimeMillis());
        systemPropertiesT.setStoreTimestamp(System.currentTimeMillis());
        systemPropertiesT.setMessageId("0000000001");
        systemPropertiesT.setDeliveryAttempts(0);
        flatMessageT.setSystemProperties(systemPropertiesT);

        FlatBufferBuilder builder = new FlatBufferBuilder(1024);
        int root = FlatMessage.pack(builder, flatMessageT);
        builder.finish(root);
        return FlatMessage.getRootAsFlatMessage(builder.dataBuffer());
    }
}
