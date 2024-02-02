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
