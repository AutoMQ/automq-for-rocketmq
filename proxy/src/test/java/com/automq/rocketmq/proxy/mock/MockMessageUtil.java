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

package com.automq.rocketmq.proxy.mock;

import com.automq.rocketmq.common.model.FlatMessageExt;
import com.automq.rocketmq.common.model.generated.FlatMessage;
import com.automq.rocketmq.common.model.generated.FlatMessageT;
import com.automq.rocketmq.common.model.generated.KeyValueT;
import com.automq.rocketmq.common.model.generated.SystemPropertiesT;
import com.google.flatbuffers.FlatBufferBuilder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class MockMessageUtil {

    public static final String DEFAULT_PAYLOAD = "Hello, AutoMQ Message";
    public static final String DEFAULT_MESSAGE_GROUP = "test-group";
    public static final String DEFAULT_KEYS = "key-a key-b";
    public static final String USER_PROPERTIES_0_KEY = "key-0";
    public static final String USER_PROPERTIES_0_VALUE = "value-0";
    public static final String DEFAULT_MESSAGE_ID = "0000000001";
    public static final long DEFAULT_MESSAGE_OFFSET = 13;

    public static FlatMessageExt buildMessage(long topicId, int queueId, String tag) {
        FlatMessageT flatMessageT = new FlatMessageT();
        flatMessageT.setTopicId(topicId);
        flatMessageT.setQueueId(queueId);
        flatMessageT.setPayload(DEFAULT_PAYLOAD.getBytes(StandardCharsets.UTF_8));
        flatMessageT.setMessageGroup(DEFAULT_MESSAGE_GROUP);
        flatMessageT.setKeys(DEFAULT_KEYS);
        flatMessageT.setTag(tag);

        List<KeyValueT> userProperties = new ArrayList<>();
        KeyValueT keyValueT = new KeyValueT();
        keyValueT.setKey(USER_PROPERTIES_0_KEY);
        keyValueT.setValue(USER_PROPERTIES_0_VALUE);
        userProperties.add(keyValueT);
        flatMessageT.setUserProperties(userProperties.toArray(new KeyValueT[0]));

        SystemPropertiesT systemPropertiesT = new SystemPropertiesT();
        systemPropertiesT.setBornTimestamp(System.currentTimeMillis());
        systemPropertiesT.setStoreTimestamp(System.currentTimeMillis());
        systemPropertiesT.setMessageId(DEFAULT_MESSAGE_ID);
        systemPropertiesT.setDeliveryAttempts(1);
        systemPropertiesT.setOriginalQueueOffset(0);
        flatMessageT.setSystemProperties(systemPropertiesT);

        FlatBufferBuilder builder = new FlatBufferBuilder(1024);
        int root = FlatMessage.pack(builder, flatMessageT);
        builder.finish(root);
        FlatMessage flatMessage = FlatMessage.getRootAsFlatMessage(builder.dataBuffer());
        return FlatMessageExt.Builder.builder()
            .message(flatMessage)
            .offset(DEFAULT_MESSAGE_OFFSET)
            .build();
    }
}
