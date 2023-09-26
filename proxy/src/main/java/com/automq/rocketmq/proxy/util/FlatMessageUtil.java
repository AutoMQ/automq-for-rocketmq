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

package com.automq.rocketmq.proxy.util;

import com.automq.rocketmq.common.model.generated.FlatMessage;
import com.automq.rocketmq.common.model.generated.FlatMessageT;
import com.automq.rocketmq.common.model.generated.KeyValueT;
import com.automq.rocketmq.common.model.generated.SystemPropertiesT;
import com.google.common.base.Strings;
import com.google.flatbuffers.FlatBufferBuilder;
import java.util.Map;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageConst;

/**
 * An utility class to convert RocketMQ message models to {@link FlatMessage}, and vice versa.
 */
public class FlatMessageUtil {
    public static FlatMessage transferFrom(long topicId, int queueId, Message rmqMessage) {
        FlatMessageT flatMessageT = new FlatMessageT();
        flatMessageT.setTopicId(topicId);
        flatMessageT.setQueueId(queueId);
        flatMessageT.setPayload(rmqMessage.getBody());

        // The properties of RocketMQ message contains user properties and system properties.
        // We should split them into two parts.
        Map<String, String> properties = rmqMessage.getProperties();

        SystemPropertiesT systemPropertiesT = splitSystemProperties(flatMessageT, properties);
        flatMessageT.setSystemProperties(systemPropertiesT);

        // The rest of the properties are user properties.
        KeyValueT[] userProperties = properties.entrySet().stream()
            .map(entry -> {
                KeyValueT keyValueT = new KeyValueT();
                keyValueT.setKey(entry.getKey());
                keyValueT.setValue(entry.getValue());
                return keyValueT;
            })
            .toArray(KeyValueT[]::new);
        flatMessageT.setUserProperties(userProperties);

        FlatBufferBuilder builder = new FlatBufferBuilder();
        // Enable force_defaults to enable inplace update for default values.
        // Currently, we only need to update the default value of `deliveryAttempt`.
        builder.forceDefaults(true);

        int root = FlatMessage.pack(builder, flatMessageT);
        builder.finish(root);
        return FlatMessage.getRootAsFlatMessage(builder.dataBuffer());
    }

    /**
     * Split the properties of RocketMQ message into user properties and system properties.
     * <p>
     * The properties parameters will be mutated, the system properties will be removed from it.
     *
     * @param flatMessageT the FlatMessageT to be filled
     * @param properties   the properties of RocketMQ message
     * @return the system properties of RocketMQ message
     */
    private static SystemPropertiesT splitSystemProperties(FlatMessageT flatMessageT, Map<String, String> properties) {
        SystemPropertiesT systemPropertiesT = new SystemPropertiesT();

        flatMessageT.setTag(properties.remove(MessageConst.PROPERTY_TAGS));
        flatMessageT.setKeys(properties.remove(MessageConst.PROPERTY_KEYS));
        String messageGroup = properties.remove(MessageConst.PROPERTY_SHARDING_KEY);
        if (!Strings.isNullOrEmpty(messageGroup)) {
            flatMessageT.setMessageGroup(messageGroup);
        }

        systemPropertiesT.setMessageId(properties.remove(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX));
        systemPropertiesT.setTraceContext(properties.remove(MessageConst.PROPERTY_TRACE_CONTEXT));
        String reconsumeTimes = properties.remove(MessageConst.PROPERTY_RECONSUME_TIME);
        if (!Strings.isNullOrEmpty(reconsumeTimes)) {
            systemPropertiesT.setDeliveryAttempt(Integer.parseInt(reconsumeTimes));
        }

        systemPropertiesT.setBornHost(properties.remove(MessageConst.PROPERTY_BORN_HOST));
        String bornTimestampStr = properties.remove(MessageConst.PROPERTY_BORN_TIMESTAMP);
        if (!Strings.isNullOrEmpty(bornTimestampStr)) {
            systemPropertiesT.setBornTimestamp(Long.parseLong(bornTimestampStr));
        }
        // TODO: Split transaction and timer properties
        // TODO: handle producer group for transaction message
        return systemPropertiesT;
    }
}
