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

import com.automq.rocketmq.common.model.FlatMessageExt;
import com.automq.rocketmq.common.model.generated.FlatMessage;
import com.automq.rocketmq.common.model.generated.FlatMessageT;
import com.automq.rocketmq.common.model.generated.KeyValue;
import com.automq.rocketmq.common.model.generated.KeyValueT;
import com.automq.rocketmq.common.model.generated.SystemProperties;
import com.automq.rocketmq.common.model.generated.SystemPropertiesT;
import com.automq.rocketmq.proxy.model.VirtualQueue;
import com.google.common.base.Strings;
import com.google.flatbuffers.FlatBufferBuilder;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageExt;

/**
 * An utility class to convert RocketMQ message models to {@link FlatMessage}, and vice versa.
 */
public class FlatMessageUtil {
    public static FlatMessage convertFrom(long topicId, int queueId, String storeHost, Message rmqMessage) {
        FlatMessageT flatMessageT = new FlatMessageT();
        flatMessageT.setTopicId(topicId);
        flatMessageT.setQueueId(queueId);
        flatMessageT.setPayload(rmqMessage.getBody());

        // The properties of RocketMQ message contains user properties and system properties.
        // We should split them into two parts.
        Map<String, String> properties = rmqMessage.getProperties();

        SystemPropertiesT systemPropertiesT = splitSystemProperties(flatMessageT, properties);
        systemPropertiesT.setStoreTimestamp(System.currentTimeMillis());
        systemPropertiesT.setStoreHost(storeHost);
        systemPropertiesT.setDeliveryAttempts(1);
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

    public static MessageExt convertFrom(FlatMessageExt flatMessage, String topicName) {
        MessageExt messageExt = new MessageExt();

        VirtualQueue virtualQueue = new VirtualQueue(flatMessage.message().topicId(), flatMessage.message().queueId());

        messageExt.setTopic(topicName);
        messageExt.setBrokerName(virtualQueue.brokerName());
        // The original queue id always is 0.
        messageExt.setQueueId(0);
        messageExt.setQueueOffset(flatMessage.originalOffset());

        ByteBuffer payloadBuffer = flatMessage.message().payloadAsByteBuffer();

        // Convert buffer to byte array
        if (payloadBuffer.hasArray()) {
            messageExt.setBody(payloadBuffer.array());
        } else {
            byte[] payload = new byte[payloadBuffer.remaining()];
            payloadBuffer.get(payload);
            messageExt.setBody(payload);
        }

        SystemProperties systemProperties = flatMessage.message().systemProperties();
        messageExt.setBornTimestamp(systemProperties.bornTimestamp());
        messageExt.setBornHost(new InetSocketAddress(Objects.requireNonNull(systemProperties.bornHost()), 0));
        messageExt.setStoreTimestamp(systemProperties.storeTimestamp());
        messageExt.setStoreHost(new InetSocketAddress(Objects.requireNonNull(systemProperties.storeHost()), 0));
        messageExt.setMsgId(systemProperties.messageId());

        // Re-consume times is the number of delivery attempts minus 1.
        messageExt.setReconsumeTimes(systemProperties.deliveryAttempts());

        KeyValue.Vector propertiesVector = flatMessage.message().userPropertiesVector();
        for (int i = 0; i < propertiesVector.length(); i++) {
            KeyValue keyValue = propertiesVector.get(i);
            messageExt.putUserProperty(keyValue.key(), keyValue.value());
        }

        fillSystemProperties(messageExt, flatMessage);

        return messageExt;
    }

    public static List<MessageExt> convertFrom(List<FlatMessageExt> messageList, String topicName) {
        return messageList.stream()
            .map(messageExt -> convertFrom(messageExt, topicName))
            .toList();
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
            systemPropertiesT.setDeliveryAttempts(Integer.parseInt(reconsumeTimes));
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

    private static void fillSystemProperties(MessageExt messageExt, FlatMessageExt flatMessage) {
        SystemProperties systemProperties = flatMessage.message().systemProperties();
        MessageAccessor.putProperty(messageExt, MessageConst.PROPERTY_POP_CK, flatMessage.receiptHandle().orElseThrow());
        MessageAccessor.putProperty(messageExt, MessageConst.PROPERTY_TAGS, flatMessage.message().tag());
        MessageAccessor.putProperty(messageExt, MessageConst.PROPERTY_KEYS, flatMessage.message().keys());
        MessageAccessor.putProperty(messageExt, MessageConst.PROPERTY_SHARDING_KEY, flatMessage.message().messageGroup());
        MessageAccessor.putProperty(messageExt, MessageConst.PROPERTY_TRACE_CONTEXT, systemProperties.traceContext());
    }
}
