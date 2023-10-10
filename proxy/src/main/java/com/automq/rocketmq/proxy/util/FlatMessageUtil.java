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
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageExt;

import static com.automq.rocketmq.proxy.util.ReceiptHandleUtil.encodeReceiptHandle;

/**
 * An utility class to convert RocketMQ message models to {@link FlatMessage}, and vice versa.
 */
public class FlatMessageUtil {
    public static FlatMessage convertTo(long topicId, int queueId, String storeHost, Message rmqMessage) {
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

        int root = FlatMessage.pack(builder, flatMessageT);
        builder.finish(root);
        return FlatMessage.getRootAsFlatMessage(builder.dataBuffer());
    }

    public static MessageExt convertTo(FlatMessageExt flatMessage, String topicName, long invisibleTime) {
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
        String bornHost = systemProperties.bornHost();
        if (bornHost != null) {
            messageExt.setBornHost(new InetSocketAddress(bornHost, 0));
        }
        messageExt.setStoreTimestamp(systemProperties.storeTimestamp());
        String storeHost = systemProperties.storeHost();
        if (storeHost != null) {
            messageExt.setStoreHost(new InetSocketAddress(storeHost, 0));
        }
        messageExt.setMsgId(systemProperties.messageId());

        // Re-consume times is the number of delivery attempts minus 1.
        messageExt.setReconsumeTimes(systemProperties.deliveryAttempts() - 1);

        KeyValue.Vector propertiesVector = flatMessage.message().userPropertiesVector();
        for (int i = 0; i < propertiesVector.length(); i++) {
            KeyValue keyValue = propertiesVector.get(i);
            messageExt.putUserProperty(keyValue.key(), keyValue.value());
        }

        fillSystemProperties(messageExt, flatMessage, invisibleTime);

        return messageExt;
    }

    public static List<MessageExt> convertTo(List<FlatMessageExt> messageList, String topicName, long invisibleTime) {
        return messageList.stream()
            .map(messageExt -> convertTo(messageExt, topicName, invisibleTime))
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

        // Remove all system properties
        for (String systemPropertyKey : MessageConst.STRING_HASH_SET) {
            properties.remove(systemPropertyKey);
        }

        // TODO: Split transaction and timer properties
        // TODO: handle producer group for transaction message
        return systemPropertiesT;
    }

    private static void fillSystemProperties(MessageExt messageExt, FlatMessageExt flatMessage, long invisibleTime) {
        SystemProperties systemProperties = flatMessage.message().systemProperties();
        if (flatMessage.receiptHandle().isPresent()) {
            MessageAccessor.putProperty(messageExt, MessageConst.PROPERTY_POP_CK, encodeReceiptHandle(flatMessage.receiptHandle().get(), invisibleTime));
        }
        if (flatMessage.message().tag() != null) {
            MessageAccessor.putProperty(messageExt, MessageConst.PROPERTY_TAGS, flatMessage.message().tag());
        }
        if (flatMessage.message().keys() != null) {
            MessageAccessor.putProperty(messageExt, MessageConst.PROPERTY_KEYS, flatMessage.message().keys());
        }
        if (flatMessage.message().messageGroup() != null) {
            MessageAccessor.putProperty(messageExt, MessageConst.PROPERTY_SHARDING_KEY, flatMessage.message().messageGroup());
        }
        if (systemProperties.messageId() != null) {
            MessageAccessor.putProperty(messageExt, MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX, systemProperties.messageId());
        }
        if (systemProperties.traceContext() != null) {
            MessageAccessor.putProperty(messageExt, MessageConst.PROPERTY_TRACE_CONTEXT, systemProperties.traceContext());
        }
    }
}
