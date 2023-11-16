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
import com.automq.rocketmq.common.system.MessageConstants;
import com.automq.rocketmq.proxy.model.ProxyContextExt;
import com.automq.rocketmq.proxy.model.VirtualQueue;
import com.google.common.base.Strings;
import com.google.flatbuffers.FlatBufferBuilder;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.instrumentation.annotations.WithSpan;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.remoting.protocol.subscription.ExponentialRetryPolicy;
import org.apache.rocketmq.remoting.protocol.subscription.GroupRetryPolicy;

import static com.automq.rocketmq.proxy.util.ReceiptHandleUtil.encodeReceiptHandle;

/**
 * An utility class to convert RocketMQ message models to {@link FlatMessage}, and vice versa.
 */
public class FlatMessageUtil {
    private static final long[] DELAY_LEVEL_ARRAY = new long[] {
        TimeUnit.SECONDS.toMillis(1),
        TimeUnit.SECONDS.toMillis(5),
        TimeUnit.SECONDS.toMillis(10),
        TimeUnit.SECONDS.toMillis(30),
        TimeUnit.MINUTES.toMillis(1),
        TimeUnit.MINUTES.toMillis(2),
        TimeUnit.MINUTES.toMillis(3),
        TimeUnit.MINUTES.toMillis(4),
        TimeUnit.MINUTES.toMillis(5),
        TimeUnit.MINUTES.toMillis(6),
        TimeUnit.MINUTES.toMillis(7),
        TimeUnit.MINUTES.toMillis(8),
        TimeUnit.MINUTES.toMillis(9),
        TimeUnit.MINUTES.toMillis(10),
        TimeUnit.MINUTES.toMillis(20),
        TimeUnit.MINUTES.toMillis(30),
        TimeUnit.HOURS.toMillis(1),
        TimeUnit.HOURS.toMillis(2)
    };

    public static long calculateDeliveryTimestamp(GroupRetryPolicy retryPolicy, int delayLevel) {
        switch (retryPolicy.getType()) {
            case CUSTOMIZED -> {
                // The parameter of nextDelayDuration is the last consume times.
                return retryPolicy.getCustomizedRetryPolicy().nextDelayDuration(delayLevel - 2);
            }
            case EXPONENTIAL -> {
                ExponentialRetryPolicy policy = retryPolicy.getExponentialRetryPolicy();
                double result = policy.getInitial() + Math.pow(policy.getMultiplier(), delayLevel);
                return Math.min((long) result, policy.getMax());
            }
            default -> throw new IllegalArgumentException("Unsupported retry policy type: " + retryPolicy.getType());
        }
    }

    public static long calculateDeliveryTimestamp(int delayLevel) {
        delayLevel = Math.min(Math.max(0, delayLevel), DELAY_LEVEL_ARRAY.length - 1);
        return System.currentTimeMillis() + DELAY_LEVEL_ARRAY[delayLevel];
    }

    public static long calculateDeliveryTimestamp(Message message) {
        int delayLevel = message.getDelayTimeLevel();
        if (delayLevel > 0) {
            return calculateDeliveryTimestamp(delayLevel);
        }

        long deliveryTimestamp = 0;
        try {
            if (message.getProperty(MessageConst.PROPERTY_TIMER_DELAY_SEC) != null) {
                deliveryTimestamp = System.currentTimeMillis() + Long.parseLong(message.getProperty(MessageConst.PROPERTY_TIMER_DELAY_SEC)) * 1000;
            } else if (message.getProperty(MessageConst.PROPERTY_TIMER_DELAY_MS) != null) {
                deliveryTimestamp = System.currentTimeMillis() + Long.parseLong(message.getProperty(MessageConst.PROPERTY_TIMER_DELAY_MS));
            } else {
                deliveryTimestamp = Long.parseLong(message.getProperty(MessageConst.PROPERTY_TIMER_DELIVER_MS));
            }
        } catch (Exception e) {
            return deliveryTimestamp;
        }
        return deliveryTimestamp;
    }

    @WithSpan(kind = SpanKind.SERVER)
    public static FlatMessage convertTo(ProxyContextExt context, long topicId, int queueId, String storeHost,
        Message rmqMessage) {
        FlatMessageT flatMessageT = new FlatMessageT();
        flatMessageT.setTopicId(topicId);
        flatMessageT.setQueueId(queueId);
        flatMessageT.setPayload(rmqMessage.getBody());

        // The properties of RocketMQ message contains user properties and system properties.
        // We should split them into two parts.
        Map<String, String> properties = rmqMessage.getProperties();
        long deliveryTimestamp = calculateDeliveryTimestamp(rmqMessage);

        SystemPropertiesT systemPropertiesT = splitSystemProperties(flatMessageT, properties);
        systemPropertiesT.setStoreTimestamp(System.currentTimeMillis());
        systemPropertiesT.setStoreHost(storeHost);
        systemPropertiesT.setDeliveryAttempts(1);
        systemPropertiesT.setDeliveryTimestamp(deliveryTimestamp);
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

    public static MessageExt convertTo(FlatMessageExt flatMessage, String topicName, long invisibleTime, String host,
        int port) {
        MessageExt messageExt = new MessageExt();

        VirtualQueue virtualQueue = new VirtualQueue(flatMessage.message().topicId(), flatMessage.message().queueId());

        messageExt.setTopic(topicName);
        messageExt.setBrokerName(virtualQueue.brokerName());
        // The original queue id always is 0.
        messageExt.setQueueId(0);
        messageExt.setQueueOffset(flatMessage.offset());
        messageExt.setCommitLogOffset(flatMessage.offset());

        ByteBuffer payloadBuffer = flatMessage.message().payloadAsByteBuffer();

        // Convert buffer to byte array
        byte[] payload = new byte[payloadBuffer.remaining()];
        payloadBuffer.get(payloadBuffer.position(), payload);
        messageExt.setBody(payload);

        SystemProperties systemProperties = flatMessage.message().systemProperties();
        messageExt.setBornTimestamp(systemProperties.bornTimestamp());
        String bornHost = systemProperties.bornHost();
        if (bornHost != null) {
            messageExt.setBornHost(new InetSocketAddress(bornHost, 0));
        }
        messageExt.setStoreTimestamp(systemProperties.storeTimestamp());
        if (StringUtils.isNotBlank(host)) {
            messageExt.setStoreHost(new InetSocketAddress(host, port));
        }
        messageExt.setMsgId(systemProperties.messageId());

        // Re-consume times is the number of delivery attempts minus 1.
        messageExt.setReconsumeTimes(systemProperties.deliveryAttempts() - 1);

        if (systemProperties.deliveryTimestamp() > 0) {
            messageExt.setDeliverTimeMs(systemProperties.deliveryTimestamp());
        }

        KeyValue.Vector propertiesVector = flatMessage.message().userPropertiesVector();
        for (int i = 0; i < propertiesVector.length(); i++) {
            KeyValue keyValue = propertiesVector.get(i);
            messageExt.putUserProperty(keyValue.key(), keyValue.value());
        }

        fillSystemProperties(messageExt, flatMessage, invisibleTime);

        return messageExt;
    }

    @WithSpan(kind = SpanKind.SERVER)
    public static List<MessageExt> convertTo(ProxyContextExt context, List<FlatMessageExt> messageList,
        String topicName, long invisibleTime,
        String host, int port) {
        return messageList.stream()
            .map(messageExt -> convertTo(messageExt, topicName, invisibleTime, host, port))
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

        // set DLQ related properties
        String dlqOriginalTopicId = properties.remove(MessageConstants.PROPERTY_DLQ_ORIGIN_TOPIC_ID);
        if (!Strings.isNullOrEmpty(dlqOriginalTopicId)) {
            systemPropertiesT.setDlqOriginalTopicId(Long.parseLong(dlqOriginalTopicId));
        }
        String dlqOriginalMessageId = properties.remove(MessageConst.PROPERTY_DLQ_ORIGIN_MESSAGE_ID);
        if (!Strings.isNullOrEmpty(dlqOriginalMessageId)) {
            systemPropertiesT.setDlqOriginalMessageId(dlqOriginalMessageId);
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
        if (systemProperties.dlqOriginalTopicId() != 0) {
            MessageAccessor.putProperty(messageExt, MessageConstants.PROPERTY_DLQ_ORIGIN_TOPIC_ID, String.valueOf(systemProperties.dlqOriginalTopicId()));
        }
        // TODO: set MessageConstants#PROPERTY_DLQ_ORIGIN_TOPIC
        if (systemProperties.dlqOriginalMessageId() != null) {
            MessageAccessor.putProperty(messageExt, MessageConst.PROPERTY_DLQ_ORIGIN_MESSAGE_ID, String.valueOf(systemProperties.dlqOriginalMessageId()));
        }
    }
}
