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

package com.automq.rocketmq.proxy.service;

import apache.rocketmq.controller.v1.MessageType;
import apache.rocketmq.controller.v1.Topic;
import com.automq.rocketmq.common.config.BrokerConfig;
import com.automq.rocketmq.common.model.FlatMessageExt;
import com.automq.rocketmq.common.model.generated.KeyValue;
import com.automq.rocketmq.common.system.MessageConstants;
import com.automq.rocketmq.common.trace.TraceContext;
import com.automq.rocketmq.metadata.api.ProxyMetadataService;
import com.automq.rocketmq.store.api.DeadLetterSender;
import com.automq.stream.utils.ThreadUtils;
import io.opentelemetry.instrumentation.annotations.WithSpan;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.rocketmq.client.apis.ClientConfiguration;
import org.apache.rocketmq.client.apis.ClientServiceProvider;
import org.apache.rocketmq.client.apis.StaticSessionCredentialsProvider;
import org.apache.rocketmq.client.apis.message.Message;
import org.apache.rocketmq.client.apis.message.MessageBuilder;
import org.apache.rocketmq.client.apis.producer.Producer;
import org.apache.rocketmq.common.message.MessageConst;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DeadLetterService implements DeadLetterSender {
    private static final Logger LOGGER = LoggerFactory.getLogger(DeadLetterService.class);
    private final Producer producer;
    private final ClientServiceProvider provider;
    private final BrokerConfig brokerConfig;
    private final ProxyMetadataService metadataService;
    private final ExecutorService senderExecutor;

    // TODO: optimize producer related parameters
    public DeadLetterService(BrokerConfig brokerConfig, ProxyMetadataService metadataService) {
        this.brokerConfig = brokerConfig;
        this.metadataService = metadataService;
        this.provider = ClientServiceProvider.loadService();
        StaticSessionCredentialsProvider staticSessionCredentialsProvider =
            new StaticSessionCredentialsProvider(brokerConfig.getInnerAccessKey(), brokerConfig.getInnerSecretKey());

        ClientConfiguration clientConfiguration = ClientConfiguration.newBuilder()
            .setEndpoints(brokerConfig.advertiseAddress())
            .setCredentialProvider(staticSessionCredentialsProvider)
            .setRequestTimeout(Duration.ofSeconds(10))
            .build();

        try {
            this.producer = provider.newProducerBuilder()
                .setClientConfiguration(clientConfiguration)
                .build();
        } catch (Exception e) {
            LOGGER.error("Failed to create DLQ-Producer", e);
            throw new RuntimeException(e);
        }
        this.senderExecutor = Executors.newSingleThreadExecutor(
            ThreadUtils.createThreadFactory("dlq-sender", false));
    }

    @Override
    @WithSpan
    public CompletableFuture<Void> send(TraceContext context, long consumerGroupId, FlatMessageExt flatMessageExt) {
        CompletableFuture<Topic> dlqQueryCf = metadataService.consumerGroupOf(consumerGroupId)
            .thenComposeAsync(consumerGroup -> {
                long deadLetterTopicId = consumerGroup.getDeadLetterTopicId();
                long topicId = flatMessageExt.message().topicId();
                if (deadLetterTopicId == MessageConstants.UNINITIALIZED_TOPIC_ID) {
                    // not allow to send to DLQ
                    LOGGER.warn("Message: {} is dropped because the consumer group: {} doesn't have DLQ topic",
                        flatMessageExt, consumerGroupId);
                    return CompletableFuture.completedFuture(null);
                }
                if (deadLetterTopicId == topicId) {
                    LOGGER.error("Message: {} is dropped because the consumer group: {} has the same DLQ topic: {} with original topic",
                        flatMessageExt, consumerGroupId, topicId);
                    return CompletableFuture.completedFuture(null);
                }
                // get dlq topic info
                return metadataService.topicOf(deadLetterTopicId);
            }, senderExecutor);

        return dlqQueryCf.thenComposeAsync(dlqTopic -> {
            if (dlqTopic == null) {
                return CompletableFuture.completedFuture(null);
            }
            // verify dlq topic is valid
            if (!(dlqTopic.getAcceptTypes().getTypesList().contains(MessageType.NORMAL)
                || dlqTopic.getAcceptTypes().getTypesList().contains(MessageType.FIFO))) {
                LOGGER.error("Message: {} is dropped because the consumer group: {} has invalid DLQ topic: {}",
                    flatMessageExt, consumerGroupId, dlqTopic);
                return CompletableFuture.completedFuture(null);
            }

            // rebuild this message to DLQ message
            ByteBuffer payload = flatMessageExt.message().payloadAsByteBuffer();
            byte[] body;
            if (payload.hasArray()) {
                body = payload.array();
            } else {
                body = new byte[payload.remaining()];
                payload.get(body);
            }
            MessageBuilder messageBuilder = provider.newMessageBuilder()
                .setTopic(dlqTopic.getName())
                .setBody(body);
            Message dlqMsg = buildDLQMessage(messageBuilder, flatMessageExt);
            return getProducer().sendAsync(dlqMsg)
                .thenAcceptAsync(sendReceipt -> {
                    if (sendReceipt == null) {
                        return;
                    }
                    LOGGER.info("Message: {} is sent to DLQ topic: {}, receipt: {}",
                        flatMessageExt, dlqTopic, sendReceipt);
                }, senderExecutor);
        }, senderExecutor);

    }

    private Message buildDLQMessage(MessageBuilder messageBuilder, FlatMessageExt flatMessage) {
        if (flatMessage.message().tag() != null) {
            messageBuilder.setTag(flatMessage.message().tag());
        }
        if (flatMessage.message().keys() != null) {
            String[] keys = flatMessage.message().keys().split(" ");
            messageBuilder.setKeys(keys);
        }
        if (flatMessage.message().messageGroup() != null) {
            messageBuilder.setMessageGroup(flatMessage.message().messageGroup());
        }
        messageBuilder.addProperty(MessageConst.PROPERTY_DLQ_ORIGIN_MESSAGE_ID, flatMessage.message().systemProperties().messageId());
        messageBuilder.addProperty(MessageConstants.PROPERTY_DLQ_ORIGIN_TOPIC_ID, String.valueOf(flatMessage.message().topicId()));

        // don't set message id because we expect a new message id

        // fill user properties
        KeyValue.Vector propertiesVector = flatMessage.message().userPropertiesVector();
        for (int i = 0; i < propertiesVector.length(); i++) {
            KeyValue keyValue = propertiesVector.get(i);
            messageBuilder.addProperty(keyValue.key(), keyValue.value());
        }
        return messageBuilder.build();
    }

    public Producer getProducer() {
        return producer;
    }
}
