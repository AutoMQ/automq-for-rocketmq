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

import apache.rocketmq.controller.v1.ConsumerGroup;
import apache.rocketmq.controller.v1.GroupType;
import apache.rocketmq.controller.v1.MessageType;
import apache.rocketmq.controller.v1.Topic;
import com.automq.rocketmq.metadata.api.ProxyMetadataService;
import java.util.concurrent.CompletableFuture;
import org.apache.rocketmq.common.attribute.TopicMessageType;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.service.metadata.MetadataService;
import org.apache.rocketmq.remoting.protocol.subscription.SubscriptionGroupConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provide resource metadata service, such as topic message type, subscription group config.
 */
public class ResourceMetadataService implements MetadataService {
    public static final Logger LOGGER = LoggerFactory.getLogger(ResourceMetadataService.class);
    private final ProxyMetadataService metadataService;

    public ResourceMetadataService(ProxyMetadataService service) {
        metadataService = service;
    }

    @Override
    public TopicMessageType getTopicMessageType(ProxyContext ctx, String topic) {
        CompletableFuture<Topic> topicFuture = metadataService.topicOf(topic);
        try {
            Topic topicObj = topicFuture.get();
            if (topicObj.getAcceptMessageTypesCount() == 1) {
                MessageType type = topicObj.getAcceptMessageTypes(0);
                switch (type) {
                    case NORMAL -> {
                        return TopicMessageType.NORMAL;
                    }
                    case FIFO -> {
                        return TopicMessageType.FIFO;
                    }
                    case DELAY -> {
                        return TopicMessageType.DELAY;
                    }
                    case TRANSACTION -> {
                        return TopicMessageType.TRANSACTION;
                    }
                    case MESSAGE_TYPE_UNSPECIFIED -> {
                        return TopicMessageType.UNSPECIFIED;
                    }
                }
            } else {
                LOGGER.warn("Topic {} has multiple message types, please specify only one accepted message type", topic);
            }
        } catch (Exception e) {
            LOGGER.error("Failed to get topic message type for {}", topic, e);
        }
        LOGGER.warn("Topic {} has no message type specified, use normal message type as default", topic);
        return TopicMessageType.NORMAL;
    }

    @Override
    public SubscriptionGroupConfig getSubscriptionGroupConfig(ProxyContext ctx, String group) {
        CompletableFuture<ConsumerGroup> groupFuture = metadataService.consumerGroupOf(group);
        try {
            ConsumerGroup consumerGroup = groupFuture.get();
            SubscriptionGroupConfig groupConfig = new SubscriptionGroupConfig();
            groupConfig.setGroupName(consumerGroup.getName());
            if (consumerGroup.getGroupType() == GroupType.GROUP_TYPE_FIFO) {
                groupConfig.setConsumeMessageOrderly(true);
            }
            groupConfig.setRetryMaxTimes(consumerGroup.getMaxDeliveryAttempt());
            // TODO: Support exponential retry policy
            return groupConfig;
        } catch (Exception e) {
            LOGGER.error("Failed to get subscription group config for {}", group, e);
        }
        return null;
    }
}
