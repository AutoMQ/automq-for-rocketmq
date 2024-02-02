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
            if (topicObj.getAcceptTypes().getTypesList().size() == 1) {
                MessageType type = topicObj.getAcceptTypes().getTypesList().get(0);
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
        LOGGER.warn("Topic {} has no message type specified, use unspecified message type as default", topic);
        return TopicMessageType.UNSPECIFIED;
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
