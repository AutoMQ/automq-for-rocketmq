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

import apache.rocketmq.common.v1.Code;
import apache.rocketmq.controller.v1.MessageQueueAssignment;
import apache.rocketmq.controller.v1.MessageType;
import apache.rocketmq.controller.v1.Topic;
import com.automq.rocketmq.common.config.BrokerConfig;
import com.automq.rocketmq.common.model.generated.FlatMessage;
import com.automq.rocketmq.common.system.MessageConstants;
import com.automq.rocketmq.common.trace.TraceContext;
import com.automq.rocketmq.metadata.api.ProxyMetadataService;
import com.automq.rocketmq.proxy.grpc.ProxyClient;
import com.automq.rocketmq.store.api.DeadLetterSender;
import com.automq.rocketmq.store.api.MessageStore;
import com.automq.rocketmq.store.model.StoreContext;
import com.automq.rocketmq.store.model.message.PutResult;
import com.automq.stream.utils.ThreadUtils;
import io.opentelemetry.instrumentation.annotations.WithSpan;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DeadLetterService implements DeadLetterSender {
    private static final Logger LOGGER = LoggerFactory.getLogger(DeadLetterService.class);
    private final ProxyClient relayClient;
    private final BrokerConfig brokerConfig;
    private final ProxyMetadataService metadataService;
    private final ExecutorService senderExecutor;
    private MessageStore messageStore;

    public DeadLetterService(BrokerConfig brokerConfig, ProxyMetadataService metadataService, ProxyClient relayClient) {
        this.brokerConfig = brokerConfig;
        this.metadataService = metadataService;
        this.relayClient = relayClient;
        this.senderExecutor = Executors.newSingleThreadExecutor(
            ThreadUtils.createThreadFactory("dlq-sender", false));
    }

    public void init(MessageStore messageStore) {
        this.messageStore = messageStore;
    }

    @Override
    @WithSpan
    public CompletableFuture<Void> send(TraceContext context, long consumerGroupId, FlatMessage message) {
        if (messageStore == null) {
            return CompletableFuture.failedFuture(new IllegalStateException("Message store is not initialized"));
        }

        CompletableFuture<Topic> dlqQueryCf = metadataService.consumerGroupOf(consumerGroupId)
            .thenComposeAsync(consumerGroup -> {
                long deadLetterTopicId = consumerGroup.getDeadLetterTopicId();
                long topicId = message.topicId();
                if (deadLetterTopicId == MessageConstants.UNINITIALIZED_TOPIC_ID) {
                    // not allow to send to DLQ
                    LOGGER.warn("Message: {} is dropped because the consumer group: {} doesn't have DLQ topic",
                        message.systemProperties().messageId(), consumerGroupId);
                    return CompletableFuture.completedFuture(null);
                }
                if (deadLetterTopicId == topicId) {
                    LOGGER.error("Message: {} is dropped because the consumer group: {} has the same DLQ topic: {} with original topic",
                        message.systemProperties().messageId(), consumerGroupId, topicId);
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
                    message.systemProperties().messageId(), consumerGroupId, dlqTopic);
                return CompletableFuture.completedFuture(null);
            }

            message.mutateTopicId(dlqTopic.getTopicId());

            List<MessageQueueAssignment> assignmentsList = new ArrayList<>(dlqTopic.getAssignmentsList());
            if (assignmentsList.isEmpty()) {
                LOGGER.error("Message: {} is dropped because the consumer group: {} has empty DLQ topic: {}",
                    message.systemProperties().messageId(), consumerGroupId, dlqTopic);
                return CompletableFuture.completedFuture(null);
            }

            Collections.shuffle(assignmentsList);
            Optional<MessageQueueAssignment> optional = assignmentsList.stream()
                .filter(assignment -> assignment.getNodeId() == brokerConfig.nodeId())
                .findFirst();

            // Send to local DLQ
            if (optional.isPresent()) {
                MessageQueueAssignment assignment = optional.get();
                message.mutateQueueId(assignment.getQueue().getQueueId());
                return messageStore.put(StoreContext.EMPTY, message)
                    .thenAccept(result -> {
                        if (result.status() != PutResult.Status.PUT_OK) {
                            LOGGER.error("Message: {} is dropped because the consumer group: {} failed to send to DLQ topic: {}, code: {}",
                                message.systemProperties().messageId(), consumerGroupId, dlqTopic, result.status());
                        }
                    });
            }

            // Send to remote DLQ
            MessageQueueAssignment assignment = assignmentsList.get(0);
            message.mutateQueueId(assignment.getQueue().getQueueId());
            return metadataService.addressOf(assignment.getNodeId())
                .thenCompose(address -> relayClient.relayMessage(address, message))
                .thenAccept(status -> {
                    if (status.getCode() != Code.OK) {
                        LOGGER.error("Message: {} is dropped because the consumer group: {} failed to send to DLQ topic: {}, code: {}, reason: {}",
                            message.systemProperties().messageId(), consumerGroupId, dlqTopic, status.getCode(), status.getMessage());
                    }
                });
        }, senderExecutor);
    }
}
