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

import apache.rocketmq.controller.v1.Code;
import apache.rocketmq.controller.v1.ConsumerGroup;
import apache.rocketmq.controller.v1.SubscriptionMode;
import apache.rocketmq.controller.v1.Topic;
import com.automq.rocketmq.common.config.ProxyConfig;
import com.automq.rocketmq.common.exception.ControllerException;
import com.automq.rocketmq.common.model.FlatMessageExt;
import com.automq.rocketmq.common.model.generated.FlatMessage;
import com.automq.rocketmq.common.util.CommonUtil;
import com.automq.rocketmq.metadata.api.ProxyMetadataService;
import com.automq.rocketmq.proxy.exception.ProxyException;
import com.automq.rocketmq.proxy.metrics.ProxyMetricsManager;
import com.automq.rocketmq.proxy.model.VirtualQueue;
import com.automq.rocketmq.proxy.util.FlatMessageUtil;
import com.automq.rocketmq.proxy.util.ReceiptHandleUtil;
import com.automq.rocketmq.store.api.DeadLetterSender;
import com.automq.rocketmq.store.api.MessageStore;
import com.automq.rocketmq.store.model.message.Filter;
import com.automq.rocketmq.store.model.message.PutResult;
import com.automq.rocketmq.store.model.message.ResetConsumeOffsetResult;
import com.automq.rocketmq.store.model.message.SQLFilter;
import com.automq.rocketmq.store.model.message.TagFilter;
import io.opentelemetry.instrumentation.annotations.WithSpan;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.rocketmq.client.consumer.AckResult;
import org.apache.rocketmq.client.consumer.AckStatus;
import org.apache.rocketmq.client.consumer.PopResult;
import org.apache.rocketmq.client.consumer.PopStatus;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.client.consumer.PullStatus;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.attribute.TopicMessageType;
import org.apache.rocketmq.common.consumer.ReceiptHandle;
import org.apache.rocketmq.common.filter.ExpressionType;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageClientIDSetter;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.common.utils.ExceptionUtils;
import org.apache.rocketmq.proxy.service.message.MessageService;
import org.apache.rocketmq.proxy.service.route.AddressableMessageQueue;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.apache.rocketmq.remoting.protocol.body.LockBatchRequestBody;
import org.apache.rocketmq.remoting.protocol.body.UnlockBatchRequestBody;
import org.apache.rocketmq.remoting.protocol.header.AckMessageRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.ChangeInvisibleTimeRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.ConsumerSendMsgBackRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.EndTransactionRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.GetMaxOffsetRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.GetMinOffsetRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.PopMessageRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.PullMessageRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.QueryConsumerOffsetRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.SendMessageRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.UpdateConsumerOffsetRequestHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MessageServiceImpl implements MessageService {
    private static final Logger LOGGER = LoggerFactory.getLogger(MessageServiceImpl.class);
    private final ProxyConfig config;
    private final ProxyMetadataService metadataService;
    private final MessageStore store;
    private final LockService lockService;
    private final DeadLetterSender deadLetterService;
    private final SuspendRequestService suspendRequestService;

    public MessageServiceImpl(ProxyConfig config, MessageStore store, ProxyMetadataService metadataService,
        LockService lockService, DeadLetterSender deadLetterService) {
        this.config = config;
        this.store = store;
        this.metadataService = metadataService;
        this.deadLetterService = deadLetterService;
        this.lockService = lockService;
        this.suspendRequestService = SuspendRequestService.getInstance();
    }

    public TopicMessageType getMessageType(SendMessageRequestHeader requestHeader) {
        Map<String, String> properties = MessageDecoder.string2messageProperties(requestHeader.getProperties());
        String traFlag = properties.get(MessageConst.PROPERTY_TRANSACTION_PREPARED);
        TopicMessageType topicMessageType = TopicMessageType.NORMAL;
        if (Boolean.parseBoolean(traFlag)) {
            topicMessageType = TopicMessageType.TRANSACTION;
        } else if (properties.containsKey(MessageConst.PROPERTY_SHARDING_KEY)) {
            topicMessageType = TopicMessageType.FIFO;
        } else if (properties.get("__STARTDELIVERTIME") != null
            || properties.get(MessageConst.PROPERTY_DELAY_TIME_LEVEL) != null
            || properties.get(MessageConst.PROPERTY_TIMER_DELIVER_MS) != null
            || properties.get(MessageConst.PROPERTY_TIMER_DELAY_SEC) != null) {
            topicMessageType = TopicMessageType.DELAY;
        }
        return topicMessageType;
    }

    @Override
    public CompletableFuture<List<SendResult>> sendMessage(ProxyContext ctx, AddressableMessageQueue messageQueue,
        List<Message> msgList, SendMessageRequestHeader requestHeader, long timeoutMillis) {
        if (msgList.size() != 1) {
            throw new UnsupportedOperationException("Batch message is not supported");
        }
        Message message = msgList.get(0);
        String messageId = MessageClientIDSetter.getUniqID(message);
        VirtualQueue virtualQueue = new VirtualQueue(messageQueue);

        CompletableFuture<Topic> topicFuture = topicOf(requestHeader.getTopic());

        CompletableFuture<PutResult> putFuture = topicFuture.thenCompose(topic -> {
            if (topic.getTopicId() != virtualQueue.topicId()) {
                LOGGER.error("Topic id in request header {} does not match topic id in message queue {}, maybe the topic is recreated.",
                    topic.getTopicId(), virtualQueue.topicId());
                return CompletableFuture.failedFuture(new ProxyException(apache.rocketmq.v2.Code.TOPIC_NOT_FOUND, "Topic resource does not exist."));
            }
            FlatMessage flatMessage = FlatMessageUtil.convertTo(topic.getTopicId(), virtualQueue.physicalQueueId(), config.hostName(), message);

            if (requestHeader.getTopic().startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)) {
                flatMessage.systemProperties().mutateDeliveryAttempts(requestHeader.getReconsumeTimes() + 1);
                if (requestHeader.getReconsumeTimes() > requestHeader.getMaxReconsumeTimes()) {
                    String groupName = requestHeader.getTopic().replace(MixAll.RETRY_GROUP_TOPIC_PREFIX, "");
                    FlatMessageExt flatMessageExt = FlatMessageExt.Builder.builder()
                        .message(flatMessage)
                        .offset(0)
                        .build();
                    return consumerGroupOf(groupName)
                        .thenCompose(group -> deadLetterService.send(group.getGroupId(), flatMessageExt))
                        .thenApply(ignore -> new PutResult(PutResult.Status.PUT_OK, 0));
                } else {
                    return store.put(flatMessage);
                }
            }

            return store.put(flatMessage);
        });

        return putFuture.thenApply(putResult -> {
            // Wakeup the suspended pop request if there is any message arrival.
            suspendRequestService.notifyMessageArrival(requestHeader.getTopic(), virtualQueue.physicalQueueId(), message.getTags());

            ProxyMetricsManager.recordIncomingMessages(requestHeader.getTopic(), getMessageType(requestHeader), 1, message.getBody().length);

            SendResult result = new SendResult();
            result.setSendStatus(SendStatus.SEND_OK);
            result.setMsgId(messageId);
            result.setMessageQueue(new MessageQueue(messageQueue.getMessageQueue()));
            result.setQueueOffset(putResult.offset());
            return Collections.singletonList(result);
        });
    }

    @Override
    public CompletableFuture<RemotingCommand> sendMessageBack(ProxyContext ctx, ReceiptHandle handle, String messageId,
        ConsumerSendMsgBackRequestHeader requestHeader, long timeoutMillis) {
        // Build the response.
        final RemotingCommand response = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, null, null);

        Integer delayLevel = requestHeader.getDelayLevel();
        if (Objects.isNull(delayLevel)) {
            response.setCode(ResponseCode.ILLEGAL_OPERATION);
            response.setRemark("argument delay level is null");
            return CompletableFuture.completedFuture(response);
        }

        Long offset = requestHeader.getOffset();
        if (Objects.isNull(offset)) {
            response.setCode(ResponseCode.ILLEGAL_OPERATION);
            response.setRemark("argument offset is null");
            return CompletableFuture.completedFuture(response);
        }

        VirtualQueue virtualQueue = new VirtualQueue(requestHeader.getBname());

        CompletableFuture<Topic> topicFuture = topicOf(requestHeader.getOriginTopic());
        CompletableFuture<ConsumerGroup> groupFuture = consumerGroupOf(requestHeader.getGroup());
        return topicFuture.thenCombine(groupFuture, (topic, group) -> {
            if (topic.getTopicId() != virtualQueue.topicId()) {
                LOGGER.error("Topic id in request header {} does not match topic id in message queue {}, maybe the topic is recreated.",
                    topic.getTopicId(), virtualQueue.topicId());
                throw new ProxyException(apache.rocketmq.v2.Code.TOPIC_NOT_FOUND, "Topic resource does not exist.");
            }
            return Pair.of(topic, group);
        }).thenCompose(pair -> {
            Topic topic = pair.getLeft();
            ConsumerGroup group = pair.getRight();

            return store.pull(group.getGroupId(), topic.getTopicId(), virtualQueue.physicalQueueId(),
                    Filter.DEFAULT_FILTER, requestHeader.getOffset(), 1, false)
                .thenApply(pullResult -> {
                    if (pullResult.status() == com.automq.rocketmq.store.model.message.PullResult.Status.FOUND) {
                        return pullResult.messageList().get(0);
                    }
                    throw new ProxyException(apache.rocketmq.v2.Code.MESSAGE_NOT_FOUND, "Message not found from server.");
                }).thenCompose(messageExt -> {
                    if (messageExt.deliveryAttempts() > group.getMaxDeliveryAttempt()) {
                        return deadLetterService.send(group.getGroupId(), messageExt);
                    }

                    // Message consume retry strategy
                    // <0: no retry,put into DLQ directly
                    // =0: broker control retry frequency
                    // >0: client control retry frequency
                    return switch (Integer.compare(delayLevel, 0)) {
                        case -1 -> deadLetterService.send(group.getGroupId(), messageExt);
                        case 0 -> topicOf(MixAll.RETRY_GROUP_TOPIC_PREFIX + requestHeader.getGroup())
                            .thenCompose(retryTopic -> {
                                // Keep the same logic as apache RocketMQ.
                                int serverDelayLevel = messageExt.deliveryAttempts() + 1;
                                messageExt.setDeliveryAttempts(serverDelayLevel);
                                messageExt.setOriginalQueueOffset(messageExt.originalOffset());

                                FlatMessage message = messageExt.message();
                                message.mutateTopicId(retryTopic.getTopicId());

                                message.systemProperties().mutateDeliveryTimestamp(FlatMessageUtil.calculateDeliveryTimestamp(serverDelayLevel));
                                return store.put(message)
                                    .exceptionally(ex -> {
                                        LOGGER.error("Put messageExt to retry topic failed", ex);
                                        return null;
                                    })
                                    .thenApply(ignore -> null);
                            });
                        case 1 -> topicOf(MixAll.RETRY_GROUP_TOPIC_PREFIX + requestHeader.getGroup())
                            .thenCompose(retryTopic -> {
                                messageExt.setDeliveryAttempts(messageExt.deliveryAttempts() + 1);
                                messageExt.setOriginalQueueOffset(messageExt.originalOffset());

                                FlatMessage message = messageExt.message();
                                message.mutateTopicId(retryTopic.getTopicId());

                                message.systemProperties().mutateDeliveryTimestamp(FlatMessageUtil.calculateDeliveryTimestamp(delayLevel));
                                return store.put(message)
                                    .exceptionally(ex -> {
                                        LOGGER.error("Put message to retry topic failed", ex);
                                        return null;
                                    })
                                    .thenApply(ignore -> null);
                            });
                        default -> throw new IllegalStateException("Never reach here");
                    };
                });
        }).whenComplete((nil, throwable) -> {
            if (throwable != null) {
                response.setCode(ResponseCode.SYSTEM_ERROR);
                response.setRemark(throwable.getMessage());
            }
        }).thenApply(nil -> response);
    }

    @Override
    public CompletableFuture<Void> endTransactionOneway(ProxyContext ctx, String brokerName,
        EndTransactionRequestHeader requestHeader, long timeoutMillis) {
        throw new UnsupportedOperationException();
    }

    record InnerPopResult(
        long restMessageCount,
        List<FlatMessageExt> messageList
    ) implements SuspendRequestService.GetMessageResult {
        @Override
        public boolean needWriteResponse() {
            return restMessageCount > 0 || !messageList.isEmpty();
        }
    }

    @WithSpan
    private CompletableFuture<InnerPopResult> popSpecifiedQueueUnsafe(ProxyContext context, ConsumerGroup consumerGroup,
        Topic topic,
        int queueId, Filter filter, int batchSize, boolean fifo, long invisibleDuration) {
        List<FlatMessageExt> messageList = new ArrayList<>();
        long consumerGroupId = consumerGroup.getGroupId();
        long topicId = topic.getTopicId();

        // Decide whether to pop the retry-messages first
        boolean retryPriority = !fifo && CommonUtil.applyPercentage(config.retryPriorityPercentage());

        CompletableFuture<InnerPopResult> popFuture = store.pop(consumerGroupId, topicId, queueId, filter, batchSize, fifo, retryPriority, invisibleDuration)
            .thenApply(firstResult -> {
                List<FlatMessageExt> resultList = firstResult.messageList();
                messageList.addAll(resultList);
                Integer totalSize = resultList.stream()
                    .map(message -> message.message().payloadAsByteBuffer().remaining())
                    .reduce(0, Integer::sum);
                ProxyMetricsManager.recordOutgoingMessages(topic.getName(), consumerGroup.getName(), resultList.size(), totalSize, retryPriority);
                return new InnerPopResult(firstResult.restMessageCount(), messageList);
            });

        // There is no retry message when pop orderly. So that return origin messages directly.
        if (fifo) {
            return popFuture;
        }

        return popFuture.thenCompose(firstPopResult -> {
            int firstPopMessageCount = firstPopResult.messageList.size();
            if (firstPopMessageCount < batchSize) {
                return store.pop(consumerGroupId, topicId, queueId, filter, batchSize - firstPopMessageCount, false, !retryPriority, invisibleDuration)
                    .thenApply(secondResult -> {
                        List<FlatMessageExt> secondPopMessageList = secondResult.messageList();
                        messageList.addAll(secondPopMessageList);
                        Integer totalSize = secondPopMessageList.stream()
                            .map(message -> message.message().payloadAsByteBuffer().remaining())
                            .reduce(0, Integer::sum);
                        ProxyMetricsManager.recordOutgoingMessages(topic.getName(), consumerGroup.getName(), secondPopMessageList.size(), totalSize, !retryPriority);

                        return new InnerPopResult(firstPopResult.restMessageCount + secondResult.restMessageCount(), messageList);
                    });
            }
            return CompletableFuture.completedFuture(firstPopResult);
        });
    }

    @WithSpan
    private CompletableFuture<InnerPopResult> popSpecifiedQueue(ProxyContext context, ConsumerGroup consumerGroup,
        String clientId,
        Topic topic, int queueId, Filter filter, int batchSize, boolean fifo, long invisibleDuration,
        long timeoutMillis) {
        long topicId = topic.getTopicId();
        if (lockService.tryLock(topicId, queueId, clientId, fifo, false)) {
            return popSpecifiedQueueUnsafe(context, consumerGroup, topic, queueId, filter, batchSize, fifo, invisibleDuration)
                .orTimeout(timeoutMillis, TimeUnit.MILLISECONDS)
                .whenComplete((v, throwable) -> {
                    if (throwable != null) {
                        LOGGER.error("Error while pop message from topic: {}, queue: {}, batch size: {}.", topic.getName(), queueId, batchSize, throwable);
                    }

                    // Release lock since complete or timeout.
                    lockService.release(topicId, queueId);
                });
        }
        return CompletableFuture.completedFuture(new InnerPopResult(0, Collections.emptyList()));
    }

    @Override
    @WithSpan
    public CompletableFuture<PopResult> popMessage(ProxyContext ctx, AddressableMessageQueue messageQueue,
        PopMessageRequestHeader requestHeader, long timeoutMillis) {
        CompletableFuture<Topic> topicFuture = topicOf(requestHeader.getTopic());
        CompletableFuture<ConsumerGroup> groupFuture = consumerGroupOf(requestHeader.getConsumerGroup());

        // Build the virtual queue
        VirtualQueue virtualQueue = new VirtualQueue(messageQueue);
        String clientId = ctx.getClientID();

        Filter filter;
        if (StringUtils.isNotBlank(requestHeader.getExpType())) {
            filter = switch (requestHeader.getExpType()) {
                case ExpressionType.TAG ->
                    requestHeader.getExp().contains(TagFilter.SUB_ALL) ? Filter.DEFAULT_FILTER : new TagFilter(requestHeader.getExp());
                case ExpressionType.SQL92 -> new SQLFilter(requestHeader.getExp());
                default -> Filter.DEFAULT_FILTER;
            };
        } else {
            filter = Filter.DEFAULT_FILTER;
        }

        AtomicReference<Topic> topicReference = new AtomicReference<>();
        AtomicReference<ConsumerGroup> consumerGroupReference = new AtomicReference<>();
        CompletableFuture<InnerPopResult> popMessageFuture = topicFuture.thenCombine(groupFuture, (topic, group) -> {
            if (topic.getTopicId() != virtualQueue.topicId()) {
                LOGGER.error("Topic id in request header {} does not match topic id in message queue {}, maybe the topic is recreated.",
                    topic.getTopicId(), virtualQueue.topicId());
                throw new ProxyException(apache.rocketmq.v2.Code.TOPIC_NOT_FOUND, "Topic resource does not exist.");
            }

            if (group.getSubMode() != SubscriptionMode.SUB_MODE_POP) {
                throw new ProxyException(apache.rocketmq.v2.Code.FORBIDDEN, String.format("The consumer group [%s] is not allowed to consume message with pop mode.", group.getName()));
            }

            topicReference.set(topic);
            consumerGroupReference.set(group);
            return null;
        }).thenCompose(nil -> popSpecifiedQueue(ctx, consumerGroupReference.get(), clientId, topicReference.get(), virtualQueue.physicalQueueId(), filter,
            requestHeader.getMaxMsgNums(), requestHeader.isOrder(), requestHeader.getInvisibleTime(), timeoutMillis));

        return popMessageFuture.thenCompose(result -> {
            if (result.messageList.isEmpty()) {
                if (result.restMessageCount > 0) {
                    // This means there are messages in the queue but not match the filter. So we should prevent long polling.
                    return CompletableFuture.completedFuture(new PopResult(PopStatus.NO_NEW_MSG, Collections.emptyList()));
                } else {
                    return suspendRequestService.suspendRequest(ctx, requestHeader.getTopic(), virtualQueue.physicalQueueId(), filter, timeoutMillis,
                            // Function to pop message later.
                            timeout -> popSpecifiedQueue(ctx, consumerGroupReference.get(), clientId, topicReference.get(), virtualQueue.physicalQueueId(), filter,
                                requestHeader.getMaxMsgNums(), requestHeader.isOrder(), requestHeader.getInvisibleTime(), timeout))
                        .thenApply(suspendResult -> {
                            if (suspendResult.isEmpty() || suspendResult.get().messageList().isEmpty()) {
                                return new PopResult(PopStatus.POLLING_NOT_FOUND, Collections.emptyList());
                            }
                            return new PopResult(PopStatus.FOUND, FlatMessageUtil.convertTo(suspendResult.get().messageList(), requestHeader.getTopic(), requestHeader.getInvisibleTime(), config.hostName(), config.grpcListenPort()));
                        });
                }
            }
            return CompletableFuture.completedFuture(new PopResult(PopStatus.FOUND, FlatMessageUtil.convertTo(result.messageList, requestHeader.getTopic(), requestHeader.getInvisibleTime(), config.hostName(), config.grpcListenPort())));
        });
    }

    @Override
    public CompletableFuture<AckResult> changeInvisibleTime(ProxyContext ctx, ReceiptHandle handle, String messageId,
        ChangeInvisibleTimeRequestHeader requestHeader, long timeoutMillis) {

        // The real receipt handle generated by S3RocketMQ.
        String rawHandle;
        try {
            rawHandle = ReceiptHandleUtil.decodeReceiptHandle(requestHeader.getExtraInfo());
        } catch (Exception e) {
            org.apache.rocketmq.client.consumer.AckResult result = new org.apache.rocketmq.client.consumer.AckResult();
            result.setStatus(AckStatus.NO_EXIST);
            return CompletableFuture.completedFuture(result);
        }

        return store.changeInvisibleDuration(rawHandle, requestHeader.getInvisibleTime())
            .thenApply(changeInvisibleDurationResult -> {
                org.apache.rocketmq.client.consumer.AckResult ackResult = new org.apache.rocketmq.client.consumer.AckResult();
                switch (changeInvisibleDurationResult.status()) {
                    case SUCCESS -> ackResult.setStatus(AckStatus.OK);
                    case ERROR -> ackResult.setStatus(AckStatus.NO_EXIST);
                }
                return ackResult;
            });
    }

    @Override
    public CompletableFuture<AckResult> ackMessage(ProxyContext ctx, ReceiptHandle handle, String messageId,
        AckMessageRequestHeader requestHeader, long timeoutMillis) {
        Integer queueId = requestHeader.getQueueId();

        // The real receipt handle generated by S3RocketMQ.
        String rawHandle;
        try {
            rawHandle = ReceiptHandleUtil.decodeReceiptHandle(requestHeader.getExtraInfo());
        } catch (Exception e) {
            org.apache.rocketmq.client.consumer.AckResult result = new org.apache.rocketmq.client.consumer.AckResult();
            result.setStatus(AckStatus.NO_EXIST);
            return CompletableFuture.completedFuture(result);
        }

        CompletableFuture<Topic> topicFuture = topicOf(requestHeader.getTopic());
        CompletableFuture<ConsumerGroup> groupFuture = consumerGroupOf(requestHeader.getConsumerGroup());

        CompletableFuture<AckResult> resultF = store.ack(rawHandle)
            .thenApply(ackResult -> {
                    org.apache.rocketmq.client.consumer.AckResult result = new org.apache.rocketmq.client.consumer.AckResult();
                    switch (ackResult.status()) {
                        case SUCCESS -> result.setStatus(AckStatus.OK);
                        case ERROR -> result.setStatus(AckStatus.NO_EXIST);
                    }
                    return result;
                }
            );

        CompletableFuture<Pair<Topic, ConsumerGroup>> resourceF = topicFuture.thenCombine(groupFuture, Pair::of);

        resultF.thenCombine(resourceF, (ackResult, pair) -> {
            long groupId = pair.getRight().getGroupId();
            long topicId = pair.getLeft().getTopicId();
            return store.getInflightStats(groupId, topicId, queueId).thenAccept(inflight -> {
                // Expire the lock later to allow other client preempted when all inflight messages are acked.
                if (inflight == 0) {
                    lockService.tryExpire(topicId, queueId, Duration.ofSeconds(1).toMillis());
                }
            });
        });

        return resultF;
    }

    @Override
    public CompletableFuture<Long> queryConsumerOffset(ProxyContext ctx, AddressableMessageQueue messageQueue,
        QueryConsumerOffsetRequestHeader requestHeader, long timeoutMillis) {
        CompletableFuture<ConsumerGroup> consumeGroupFuture = metadataService.consumerGroupOf(requestHeader.getConsumerGroup());
        CompletableFuture<Topic> topicFuture = metadataService.topicOf(requestHeader.getTopic());
        VirtualQueue virtualQueue = new VirtualQueue(messageQueue);

        // TODO：Implement the bellow logic in the next iteration.
        // If the consumer doesn't have offset record on the specified queue:
        // * If the consumer never consume any message of the topic, return -1, means QUERY_NOT_FOUND.
        // * If the consumer has consumed some messages of the topic, return the MIN_OFFSET of the specified queue.
        return consumeGroupFuture.thenCombine(topicFuture, Pair::of)
            .thenCompose(pair -> {
                ConsumerGroup consumerGroup = pair.getLeft();
                Topic topic = pair.getRight();
                if (consumerGroup.getSubMode() == SubscriptionMode.SUB_MODE_PULL) {
                    return metadataService.consumerOffsetOf(
                        pair.getLeft().getGroupId(),
                        pair.getRight().getTopicId(),
                        virtualQueue.physicalQueueId());
                }
                return store.getConsumeOffset(consumerGroup.getGroupId(), topic.getTopicId(), virtualQueue.physicalQueueId());
            });
    }

    @Override
    public CompletableFuture<Void> updateConsumerOffset(ProxyContext ctx, AddressableMessageQueue messageQueue,
        UpdateConsumerOffsetRequestHeader requestHeader, long timeoutMillis) {
        CompletableFuture<ConsumerGroup> consumeGroupFuture = metadataService.consumerGroupOf(requestHeader.getConsumerGroup());
        CompletableFuture<Topic> topicFuture = metadataService.topicOf(requestHeader.getTopic());
        VirtualQueue virtualQueue = new VirtualQueue(messageQueue);

        return consumeGroupFuture.thenCombine(topicFuture, Pair::of)
            .thenCompose(pair -> {
                ConsumerGroup consumerGroup = pair.getLeft();
                Topic topic = pair.getRight();
                if (consumerGroup.getSubMode() == SubscriptionMode.SUB_MODE_PULL) {
                    return metadataService.updateConsumerOffset(
                            consumerGroup.getGroupId(),
                            topic.getTopicId(),
                            virtualQueue.physicalQueueId(),
                            requestHeader.getCommitOffset())
                        .thenApply(nil -> new ResetConsumeOffsetResult(ResetConsumeOffsetResult.Status.SUCCESS));
                }
                return store.resetConsumeOffset(consumerGroup.getGroupId(), topic.getTopicId(), virtualQueue.physicalQueueId(), requestHeader.getCommitOffset());
            }).thenAccept(resetConsumeOffsetResult -> {
                if (resetConsumeOffsetResult.status() != ResetConsumeOffsetResult.Status.SUCCESS) {
                    throw new ProxyException(apache.rocketmq.v2.Code.INTERNAL_ERROR, "Reset consume offset failed");
                }
            });
    }

    record PullResultWrapper(
        com.automq.rocketmq.store.model.message.PullResult inner
    ) implements SuspendRequestService.GetMessageResult {
        @Override
        public boolean needWriteResponse() {
            return inner.maxOffset() > inner.nextBeginOffset() || !inner.messageList().isEmpty();
        }
    }

    @Override
    public CompletableFuture<PullResult> pullMessage(ProxyContext ctx, AddressableMessageQueue messageQueue,
        PullMessageRequestHeader requestHeader, long timeoutMillis) {
        VirtualQueue virtualQueue = new VirtualQueue(messageQueue);
        CompletableFuture<Topic> topicFuture = metadataService.topicOf(requestHeader.getTopic());
        CompletableFuture<ConsumerGroup> groupFuture = metadataService.consumerGroupOf(requestHeader.getConsumerGroup());

        AtomicReference<Topic> topicReference = new AtomicReference<>();
        AtomicReference<ConsumerGroup> consumerGroupReference = new AtomicReference<>();

        Filter filter;
        if (StringUtils.isNotBlank(requestHeader.getExpressionType())) {
            filter = switch (requestHeader.getExpressionType()) {
                case ExpressionType.TAG ->
                    requestHeader.getSubscription().contains(TagFilter.SUB_ALL) ? Filter.DEFAULT_FILTER : new TagFilter(requestHeader.getSubscription());
                case ExpressionType.SQL92 -> new SQLFilter(requestHeader.getSubscription());
                default -> Filter.DEFAULT_FILTER;
            };
        } else {
            filter = Filter.DEFAULT_FILTER;
        }

        return topicFuture.thenCombine(groupFuture, Pair::of)
            .thenCompose(pair -> {
                Topic topic = pair.getLeft();
                ConsumerGroup group = pair.getRight();
                if (topic.getTopicId() != virtualQueue.topicId()) {
                    LOGGER.error("Topic id in request header {} does not match topic id in message queue {}, maybe the topic is recreated.",
                        topic.getTopicId(), virtualQueue.topicId());
                    throw new ProxyException(apache.rocketmq.v2.Code.TOPIC_NOT_FOUND, "Topic resource does not exist.");
                }

                if (group.getSubMode() != SubscriptionMode.SUB_MODE_PULL) {
                    throw new ProxyException(apache.rocketmq.v2.Code.FORBIDDEN, String.format("The consumer group [%s] is not allowed to consume message with pull mode.", group.getName()));
                }

                topicReference.set(topic);
                consumerGroupReference.set(group);

                // Update the consumer offset if the commit offset is specified.
                if (requestHeader.getCommitOffset() != null && requestHeader.getCommitOffset() >= 0) {
                    metadataService.updateConsumerOffset(group.getGroupId(), topic.getTopicId(), virtualQueue.physicalQueueId(), requestHeader.getCommitOffset());
                }

                return store.pull(group.getGroupId(), topic.getTopicId(), virtualQueue.physicalQueueId(), filter, requestHeader.getQueueOffset(), requestHeader.getMaxMsgNums(), false);
            })
            .thenCompose(result -> {
                if (result.messageList().isEmpty()) {
                    if (result.maxOffset() - result.nextBeginOffset() > 0) {
                        // This means there are messages in the queue but not match the filter. So we should prevent long polling.
                        return CompletableFuture.completedFuture(new PullResult(PullStatus.NO_MATCHED_MSG, result.nextBeginOffset(), result.minOffset(), result.maxOffset(), Collections.emptyList()));
                    } else {
                        return suspendRequestService.suspendRequest(ctx, requestHeader.getTopic(), virtualQueue.physicalQueueId(), filter, timeoutMillis,
                                // Function to pull message later.
                                timeout -> store.pull(consumerGroupReference.get().getGroupId(), topicReference.get().getTopicId(), virtualQueue.physicalQueueId(), filter,
                                        requestHeader.getQueueOffset(), requestHeader.getMaxMsgNums(), false)
                                    .thenApply(PullResultWrapper::new))
                            .thenApply(resultWrapper -> {
                                if (resultWrapper.isEmpty() || resultWrapper.get().inner().messageList().isEmpty()) {
                                    return new PullResult(PullStatus.NO_MATCHED_MSG, result.nextBeginOffset(), result.minOffset(), result.maxOffset(), Collections.emptyList());
                                }
                                com.automq.rocketmq.store.model.message.PullResult suspendResult = resultWrapper.get().inner();
                                return new PullResult(PullStatus.FOUND, suspendResult.nextBeginOffset(), suspendResult.minOffset(), suspendResult.maxOffset(),
                                    FlatMessageUtil.convertTo(suspendResult.messageList(), requestHeader.getTopic(), 0, config.hostName(), config.remotingListenPort()));
                            });
                    }
                }
                return CompletableFuture.completedFuture(
                    new PullResult(PullStatus.FOUND, result.nextBeginOffset(), result.minOffset(), result.maxOffset(),
                        FlatMessageUtil.convertTo(result.messageList(), requestHeader.getTopic(), 0, config.hostName(), config.remotingListenPort()))
                );
            });
    }

    @Override
    public CompletableFuture<Set<MessageQueue>> lockBatchMQ(ProxyContext ctx, AddressableMessageQueue messageQueue,
        LockBatchRequestBody requestBody, long timeoutMillis) {
        HashSet<MessageQueue> successSet = new HashSet<>();

        for (MessageQueue queue : requestBody.getMqSet()) {
            VirtualQueue virtualQueue = new VirtualQueue(queue.getBrokerName());
            // Expire time is 60 seconds which is a magic number from apache rocketmq.
            boolean result = lockService.tryLock(virtualQueue.topicId(), virtualQueue.physicalQueueId(), requestBody.getClientId(), false, true, 60_000);
            if (result) {
                successSet.add(queue);
            }
        }

        return CompletableFuture.completedFuture(successSet);
    }

    @Override
    public CompletableFuture<Void> unlockBatchMQ(ProxyContext ctx, AddressableMessageQueue messageQueue,
        UnlockBatchRequestBody requestBody, long timeoutMillis) {
        for (MessageQueue queue : requestBody.getMqSet()) {
            VirtualQueue virtualQueue = new VirtualQueue(queue.getBrokerName());
            lockService.release(virtualQueue.topicId(), virtualQueue.physicalQueueId());
        }

        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Long> getMaxOffset(ProxyContext ctx, AddressableMessageQueue messageQueue,
        GetMaxOffsetRequestHeader requestHeader, long timeoutMillis) {
        // TODO: Support in the next iteration
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<Long> getMinOffset(ProxyContext ctx, AddressableMessageQueue messageQueue,
        GetMinOffsetRequestHeader requestHeader, long timeoutMillis) {
        // TODO: Support in the next iteration
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<RemotingCommand> request(ProxyContext ctx, String brokerName, RemotingCommand request,
        long timeoutMillis) {
        throw new UnsupportedOperationException("Shouldn't call the method directly, we never implement it");
    }

    @Override
    public CompletableFuture<Void> requestOneway(ProxyContext ctx, String brokerName, RemotingCommand request,
        long timeoutMillis) {
        throw new UnsupportedOperationException("Shouldn't call the method directly, we never implement it");
    }

    private CompletableFuture<Topic> topicOf(String topicName) {
        CompletableFuture<Topic> topicFuture = metadataService.topicOf(topicName);

        return topicFuture.exceptionally(throwable -> {
            Throwable t = ExceptionUtils.getRealException(throwable);
            if (t instanceof ControllerException controllerException) {
                if (controllerException.getErrorCode() == Code.NOT_FOUND.ordinal()) {
                    throw new ProxyException(apache.rocketmq.v2.Code.TOPIC_NOT_FOUND, "Topic resource does not exist.");
                }
            }
            // Rethrow other exceptions.
            throw new CompletionException(t);
        });
    }

    private CompletableFuture<ConsumerGroup> consumerGroupOf(String groupName) {
        CompletableFuture<ConsumerGroup> groupFuture = metadataService.consumerGroupOf(groupName);

        return groupFuture.exceptionally(throwable -> {
            Throwable t = ExceptionUtils.getRealException(throwable);
            if (t instanceof ControllerException controllerException) {
                if (controllerException.getErrorCode() == Code.NOT_FOUND.ordinal()) {
                    throw new ProxyException(apache.rocketmq.v2.Code.CONSUMER_GROUP_NOT_FOUND, "Consumer group resource does not exist.");
                }
            }
            // Rethrow other exceptions.
            throw new CompletionException(t);
        });
    }
}
