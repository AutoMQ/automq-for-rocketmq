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
import apache.rocketmq.controller.v1.ConsumerGroup;
import apache.rocketmq.controller.v1.MessageQueueAssignment;
import apache.rocketmq.controller.v1.OngoingMessageQueueReassignment;
import apache.rocketmq.controller.v1.StreamRole;
import apache.rocketmq.controller.v1.SubscriptionMode;
import apache.rocketmq.controller.v1.Topic;
import apache.rocketmq.proxy.v1.QueueStats;
import apache.rocketmq.proxy.v1.StreamStats;
import com.automq.rocketmq.common.config.BrokerConfig;
import com.automq.rocketmq.common.config.ProxyConfig;
import com.automq.rocketmq.common.exception.ControllerException;
import com.automq.rocketmq.common.model.FlatMessageExt;
import com.automq.rocketmq.common.model.generated.FlatMessage;
import com.automq.rocketmq.common.util.CommonUtil;
import com.automq.rocketmq.metadata.api.ProxyMetadataService;
import com.automq.rocketmq.proxy.exception.ProxyException;
import com.automq.rocketmq.proxy.grpc.ProxyClient;
import com.automq.rocketmq.proxy.metrics.ProxyMetricsManager;
import com.automq.rocketmq.proxy.model.ProxyContextExt;
import com.automq.rocketmq.proxy.model.VirtualQueue;
import com.automq.rocketmq.proxy.util.ContextUtil;
import com.automq.rocketmq.proxy.util.FlatMessageUtil;
import com.automq.rocketmq.proxy.util.ReceiptHandleUtil;
import com.automq.rocketmq.store.api.DeadLetterSender;
import com.automq.rocketmq.store.api.LogicQueue;
import com.automq.rocketmq.store.api.MessageStore;
import com.automq.rocketmq.store.exception.StoreException;
import com.automq.rocketmq.store.model.StoreContext;
import com.automq.rocketmq.store.model.generated.TimerTag;
import com.automq.rocketmq.store.model.message.Filter;
import com.automq.rocketmq.store.model.message.PutResult;
import com.automq.rocketmq.store.model.message.ResetConsumeOffsetResult;
import com.automq.rocketmq.store.model.message.SQLFilter;
import com.automq.rocketmq.store.model.message.TagFilter;
import com.automq.rocketmq.store.model.transaction.TransactionResolution;
import io.netty.channel.Channel;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.instrumentation.annotations.SpanAttribute;
import io.opentelemetry.instrumentation.annotations.WithSpan;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.rocketmq.broker.client.ProducerManager;
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
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.sysflag.MessageSysFlag;
import org.apache.rocketmq.common.thread.ThreadPoolMonitor;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.common.utils.ExceptionUtils;
import org.apache.rocketmq.proxy.remoting.common.RemotingConverter;
import org.apache.rocketmq.proxy.service.message.MessageService;
import org.apache.rocketmq.proxy.service.route.AddressableMessageQueue;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RequestCode;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.apache.rocketmq.remoting.protocol.body.LockBatchRequestBody;
import org.apache.rocketmq.remoting.protocol.body.UnlockBatchRequestBody;
import org.apache.rocketmq.remoting.protocol.header.AckMessageRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.ChangeInvisibleTimeRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.CheckTransactionStateRequestHeader;
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

public class MessageServiceImpl implements MessageService, ExtendMessageService {
    private static final Logger LOGGER = LoggerFactory.getLogger(MessageServiceImpl.class);
    private final BrokerConfig brokerConfig;
    private final ProxyConfig config;
    private final ProxyMetadataService metadataService;
    private final MessageStore store;
    private final LockService lockService;
    private final DeadLetterSender deadLetterService;
    private final SuspendRequestService suspendRequestService;
    private final ProducerManager producerManager;
    private final ProxyClient relayClient;
    private final ExecutorService executorService = ThreadPoolMonitor.createAndMonitor(2, 5, 100, TimeUnit.SECONDS,
        "Transaction-msg-check-thread", 2000);

    public MessageServiceImpl(BrokerConfig config, MessageStore store, ProxyMetadataService metadataService,
        LockService lockService, DeadLetterSender deadLetterService, ProducerManager producerManager,
        ProxyClient relayClient) throws StoreException {
        this.brokerConfig = config;
        this.config = config.proxy();
        this.store = store;
        this.metadataService = metadataService;
        this.deadLetterService = deadLetterService;
        this.lockService = lockService;
        this.suspendRequestService = SuspendRequestService.getInstance();
        this.producerManager = producerManager;
        this.relayClient = relayClient;

        store.registerTimerMessageHandler(timerTag -> executorService.execute(() -> {
            try {
                ByteBuffer payload = timerTag.payloadAsByteBuffer();
                FlatMessage message = FlatMessage.getRootAsFlatMessage(payload);
                putMessage(message).join();
            } catch (Throwable t) {
                LOGGER.error("Error while check transaction status", t);
            }
        }));

        store.registerTransactionCheckHandler(timerTag -> executorService.execute(() -> {
            try {
                checkTransactionStatus(timerTag);
            } catch (Throwable t) {
                LOGGER.error("Error while check transaction status", t);
            }
        }));
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

    private CompletableFuture<PutResult> putMessage(FlatMessage message) {
        return putMessage(null, message);
    }

    private CompletableFuture<PutResult> putMessage(ProxyContext ctx, FlatMessage message) {
        return topicOf(message.topicId())
            .thenCompose(topic -> putMessage(ctx, topic, message));
    }

    private CompletableFuture<PutResult> putMessage(ProxyContext ctx, Topic topic, FlatMessage message) {
        Optional<MessageQueueAssignment> optional = topic.getAssignmentsList().stream().filter(item -> item.getQueue().getQueueId() == message.queueId()).findFirst();
        if (optional.isEmpty()) {
            Optional<OngoingMessageQueueReassignment> reassignment = topic.getReassignmentsList().stream().filter(item -> item.getQueue().getQueueId() == message.queueId()).findFirst();
            if (reassignment.isPresent()) {
                return forwardMessage(ctx, reassignment.get().getDstNodeId(), message);
            }

            // If the queue is not assigned to any node or under ongoing reassignment, the message will be dropped.
            LOGGER.error("Message: {} is dropped because the topic: {} queue id: {} is not assigned to any node.",
                message.systemProperties().messageId(), topic.getName(), message.queueId());
            return CompletableFuture.failedFuture(new ProxyException(apache.rocketmq.v2.Code.BAD_REQUEST, "Topic " + topic.getName() + "queue id " + message.queueId() + " is not assigned to any node."));
        }

        MessageQueueAssignment assignment = optional.get();
        if (assignment.getNodeId() != brokerConfig.nodeId()) {
            return forwardMessage(ctx, assignment.getNodeId(), message);
        }
        StoreContext storeContext = StoreContext.EMPTY;
        if (ctx != null) {
            storeContext = ContextUtil.buildStoreContext(ctx, topic.getName(), "");
        }
        return store.put(storeContext, message);
    }

    private CompletableFuture<PutResult> forwardMessage(ProxyContext ctx, int nodeId, FlatMessage message) {
        if (ctx instanceof ProxyContextExt contextExt) {
            contextExt.setRelayed(true);
        }
        return metadataService.addressOf(nodeId)
            .thenCompose(address -> relayClient.relayMessage(address, message))
            .thenApply(status -> new PutResult(PutResult.Status.PUT_OK, 0));
    }

    @Override
    @WithSpan(kind = SpanKind.SERVER)
    public CompletableFuture<List<SendResult>> sendMessage(ProxyContext ctx,
        @SpanAttribute AddressableMessageQueue messageQueue,
        List<Message> msgList, @SpanAttribute SendMessageRequestHeader requestHeader, long timeoutMillis) {
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
            ProxyContextExt contextExt = (ProxyContextExt) ctx;
            FlatMessage flatMessage = FlatMessageUtil.convertTo(contextExt, topic.getTopicId(), virtualQueue.physicalQueueId(), config.hostName(), message);

            if (requestHeader.getTopic().startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)) {
                flatMessage.systemProperties().mutateDeliveryAttempts(requestHeader.getReconsumeTimes() + 1);
                if (requestHeader.getReconsumeTimes() > requestHeader.getMaxReconsumeTimes()) {
                    String groupName = requestHeader.getTopic().replace(MixAll.RETRY_GROUP_TOPIC_PREFIX, "");
                    contextExt.span().ifPresent(span -> {
                        span.setAttribute("deadLetter", true);
                        span.setAttribute("group", groupName);
                    });
                    return consumerGroupOf(groupName)
                        .thenCompose(group -> deadLetterService.send(contextExt, group.getGroupId(), flatMessage))
                        .thenApply(ignore -> new PutResult(PutResult.Status.PUT_OK, 0));
                } else {
                    String groupName = requestHeader.getTopic().replace(MixAll.RETRY_GROUP_TOPIC_PREFIX, "");
                    contextExt.span().ifPresent(span -> {
                        span.setAttribute("retry", true);
                        span.setAttribute("group", groupName);
                        span.setAttribute("reconsumeTimes", requestHeader.getReconsumeTimes());
                        span.setAttribute("deliveryTimestamp", flatMessage.systemProperties().deliveryTimestamp());
                    });
                    return putMessage(ctx, topic, flatMessage);
                }
            }
            return putMessage(ctx, topic, flatMessage);
        });

        return putFuture.thenApply(putResult -> {
            ProxyMetricsManager.recordIncomingMessages(requestHeader.getTopic(), getMessageType(requestHeader), 1, message.getBody().length);

            SendResult result = new SendResult();
            result.setSendStatus(SendStatus.SEND_OK);
            result.setMsgId(messageId);
            result.setTransactionId(putResult.transactionId());
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

            return store.pull(StoreContext.EMPTY, group.getGroupId(), topic.getTopicId(), virtualQueue.physicalQueueId(),
                    Filter.DEFAULT_FILTER, requestHeader.getOffset(), 1, false)
                .thenApply(pullResult -> {
                    if (pullResult.status() == com.automq.rocketmq.store.model.message.PullResult.Status.FOUND) {
                        return pullResult.messageList().get(0);
                    }
                    throw new ProxyException(apache.rocketmq.v2.Code.MESSAGE_NOT_FOUND, "Message not found from server.");
                }).thenCompose(messageExt -> {
                    if (messageExt.deliveryAttempts() > group.getMaxDeliveryAttempt()) {
                        return deadLetterService.send((ProxyContextExt) ctx, group.getGroupId(), messageExt.message());
                    }

                    // Message consume retry strategy
                    // <0: no retry,put into DLQ directly
                    // =0: broker control retry frequency
                    // >0: client control retry frequency
                    return switch (Integer.compare(delayLevel, 0)) {
                        case -1 ->
                            deadLetterService.send((ProxyContextExt) ctx, group.getGroupId(), messageExt.message());
                        case 0 -> topicOf(MixAll.RETRY_GROUP_TOPIC_PREFIX + requestHeader.getGroup())
                            .thenCompose(retryTopic -> {
                                // Keep the same logic as apache RocketMQ.
                                int serverDelayLevel = messageExt.deliveryAttempts() + 1;
                                messageExt.setDeliveryAttempts(serverDelayLevel);
                                messageExt.setOriginalQueueOffset(messageExt.originalOffset());

                                FlatMessage message = messageExt.message();
                                message.mutateTopicId(retryTopic.getTopicId());

                                message.systemProperties().mutateDeliveryTimestamp(FlatMessageUtil.calculateDeliveryTimestamp(serverDelayLevel));
                                return store.put(StoreContext.EMPTY, message)
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
                                return store.put(StoreContext.EMPTY, message)
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
        TransactionResolution resolution;
        switch (requestHeader.getCommitOrRollback()) {
            case MessageSysFlag.TRANSACTION_COMMIT_TYPE -> resolution = TransactionResolution.COMMIT;
            case MessageSysFlag.TRANSACTION_ROLLBACK_TYPE, MessageSysFlag.TRANSACTION_NOT_TYPE ->
                resolution = TransactionResolution.ROLLBACK;
            default -> {
                return CompletableFuture.failedFuture(new ProxyException(apache.rocketmq.v2.Code.BAD_REQUEST, "Unknown transaction resolution"));
            }
        }

        return store.endTransaction(requestHeader.getTransactionId(), resolution)
            .thenCompose(optional -> {
                if (optional.isEmpty()) {
                    return CompletableFuture.completedFuture(null);
                }

                return putMessage(ctx, optional.get())
                    .thenAccept(ignore -> {
                    });
            });
    }

    private void checkTransactionStatus(TimerTag timerTag) {
        ByteBuffer payload = timerTag.payloadAsByteBuffer();
        FlatMessage message = FlatMessage.getRootAsFlatMessage(payload);
        try {
            message.systemProperties().mutateOrphanedTransactionCheckTimes(message.systemProperties().orphanedTransactionCheckTimes() + 1);
            store.scheduleCheckTransaction(message);

            Topic topic = metadataService.topicOf(message.topicId()).join();
            String producerGroup = message.systemProperties().orphanedTransactionProducer();
            if (StringUtils.isBlank(producerGroup)) {
                producerGroup = topic.getName();
            }
            Channel channel = producerManager.getAvailableChannel(producerGroup);
            if (channel != null) {
                CheckTransactionStateRequestHeader requestHeader = new CheckTransactionStateRequestHeader();
                requestHeader.setCommitLogOffset(0L);
                requestHeader.setOffsetMsgId("");
                requestHeader.setTranStateTableOffset(0L);
                requestHeader.setMsgId(message.systemProperties().messageId());
                requestHeader.setTransactionId(message.systemProperties().messageId());

                ByteBuffer buffer = timerTag.identityAsByteBuffer();
                byte[] identity = new byte[buffer.remaining()];
                buffer.get(identity);
                requestHeader.setTransactionId(new String(identity));

                RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.CHECK_TRANSACTION_STATE, requestHeader);
                FlatMessageExt flatMessageExt = FlatMessageExt.Builder.builder()
                    .offset(0L)
                    .message(message)
                    .build();
                MessageExt messageExt = FlatMessageUtil.convertTo(flatMessageExt, topic.getName(), 0, config.hostName(), config.grpcListenPort());
                request.setBody(RemotingConverter.getInstance().convertMsgToBytes(messageExt));
                channel.writeAndFlush(request);
            }
        } catch (Exception e) {
            LOGGER.error("Error while check transaction: message {}", message.systemProperties().messageId(), e);
        }
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

    @WithSpan(kind = SpanKind.SERVER)
    private CompletableFuture<InnerPopResult> popSpecifiedQueueUnsafe(ProxyContext context,
        ConsumerGroup consumerGroup, Topic topic, int queueId, Filter filter, int batchSize, boolean fifo,
        long invisibleDuration) {
        List<FlatMessageExt> messageList = new ArrayList<>();
        long consumerGroupId = consumerGroup.getGroupId();
        long topicId = topic.getTopicId();
        StoreContext storeContext = ContextUtil.buildStoreContext(context, topic.getName(), consumerGroup.getName());

        // Decide whether to pop the retry-messages first
        boolean retryPriority = !fifo && CommonUtil.applyPercentage(config.retryPriorityPercentage());

        CompletableFuture<InnerPopResult> popFuture = store.pop(storeContext, consumerGroupId, topicId, queueId, filter, batchSize, fifo, retryPriority, invisibleDuration)
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
                return store.pop(storeContext, consumerGroupId, topicId, queueId, filter, batchSize - firstPopMessageCount, false, !retryPriority, invisibleDuration)
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

    @WithSpan(kind = SpanKind.SERVER)
    private CompletableFuture<InnerPopResult> popSpecifiedQueue(ProxyContext context,
        @SpanAttribute ConsumerGroup consumerGroup,
        @SpanAttribute String clientId, @SpanAttribute Topic topic, @SpanAttribute int queueId,
        @SpanAttribute Filter filter, @SpanAttribute int batchSize, @SpanAttribute boolean fifo,
        @SpanAttribute long invisibleDuration, @SpanAttribute long timeoutMillis) {
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
        AtomicLong restOffset = new AtomicLong();
        store.getOffsetRange(topicId, queueId, -1)
            .stream()
            .filter(range -> range.streamRole() == StreamRole.STREAM_ROLE_DATA)
            .findFirst()
            .ifPresent(range -> {
                long diff = range.endOffset() - store.getConsumeOffset(consumerGroup.getGroupId(), topicId, queueId);
                if (diff > 0) {
                    restOffset.set(diff);
                }
            });
        return CompletableFuture.completedFuture(new InnerPopResult(restOffset.get(), Collections.emptyList()));
    }

    @Override
    @WithSpan(kind = SpanKind.SERVER)
    public CompletableFuture<PopResult> popMessage(ProxyContext ctx,
        @SpanAttribute AddressableMessageQueue messageQueue,
        @SpanAttribute PopMessageRequestHeader requestHeader, @SpanAttribute long timeoutMillis) {
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
        }).thenCompose(nil -> {
            long timeout = timeoutMillis - config.networkRTTMills();
            return popSpecifiedQueue(ctx, consumerGroupReference.get(), clientId, topicReference.get(), virtualQueue.physicalQueueId(), filter,
                requestHeader.getMaxMsgNums(), requestHeader.isOrder(), requestHeader.getInvisibleTime(), timeout);
        });

        return popMessageFuture.thenCompose(result -> {
            if (result.messageList.isEmpty()) {
                if (result.restMessageCount > 0) {
                    // This means there are messages in the queue but not match the filter. So we should prevent long polling.
                    PopResult popResult = new PopResult(PopStatus.NO_NEW_MSG, Collections.emptyList());
                    popResult.setRestNum(result.restMessageCount);
                    return CompletableFuture.completedFuture(popResult);
                } else {
                    return suspendRequestService.suspendRequest((ProxyContextExt) ctx, requestHeader.getTopic(), virtualQueue.physicalQueueId(), filter, timeoutMillis,
                            // Function to pop message later.
                            timeout -> popSpecifiedQueue(ctx, consumerGroupReference.get(), clientId, topicReference.get(), virtualQueue.physicalQueueId(), filter,
                                requestHeader.getMaxMsgNums(), requestHeader.isOrder(), requestHeader.getInvisibleTime(), timeout))
                        .thenApply(suspendResult -> {
                            if (suspendResult.isEmpty() || suspendResult.get().messageList().isEmpty()) {
                                return new PopResult(PopStatus.POLLING_NOT_FOUND, Collections.emptyList());
                            }
                            PopResult popResult = new PopResult(PopStatus.FOUND, FlatMessageUtil.convertTo((ProxyContextExt) ctx, suspendResult.get().messageList(), requestHeader.getTopic(), requestHeader.getInvisibleTime(), config.hostName(), config.grpcListenPort()));
                            popResult.setRestNum(suspendResult.get().restMessageCount());
                            return popResult;
                        });
                }
            }
            PopResult popResult = new PopResult(PopStatus.FOUND, FlatMessageUtil.convertTo((ProxyContextExt) ctx, result.messageList, requestHeader.getTopic(), requestHeader.getInvisibleTime(), config.hostName(), config.grpcListenPort()));
            popResult.setRestNum(result.restMessageCount);
            return CompletableFuture.completedFuture(popResult);
        }).thenApply(result -> {
            ((ProxyContextExt) ctx).span().ifPresent(span -> {
                span.setAttribute("result.status", result.getPopStatus().name());
                span.setAttribute("result.messageCount", result.getMsgFoundList().size());
                span.setAttribute("result.restMessageCount", result.getRestNum());
            });
            return result;
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
            result.setExtraInfo("");
            result.setStatus(AckStatus.NO_EXIST);
            return CompletableFuture.completedFuture(result);
        }

        return store.changeInvisibleDuration(rawHandle, requestHeader.getInvisibleTime())
            .thenApply(changeInvisibleDurationResult -> {
                org.apache.rocketmq.client.consumer.AckResult ackResult = new org.apache.rocketmq.client.consumer.AckResult();
                ackResult.setExtraInfo(requestHeader.getExtraInfo());
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
                return CompletableFuture.completedFuture(store.getConsumeOffset(consumerGroup.getGroupId(), topic.getTopicId(), virtualQueue.physicalQueueId()));
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
                return resetConsumeOffsetOfQueue(topic.getTopicId(), virtualQueue.physicalQueueId(), consumerGroup, requestHeader.getCommitOffset());
            });
    }

    private CompletableFuture<Void> resetConsumeOffsetOfQueue(long topicId, int queueId, ConsumerGroup consumerGroup,
        long newConsumeOffset) {
        CompletableFuture<ResetConsumeOffsetResult> cf;
        if (consumerGroup.getSubMode() == SubscriptionMode.SUB_MODE_PULL) {
            cf = metadataService.updateConsumerOffset(
                    consumerGroup.getGroupId(),
                    topicId,
                    queueId,
                    newConsumeOffset)
                .thenApply(nil -> new ResetConsumeOffsetResult(ResetConsumeOffsetResult.Status.SUCCESS));
        } else {
            cf = store.resetConsumeOffset(consumerGroup.getGroupId(), topicId, queueId, newConsumeOffset);
        }
        return cf.thenAccept(resetConsumeOffsetResult -> {
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

                StoreContext storeContext = ContextUtil.buildStoreContext(ctx, topic.getName(), group.getName());
                return store.pull(storeContext, group.getGroupId(), topic.getTopicId(), virtualQueue.physicalQueueId(), filter, requestHeader.getQueueOffset(), requestHeader.getMaxMsgNums(), false);
            })
            .thenCompose(result -> {
                Topic topic = topicReference.get();
                ConsumerGroup group = consumerGroupReference.get();
                if (result.messageList().isEmpty()) {
                    if (result.maxOffset() - result.nextBeginOffset() > 0) {
                        // This means there are messages in the queue but not match the filter. So we should prevent long polling.
                        return CompletableFuture.completedFuture(new PullResult(PullStatus.NO_MATCHED_MSG, result.nextBeginOffset(), result.minOffset(), result.maxOffset(), Collections.emptyList()));
                    } else {
                        StoreContext storeContext = ContextUtil.buildStoreContext(ctx, topic.getName(), group.getName());
                        return suspendRequestService.suspendRequest((ProxyContextExt) ctx, requestHeader.getTopic(), virtualQueue.physicalQueueId(), filter, timeoutMillis,
                                // Function to pull message later.
                                timeout -> store.pull(storeContext, group.getGroupId(), topic.getTopicId(), virtualQueue.physicalQueueId(), filter,
                                        requestHeader.getQueueOffset(), requestHeader.getMaxMsgNums(), false)
                                    .thenApply(PullResultWrapper::new))
                            .thenApply(resultWrapper -> {
                                if (resultWrapper.isEmpty() || resultWrapper.get().inner().messageList().isEmpty()) {
                                    return new PullResult(PullStatus.NO_MATCHED_MSG, result.nextBeginOffset(), result.minOffset(), result.maxOffset(), Collections.emptyList());
                                }
                                com.automq.rocketmq.store.model.message.PullResult suspendResult = resultWrapper.get().inner();
                                recordMetricsForPulling(topic.getName(), group.getName(), suspendResult);
                                return new PullResult(PullStatus.FOUND, suspendResult.nextBeginOffset(), suspendResult.minOffset(), suspendResult.maxOffset(),
                                    FlatMessageUtil.convertTo(null, suspendResult.messageList(), requestHeader.getTopic(), 0, config.hostName(), config.remotingListenPort()));
                            });
                    }
                }

                recordMetricsForPulling(topic.getName(), group.getName(), result);
                return CompletableFuture.completedFuture(
                    new PullResult(PullStatus.FOUND, result.nextBeginOffset(), result.minOffset(), result.maxOffset(),
                        FlatMessageUtil.convertTo(null, result.messageList(), requestHeader.getTopic(), 0, config.hostName(), config.remotingListenPort()))
                );
            });
    }

    private void recordMetricsForPulling(String topic, String group,
        com.automq.rocketmq.store.model.message.PullResult result) {
        Integer messageBytesTotal = result.messageList().stream().map(message -> message.message().payloadAsByteBuffer().remaining()).reduce(0, Integer::sum);
        ProxyMetricsManager.recordOutgoingMessages(topic, group, result.messageList().size(), messageBytesTotal, topic.startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX));
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
        String topic = requestHeader.getTopic();
        int queueId = requestHeader.getQueueId();

        return topicOf(topic).thenApply(topicMetadata -> {
            Optional<LogicQueue.StreamOffsetRange> dataStreamRange = store.getOffsetRange(topicMetadata.getTopicId(), queueId, -1)
                .stream()
                .filter(offsetRange -> offsetRange.streamRole() == StreamRole.STREAM_ROLE_DATA)
                .findFirst();
            if (dataStreamRange.isEmpty()) {
                throw new ProxyException(apache.rocketmq.v2.Code.BAD_REQUEST, "Topic is not opened in this node.");
            }
            return dataStreamRange.get().endOffset();
        });
    }

    @Override
    public CompletableFuture<Long> getMinOffset(ProxyContext ctx, AddressableMessageQueue messageQueue,
        GetMinOffsetRequestHeader requestHeader, long timeoutMillis) {
        String topic = requestHeader.getTopic();
        int queueId = requestHeader.getQueueId();

        return topicOf(topic).thenApply(topicMetadata -> {
            Optional<LogicQueue.StreamOffsetRange> dataStreamRange = store.getOffsetRange(topicMetadata.getTopicId(), queueId, -1)
                .stream()
                .filter(offsetRange -> offsetRange.streamRole() == StreamRole.STREAM_ROLE_DATA)
                .findFirst();
            if (dataStreamRange.isEmpty()) {
                throw new ProxyException(apache.rocketmq.v2.Code.BAD_REQUEST, "Topic is not opened in this node.");
            }
            return dataStreamRange.get().startOffset();
        });
    }

    private long getConsumerOffset(ConsumerGroup consumerGroup, Topic topic, int queueId, boolean retry) {
        if (consumerGroup.getSubMode() == SubscriptionMode.SUB_MODE_PULL) {
            if (retry) {
                topicOf(MixAll.RETRY_GROUP_TOPIC_PREFIX + consumerGroup.getName())
                    .thenCompose(retryTopic -> metadataService.consumerOffsetOf(consumerGroup.getGroupId(), retryTopic.getTopicId(), queueId));
                return metadataService.consumerOffsetOf(consumerGroup.getGroupId(), topic.getTopicId(), queueId).join();
            }
            return metadataService.consumerOffsetOf(consumerGroup.getGroupId(), topic.getTopicId(), queueId).join();
        }

        if (retry) {
            return store.getRetryConsumeOffset(consumerGroup.getGroupId(), topic.getTopicId(), queueId);
        }
        return store.getConsumeOffset(consumerGroup.getGroupId(), topic.getTopicId(), queueId);
    }

    private List<StreamStats> getStreamStats(Optional<ConsumerGroup> consumerGroup, Topic topic, int queueId) {
        return store.getOffsetRange(topic.getTopicId(), queueId, consumerGroup.map(ConsumerGroup::getGroupId).orElse(-1L))
            .stream().map(offsetRange -> {
                long consumerOffset = 0;
                if (consumerGroup.isPresent()) {
                    if (offsetRange.streamRole() == StreamRole.STREAM_ROLE_DATA) {
                        consumerOffset = getConsumerOffset(consumerGroup.get(), topic, queueId, false);
                    } else if (offsetRange.streamRole() == StreamRole.STREAM_ROLE_RETRY) {
                        consumerOffset = getConsumerOffset(consumerGroup.get(), topic, queueId, true);
                    }
                }
                return StreamStats.newBuilder()
                    .setStreamId(offsetRange.streamId())
                    .setMinOffset(offsetRange.startOffset())
                    .setMaxOffset(offsetRange.endOffset())
                    .setRole(offsetRange.streamRole())
                    .setConsumeOffset(consumerOffset)
                    .build();
            }).toList();
    }

    @Override
    public CompletableFuture<Pair<Long, List<QueueStats>>> getTopicStats(String topic, int queueId,
        String consumerGroup) {
        CompletableFuture<Optional<ConsumerGroup>> groupIdFuture;
        if (StringUtils.isBlank(consumerGroup)) {
            groupIdFuture = CompletableFuture.completedFuture(Optional.empty());
        } else {
            groupIdFuture = metadataService.consumerGroupOf(consumerGroup).thenApply(Optional::of);
        }
        return topicOf(topic)
            .thenCombine(groupIdFuture, (topicMetadata, groupMetadata) -> {
                long topicId = topicMetadata.getTopicId();
                List<QueueStats> queueStatsList = new ArrayList<>();
                if (queueId != -1) {
                    List<StreamStats> streamStatsList = getStreamStats(groupMetadata, topicMetadata, queueId);
                    queueStatsList.add(QueueStats.newBuilder().setQueueId(queueId).addAllStreamStats(streamStatsList).build());
                } else {
                    int queueCount = topicMetadata.getCount();
                    for (int i = 0; i < queueCount; i++) {
                        List<StreamStats> streamStatsList = getStreamStats(groupMetadata, topicMetadata, i);
                        if (!streamStatsList.isEmpty()) {
                            queueStatsList.add(QueueStats.newBuilder().setQueueId(queueId).addAllStreamStats(streamStatsList).build());
                        }
                    }
                }
                return Pair.of(topicId, queueStatsList);
            });
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

    private CompletableFuture<Topic> topicOf(long topicId) {
        CompletableFuture<Topic> topicFuture = metadataService.topicOf(topicId);

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

    @Override
    public CompletableFuture<Void> resetConsumeOffset(String topicName, int queueId, String consumerGroupName,
        long newConsumeOffset) {
        CompletableFuture<ConsumerGroup> consumeGroupFuture = metadataService.consumerGroupOf(consumerGroupName);
        CompletableFuture<Topic> topicFuture = metadataService.topicOf(topicName);

        return consumeGroupFuture.thenCombine(topicFuture, Pair::of)
            .thenCompose(pair -> {
                ConsumerGroup consumerGroup = pair.getLeft();
                Topic topic = pair.getRight();
                return resetConsumeOffsetOfQueue(topic.getTopicId(), queueId, consumerGroup, newConsumeOffset);
            });
    }

    @Override
    public CompletableFuture<Void> resetConsumeOffsetByTimestamp(String topic, int queueId, String consumerGroup,
        long timestamp) {
        throw new UnsupportedOperationException("Not implemented yet");
    }
}
