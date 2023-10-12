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
import apache.rocketmq.controller.v1.Topic;
import com.automq.rocketmq.common.config.ProxyConfig;
import com.automq.rocketmq.common.model.FlatMessageExt;
import com.automq.rocketmq.common.util.CommonUtil;
import com.automq.rocketmq.controller.exception.ControllerException;
import com.automq.rocketmq.metadata.api.ProxyMetadataService;
import com.automq.rocketmq.proxy.model.VirtualQueue;
import com.automq.rocketmq.proxy.util.FlatMessageUtil;
import com.automq.rocketmq.proxy.util.ReceiptHandleUtil;
import com.automq.rocketmq.store.api.MessageStore;
import com.automq.rocketmq.store.model.message.Filter;
import com.automq.rocketmq.store.model.message.PutResult;
import com.automq.rocketmq.store.model.message.SQLFilter;
import com.automq.rocketmq.store.model.message.TagFilter;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.rocketmq.client.consumer.AckResult;
import org.apache.rocketmq.client.consumer.AckStatus;
import org.apache.rocketmq.client.consumer.PopResult;
import org.apache.rocketmq.client.consumer.PopStatus;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.common.consumer.ReceiptHandle;
import org.apache.rocketmq.common.filter.ExpressionType;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageClientIDSetter;
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

    public MessageServiceImpl(ProxyConfig config, MessageStore store, ProxyMetadataService metadataService,
        LockService lockService) {
        this.config = config;
        this.store = store;
        this.metadataService = metadataService;
        this.lockService = lockService;
    }

    @Override
    public CompletableFuture<List<SendResult>> sendMessage(ProxyContext ctx, AddressableMessageQueue messageQueue,
        List<Message> msgList, SendMessageRequestHeader requestHeader, long timeoutMillis) {
        if (msgList.size() != 1) {
            throw new UnsupportedOperationException("Batch message is not supported");
        }
        Message message = msgList.get(0);
        String messageId = MessageClientIDSetter.getUniqID(message);

        CompletableFuture<Topic> topicFuture = topicOf(requestHeader.getTopic());

        CompletableFuture<PutResult> putFuture = topicFuture.thenCompose(topic -> {
            VirtualQueue virtualQueue = new VirtualQueue(messageQueue);
            if (topic.getTopicId() != virtualQueue.topicId()) {
                LOGGER.error("Topic id in request header {} does not match topic id in message queue {}, maybe the topic is recreated.",
                    topic.getTopicId(), virtualQueue.topicId());
                return CompletableFuture.failedFuture(new MQBrokerException(ResponseCode.TOPIC_NOT_EXIST, "Topic not exist"));
            }

            return store.put(FlatMessageUtil.convertTo(topic.getTopicId(), virtualQueue.physicalQueueId(), config.localHostName(), message));
        });

        return putFuture.thenApply(putResult -> {
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
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<Void> endTransactionOneway(ProxyContext ctx, String brokerName,
        EndTransactionRequestHeader requestHeader, long timeoutMillis) {
        throw new UnsupportedOperationException();
    }

    private CompletableFuture<Void> popSpecifiedQueueUnsafe(long consumerGroupId, long topicId, int queueId,
        Filter filter, int batchSize, boolean fifo, long invisibleDuration, List<FlatMessageExt> messageList) {

        // Decide whether to pop the retry-messages first
        boolean retryPriority = !fifo && CommonUtil.applyPercentage(config.retryPriorityPercentage());

        CompletableFuture<List<FlatMessageExt>> popFuture = store.pop(consumerGroupId, topicId, queueId, filter, batchSize, fifo, retryPriority, invisibleDuration)
            .thenApply(popResult -> {
                List<FlatMessageExt> resultList = popResult.messageList();
                messageList.addAll(resultList);
                return resultList;
            });

        // There is no retry message when pop orderly. So that return origin messages directly.
        if (fifo) {
            return popFuture.thenAccept(resultMessageList -> {
            });
        }

        return popFuture.thenCompose(resultMessageList -> {
            if (resultMessageList.size() < batchSize) {
                return store.pop(consumerGroupId, topicId, queueId, filter, batchSize - resultMessageList.size(), false, !retryPriority, invisibleDuration)
                    .thenApply(com.automq.rocketmq.store.model.message.PopResult::messageList);
            }
            return CompletableFuture.completedFuture(new ArrayList<FlatMessageExt>());
        }).thenAccept(messageList::addAll);
    }

    private CompletableFuture<Void> popSpecifiedQueue(long consumerGroupId, String clientId, long topicId, int queueId,
        Filter filter, int batchSize, boolean fifo, long invisibleDuration, long timeoutMillis,
        List<FlatMessageExt> messageList) {
        if (lockService.tryLock(topicId, queueId, clientId, fifo)) {
            return popSpecifiedQueueUnsafe(consumerGroupId, topicId, queueId, filter, batchSize, fifo, invisibleDuration, messageList)
                .orTimeout(timeoutMillis, TimeUnit.MILLISECONDS)
                .whenComplete((v, throwable) -> {
                    // TODO: log exception.
                    // Release lock since complete or timeout.
                    lockService.release(topicId, queueId);
                });
        }
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<PopResult> popMessage(ProxyContext ctx, AddressableMessageQueue messageQueue,
        PopMessageRequestHeader requestHeader, long timeoutMillis) {
        CompletableFuture<Topic> topicFuture = topicOf(requestHeader.getTopic());
        CompletableFuture<ConsumerGroup> groupFuture = consumerGroupOf(requestHeader.getConsumerGroup());

        // Build the virtual queue
        VirtualQueue virtualQueue = new VirtualQueue(messageQueue);
        List<FlatMessageExt> messageList = new ArrayList<>();

        CompletableFuture<Void> popMessageFuture = topicFuture.thenCombine(groupFuture, (topic, group) -> {
            if (topic.getTopicId() != virtualQueue.topicId()) {
                LOGGER.error("Topic id in request header {} does not match topic id in message queue {}, maybe the topic is recreated.",
                    topic.getTopicId(), virtualQueue.topicId());
                throw new CompletionException(new MQBrokerException(ResponseCode.TOPIC_NOT_EXIST, "Topic not exist"));
            }

            return Pair.of(topic, group);
        }).thenCompose(pair -> {
            long consumerGroupId = pair.getRight().getGroupId();
            long topicId = pair.getLeft().getTopicId();
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

            return popSpecifiedQueue(consumerGroupId, clientId, topicId, virtualQueue.physicalQueueId(), filter,
                requestHeader.getMaxMsgNums(), requestHeader.isOrder(), requestHeader.getInvisibleTime(), timeoutMillis, messageList);
        });

        return popMessageFuture.thenApply(v -> {
            PopStatus status;
            if (messageList.isEmpty()) {
                status = PopStatus.NO_NEW_MSG;
            } else {
                status = PopStatus.FOUND;
            }
            return new PopResult(status, FlatMessageUtil.convertTo(messageList, requestHeader.getTopic(), requestHeader.getInvisibleTime()));
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

        CompletableFuture<AckResult> resultF = store.ack(rawHandle).thenApply(ackResult -> {
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
    public CompletableFuture<PullResult> pullMessage(ProxyContext ctx, AddressableMessageQueue messageQueue,
        PullMessageRequestHeader requestHeader, long timeoutMillis) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<Long> queryConsumerOffset(ProxyContext ctx, AddressableMessageQueue messageQueue,
        QueryConsumerOffsetRequestHeader requestHeader, long timeoutMillis) {
        CompletableFuture<ConsumerGroup> consumeGroupFuture = metadataService.consumerGroupOf(requestHeader.getConsumerGroup());
        CompletableFuture<Topic> topicFuture = metadataService.topicOf(requestHeader.getTopic());

        return consumeGroupFuture.thenCombine(topicFuture, Pair::of)
            .thenCompose(pair -> metadataService.consumerOffsetOf(pair.getLeft().getGroupId(), pair.getRight().getTopicId(), requestHeader.getQueueId()));
    }

    @Override
    public CompletableFuture<Void> updateConsumerOffset(ProxyContext ctx, AddressableMessageQueue messageQueue,
        UpdateConsumerOffsetRequestHeader requestHeader, long timeoutMillis) {
        CompletableFuture<ConsumerGroup> consumeGroupFuture = metadataService.consumerGroupOf(requestHeader.getConsumerGroup());
        CompletableFuture<Topic> topicFuture = metadataService.topicOf(requestHeader.getTopic());
        // TODO: support retry topic.
        return consumeGroupFuture.thenCombine(topicFuture, Pair::of)
            .thenCompose(pair -> metadataService.updateConsumerOffset(pair.getLeft().getGroupId(), pair.getRight().getTopicId(), requestHeader.getQueueId(), requestHeader.getCommitOffset()));
    }

    @Override
    public CompletableFuture<Set<MessageQueue>> lockBatchMQ(ProxyContext ctx, AddressableMessageQueue messageQueue,
        LockBatchRequestBody requestBody, long timeoutMillis) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<Void> unlockBatchMQ(ProxyContext ctx, AddressableMessageQueue messageQueue,
        UnlockBatchRequestBody requestBody, long timeoutMillis) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<Long> getMaxOffset(ProxyContext ctx, AddressableMessageQueue messageQueue,
        GetMaxOffsetRequestHeader requestHeader, long timeoutMillis) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<Long> getMinOffset(ProxyContext ctx, AddressableMessageQueue messageQueue,
        GetMinOffsetRequestHeader requestHeader, long timeoutMillis) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<RemotingCommand> request(ProxyContext ctx, String brokerName, RemotingCommand request,
        long timeoutMillis) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<Void> requestOneway(ProxyContext ctx, String brokerName, RemotingCommand request,
        long timeoutMillis) {
        throw new UnsupportedOperationException();
    }

    private CompletableFuture<Topic> topicOf(String topicName) {
        CompletableFuture<Topic> topicFuture = metadataService.topicOf(topicName);

        return topicFuture.exceptionally(throwable -> {
            Throwable t = ExceptionUtils.getRealException(throwable);
            if (t instanceof ControllerException controllerException) {
                if (controllerException.getErrorCode() == Code.NOT_FOUND.ordinal()) {
                    throw new CompletionException(new MQBrokerException(ResponseCode.TOPIC_NOT_EXIST, "Topic not exist"));
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
                    throw new CompletionException(new MQBrokerException(ResponseCode.SUBSCRIPTION_GROUP_NOT_EXIST, "Consumer group not found"));
                }
            }
            // Rethrow other exceptions.
            throw new CompletionException(t);
        });
    }
}
