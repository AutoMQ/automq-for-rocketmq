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

package com.automq.rocketmq.proxy;

import com.automq.rocketmq.common.config.ProxyConfig;
import com.automq.rocketmq.common.model.MessageExt;
import com.automq.rocketmq.common.util.CommonUtil;
import com.automq.rocketmq.metadata.ProxyMetadataService;
import com.automq.rocketmq.proxy.service.LockService;
import com.automq.rocketmq.proxy.util.RocketMQMessageUtil;
import com.automq.rocketmq.store.MessageStore;
import com.automq.rocketmq.store.model.message.Filter;
import com.automq.rocketmq.store.model.message.PutResult;
import com.automq.rocketmq.store.model.message.SQLFilter;
import com.automq.rocketmq.store.model.message.TagFilter;
import com.automq.rocketmq.store.util.MessageUtil;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.consumer.AckResult;
import org.apache.rocketmq.client.consumer.AckStatus;
import org.apache.rocketmq.client.consumer.PopResult;
import org.apache.rocketmq.client.consumer.PopStatus;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.common.consumer.ReceiptHandle;
import org.apache.rocketmq.common.filter.ExpressionType;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.service.message.MessageService;
import org.apache.rocketmq.proxy.service.route.AddressableMessageQueue;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
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

public class MessageServiceImpl implements MessageService {
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
        long topicId = metadataService.queryTopicId(requestHeader.getTopic());

        List<CompletableFuture<PutResult>> completableFutureList = msgList.stream()
            .map(message -> MessageUtil.transferToMessage(topicId, requestHeader.getQueueId(), message.getTags(),
                message.getProperties(), message.getBody()))
            .map(message -> store.put(message, new HashMap<>()))
            .toList();

        return CompletableFuture.allOf(completableFutureList.toArray(CompletableFuture[]::new))
            .thenApply(v -> completableFutureList.stream()
                .map(future -> future.getNow(null))
                .filter(Objects::nonNull)
                .map(putResult -> {
                    SendResult result = new SendResult();
                    result.setSendStatus(SendStatus.SEND_OK);
                    result.setMessageQueue(new MessageQueue(requestHeader.getTopic(), requestHeader.getBname(), requestHeader.getQueueId()));
                    result.setQueueOffset(putResult.offset());
                    return result;
                }).toList());
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
        Filter filter, int batchSize, boolean fifo, long invisibleDuration, List<MessageExt> messageList) {
        long offset = metadataService.queryConsumerOffset(consumerGroupId, topicId, queueId);

        boolean retryPriority = !fifo && CommonUtil.applyPercentage(config.retryPriorityPercentage());

        // TODO: acquire lock if pop orderly.
        // Assume the variable retryPriority is false, so try to pop origin messages first.
        CompletableFuture<List<MessageExt>> popFuture = store.pop(consumerGroupId, topicId, queueId, offset, filter, batchSize, fifo, retryPriority, invisibleDuration)
            .thenApply(popResult -> {
                // Advance consumer offset for origin topic.
                List<MessageExt> resultList = popResult.messageList();
                if (!resultList.isEmpty()) {
                    MessageExt lastMessage = resultList.get(resultList.size() - 1);
                    metadataService.updateConsumerOffset(consumerGroupId, topicId, queueId, lastMessage.offset() + 1, retryPriority);
                }
                // Add all messages popped from origin topic into result list.
                messageList.addAll(resultList);
                return resultList;
            });

        // There is no retry message when pop orderly. So that return origin messages directly.
        if (fifo) {
            return popFuture.thenAccept(resultMessageList -> {
            });
        }

        // Try to pop retry messages.
        return popFuture.thenCompose(resultMessageList -> {
            if (resultMessageList.size() < batchSize) {
                return store.pop(consumerGroupId, topicId, queueId, offset, filter, batchSize - resultMessageList.size(), false, !retryPriority, invisibleDuration)
                    .thenApply(com.automq.rocketmq.store.model.message.PopResult::messageList);
            }
            return CompletableFuture.completedFuture(new ArrayList<MessageExt>());
        }).thenAccept(resultMessageList -> {
            // Advance consumer offset for retry topic.
            if (!resultMessageList.isEmpty()) {
                MessageExt lastMessage = resultMessageList.get(resultMessageList.size() - 1);
                metadataService.updateConsumerOffset(consumerGroupId, topicId, queueId, lastMessage.offset() + 1, !retryPriority);
            }
            // Add all messages popped from retry topic into result list.
            messageList.addAll(resultMessageList);
        });
    }

    private CompletableFuture<Void> popSpecifiedQueue(long consumerGroupId, String clientId, long topicId, int queueId,
        Filter filter, int batchSize, boolean fifo, long invisibleDuration, List<MessageExt> messageList) {
        if (lockService.tryLock(topicId, queueId, clientId, fifo)) {
            return popSpecifiedQueueUnsafe(consumerGroupId, topicId, queueId, filter, batchSize, fifo, invisibleDuration, messageList)
                .orTimeout(invisibleDuration, TimeUnit.NANOSECONDS)
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
        long consumerGroupId = metadataService.queryConsumerGroupId(requestHeader.getConsumerGroup());
        long topicId = metadataService.queryTopicId(requestHeader.getTopic());
        String clientId = ctx.getClientID();
        Set<Integer> assignmentQueueSet = metadataService.queryAssignmentQueueSet(topicId);

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

        List<MessageExt> messageList = new ArrayList<>();
        CompletableFuture<Void> popMessageFuture;

        // If the queue id in the request header is less than 0, the proxy needs to pop messages
        // from all assigned queues. Otherwise, the proxy pop messages from the specified queue.
        if (requestHeader.getQueueId() >= 0) {
            if (!assignmentQueueSet.contains(requestHeader.getQueueId())) {
                throw new RuntimeException(String.format("this proxy does not deal with topic %s queue %d",
                    requestHeader.getTopic(), requestHeader.getQueueId()));
            }
            popMessageFuture = popSpecifiedQueue(consumerGroupId, clientId, topicId, requestHeader.getQueueId(), filter,
                requestHeader.getMaxMsgNums(), requestHeader.isOrder(), requestHeader.getInvisibleTime(), messageList);
        } else {
            // Shuffle queue list to prevent constantly popping messages from the queue at the front.
            List<Integer> queueList = new ArrayList<>(assignmentQueueSet);
            Collections.shuffle(queueList);

            int requiredSize = requestHeader.getMaxMsgNums();
            popMessageFuture = CompletableFuture.completedFuture(null);

            // Pop message from all queues.
            for (Integer queueId : queueList) {
                popMessageFuture.thenCompose(v -> {
                    // If there are a sufficient number of messages, then skip popping from next queue.
                    int batchSize = requiredSize - messageList.size();
                    if (batchSize <= 0) {
                        return CompletableFuture.completedFuture(null);
                    }

                    return popSpecifiedQueue(consumerGroupId, clientId, topicId, queueId, filter,
                        batchSize, requestHeader.isOrder(), requestHeader.getInvisibleTime(), messageList);
                });
            }
        }

        return popMessageFuture.thenApply(v -> {
            PopStatus status;
            if (messageList.isEmpty()) {
                status = PopStatus.NO_NEW_MSG;
            } else {
                status = PopStatus.FOUND;
            }
            return new PopResult(status, RocketMQMessageUtil.transformMessageExt(messageList));
        });
    }

    @Override
    public CompletableFuture<AckResult> changeInvisibleTime(ProxyContext ctx, ReceiptHandle handle, String messageId,
        ChangeInvisibleTimeRequestHeader requestHeader, long timeoutMillis) {
        return store.changeInvisibleDuration(requestHeader.getExtraInfo(), requestHeader.getInvisibleTime())
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
        return store.ack(requestHeader.getExtraInfo())
            .thenApply(ackResult -> {
                long consumerGroupId = metadataService.queryConsumerGroupId(requestHeader.getConsumerGroup());
                long topicId = metadataService.queryTopicId(requestHeader.getTopic());
                Integer queueId = requestHeader.getQueueId();

                org.apache.rocketmq.client.consumer.AckResult result = new org.apache.rocketmq.client.consumer.AckResult();
                switch (ackResult.status()) {
                    case SUCCESS -> result.setStatus(AckStatus.OK);
                    case ERROR -> result.setStatus(AckStatus.NO_EXIST);
                }

                // Expire the lock later to allow other client preempted when all inflight messages are acked.
                if (store.getInflightStats(consumerGroupId, topicId, queueId) == 0) {
                    lockService.tryExpire(topicId, queueId, Duration.ofSeconds(1).toMillis());
                }

                return result;
            });
    }

    @Override
    public CompletableFuture<PullResult> pullMessage(ProxyContext ctx, AddressableMessageQueue messageQueue,
        PullMessageRequestHeader requestHeader, long timeoutMillis) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<Long> queryConsumerOffset(ProxyContext ctx, AddressableMessageQueue messageQueue,
        QueryConsumerOffsetRequestHeader requestHeader, long timeoutMillis) {
        long consumeGroupId = metadataService.queryConsumerGroupId(requestHeader.getConsumerGroup());
        long topicId = metadataService.queryTopicId(requestHeader.getTopic());
        return CompletableFuture.completedFuture(metadataService.queryConsumerOffset(consumeGroupId, topicId, requestHeader.getQueueId()));
    }

    @Override
    public CompletableFuture<Void> updateConsumerOffset(ProxyContext ctx, AddressableMessageQueue messageQueue,
        UpdateConsumerOffsetRequestHeader requestHeader, long timeoutMillis) {
        long consumeGroupId = metadataService.queryConsumerGroupId(requestHeader.getConsumerGroup());
        long topicId = metadataService.queryTopicId(requestHeader.getTopic());
        // TODO: support retry topic.
        metadataService.updateConsumerOffset(consumeGroupId, topicId, requestHeader.getQueueId(), requestHeader.getCommitOffset(), false);
        return CompletableFuture.completedFuture(null);
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
}
