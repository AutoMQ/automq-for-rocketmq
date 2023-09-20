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

import com.automq.rocketmq.metadata.ProxyMetadataService;
import com.automq.rocketmq.store.MessageStore;
import com.automq.rocketmq.store.model.message.PutResult;
import com.automq.rocketmq.store.util.MessageUtil;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.apache.rocketmq.client.consumer.AckStatus;
import org.apache.rocketmq.client.consumer.PopResult;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.common.consumer.ReceiptHandle;
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
    private ProxyMetadataService metadataService;
    private MessageStore store;

    public MessageServiceImpl(ProxyMetadataService metadataService, MessageStore store) {
        this.metadataService = metadataService;
        this.store = store;
    }

    @Override
    public CompletableFuture<List<SendResult>> sendMessage(ProxyContext context, AddressableMessageQueue queue,
        List<org.apache.rocketmq.common.message.Message> list, SendMessageRequestHeader header, long l) {
        long topicId = metadataService.queryTopicId(header.getTopic());

        List<CompletableFuture<PutResult>> completableFutureList = list.stream()
            .map(message -> MessageUtil.transferToMessage(topicId, header.getQueueId(), message.getTags(),
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
                    result.setMessageQueue(new MessageQueue(header.getTopic(), header.getBname(), header.getQueueId()));
                    result.setQueueOffset(putResult.offset());
                    return result;
                }).toList());
    }

    @Override
    public CompletableFuture<RemotingCommand> sendMessageBack(ProxyContext context, ReceiptHandle handle, String s,
        ConsumerSendMsgBackRequestHeader header, long l) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<Void> endTransactionOneway(ProxyContext context, String s,
        EndTransactionRequestHeader header, long l) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<PopResult> popMessage(ProxyContext context, AddressableMessageQueue queue,
        PopMessageRequestHeader header, long l) {
        return null;
    }

    @Override
    public CompletableFuture<org.apache.rocketmq.client.consumer.AckResult> changeInvisibleTime(ProxyContext context,
        ReceiptHandle handle, String s, ChangeInvisibleTimeRequestHeader header, long l) {
        return store.changeInvisibleDuration(header.getExtraInfo(), header.getInvisibleTime())
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
    public CompletableFuture<org.apache.rocketmq.client.consumer.AckResult> ackMessage(ProxyContext context,
        ReceiptHandle handle, String s, AckMessageRequestHeader header, long l) {
        return store.ack(header.getExtraInfo())
            .thenApply(ackResult -> {
                org.apache.rocketmq.client.consumer.AckResult result = new org.apache.rocketmq.client.consumer.AckResult();
                switch (ackResult.status()) {
                    case SUCCESS -> result.setStatus(AckStatus.OK);
                    case ERROR -> result.setStatus(AckStatus.NO_EXIST);
                }
                return result;
            });
    }

    @Override
    public CompletableFuture<PullResult> pullMessage(ProxyContext context, AddressableMessageQueue queue,
        PullMessageRequestHeader header, long l) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<Long> queryConsumerOffset(ProxyContext context, AddressableMessageQueue queue,
        QueryConsumerOffsetRequestHeader header, long l) {
        long consumeGroupId = metadataService.queryConsumeGroupId(header.getConsumerGroup());
        long topicId = metadataService.queryTopicId(header.getTopic());
        return CompletableFuture.completedFuture(metadataService.queryConsumerOffset(consumeGroupId, topicId, header.getQueueId()));
    }

    @Override
    public CompletableFuture<Void> updateConsumerOffset(ProxyContext context, AddressableMessageQueue queue,
        UpdateConsumerOffsetRequestHeader header, long l) {
        long consumeGroupId = metadataService.queryConsumeGroupId(header.getConsumerGroup());
        long topicId = metadataService.queryTopicId(header.getTopic());
        metadataService.updateConsumerOffset(consumeGroupId, topicId, header.getQueueId(), header.getCommitOffset());
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Set<MessageQueue>> lockBatchMQ(ProxyContext context, AddressableMessageQueue queue,
        LockBatchRequestBody body, long l) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<Void> unlockBatchMQ(ProxyContext context, AddressableMessageQueue queue,
        UnlockBatchRequestBody body, long l) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<Long> getMaxOffset(ProxyContext context, AddressableMessageQueue queue,
        GetMaxOffsetRequestHeader header, long l) {
        return null;
    }

    @Override
    public CompletableFuture<Long> getMinOffset(ProxyContext context, AddressableMessageQueue queue,
        GetMinOffsetRequestHeader header, long l) {
        return null;
    }

    @Override
    public CompletableFuture<RemotingCommand> request(ProxyContext context, String s, RemotingCommand command, long l) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<Void> requestOneway(ProxyContext context, String s, RemotingCommand command, long l) {
        throw new UnsupportedOperationException();
    }
}
