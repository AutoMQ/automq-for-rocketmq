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

package com.automq.rocketmq.proxy.grpc.activity;

import apache.rocketmq.v2.Code;
import apache.rocketmq.v2.Message;
import apache.rocketmq.v2.Resource;
import apache.rocketmq.v2.SendMessageRequest;
import apache.rocketmq.v2.SendMessageResponse;
import com.automq.rocketmq.proxy.model.VirtualQueue;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.grpc.v2.channel.GrpcChannelManager;
import org.apache.rocketmq.proxy.grpc.v2.common.GrpcClientSettingsManager;
import org.apache.rocketmq.proxy.grpc.v2.common.GrpcConverter;
import org.apache.rocketmq.proxy.grpc.v2.common.GrpcProxyException;
import org.apache.rocketmq.proxy.grpc.v2.producer.SendMessageActivity;
import org.apache.rocketmq.proxy.processor.MessagingProcessor;
import org.apache.rocketmq.proxy.processor.QueueSelector;
import org.apache.rocketmq.proxy.service.route.AddressableMessageQueue;
import org.apache.rocketmq.proxy.service.route.MessageQueueView;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExtendSendMessageActivity extends SendMessageActivity {
    public static final Logger LOGGER = LoggerFactory.getLogger(ExtendSendMessageActivity.class);
    public ExtendSendMessageActivity(MessagingProcessor messagingProcessor,
        GrpcClientSettingsManager grpcClientSettingsManager,
        GrpcChannelManager grpcChannelManager) {
        super(messagingProcessor, grpcClientSettingsManager, grpcChannelManager);
    }

    @Override
    public CompletableFuture<SendMessageResponse> sendMessage(ProxyContext ctx, SendMessageRequest request) {
        CompletableFuture<SendMessageResponse> future = new CompletableFuture<>();

        try {
            if (request.getMessagesCount() <= 0) {
                throw new GrpcProxyException(Code.MESSAGE_CORRUPTED, "no message to send");
            }

            List<Message> messageList = request.getMessagesList();
            apache.rocketmq.v2.Message message = messageList.get(0);
            Resource topic = message.getTopic();
            validateTopic(topic);

            future = this.messagingProcessor.sendMessage(
                ctx,
                new SendMessageQUeueSelector(request),
                GrpcConverter.getInstance().wrapResourceWithNamespace(topic),
                buildSysFlag(message),
                buildMessage(ctx, request.getMessagesList(), topic)
            ).thenApply(result -> convertToSendMessageResponse(ctx, request, result));
        } catch (Throwable t) {
            future.completeExceptionally(t);
        }
        return future;
    }

    static class SendMessageQUeueSelector implements QueueSelector {
        private final SendMessageRequest request;
        public SendMessageQUeueSelector(SendMessageRequest request) {
            this.request = request;
        }

        @Override
        public AddressableMessageQueue select(ProxyContext ctx, MessageQueueView messageQueueView) {
            apache.rocketmq.v2.Message message = request.getMessages(0);
            // For gRPC clients, message queue has been selected by the client, just honor it.
            int queueId = message.getSystemProperties().getQueueId();

            for (AddressableMessageQueue messageQueue : messageQueueView.getWriteSelector().getQueues()) {
                VirtualQueue virtualQueue = new VirtualQueue(messageQueue);
                if (virtualQueue.physicalQueueId() == queueId) {
                    return messageQueue;
                }
            }
            LOGGER.error("Failed to find queue {} in message queue view {}", queueId, messageQueueView);
            return null;
        }
    }
}
