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

package com.automq.rocketmq.proxy.grpc.activity;

import apache.rocketmq.v2.Code;
import apache.rocketmq.v2.Message;
import apache.rocketmq.v2.Resource;
import apache.rocketmq.v2.SendMessageRequest;
import apache.rocketmq.v2.SendMessageResponse;
import com.automq.rocketmq.common.trace.TraceHelper;
import com.automq.rocketmq.proxy.model.ProxyContextExt;
import com.automq.rocketmq.proxy.model.VirtualQueue;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.Tracer;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.rocketmq.proxy.common.ContextVariable;
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

        ProxyContextExt contextExt = (ProxyContextExt) ctx;
        Tracer tracer = contextExt.tracer().get();
        Span rootSpan = tracer.spanBuilder("SendMessage")
            .setNoParent()
            .setSpanKind(SpanKind.SERVER)
            .setAttribute(ContextVariable.PROTOCOL_TYPE, ctx.getProtocolType())
            .setAttribute(ContextVariable.ACTION, ctx.getAction())
            .setAttribute(ContextVariable.CLIENT_ID, ctx.getClientID())
            .startSpan();
        contextExt.attachSpan(rootSpan);

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
                    new SendMessageQueueSelector(request),
                    GrpcConverter.getInstance().wrapResourceWithNamespace(topic),
                    buildSysFlag(message),
                    buildMessage(ctx, request.getMessagesList(), topic)
                ).thenApply(result -> convertToSendMessageResponse(ctx, request, result))
                .whenComplete((response, throwable) -> {
                    if (response != null) {
                        rootSpan.setAttribute("code", response.getStatus().getCode().name().toLowerCase());
                    }
                    TraceHelper.endSpan(contextExt, rootSpan, throwable);
                });
        } catch (Throwable t) {
            TraceHelper.endSpan(contextExt, rootSpan, t);
            future.completeExceptionally(t);
        }
        return future;
    }

    static class SendMessageQueueSelector implements QueueSelector {
        private final SendMessageRequest request;

        public SendMessageQueueSelector(SendMessageRequest request) {
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
