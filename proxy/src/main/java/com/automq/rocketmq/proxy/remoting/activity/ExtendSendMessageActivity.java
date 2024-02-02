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

package com.automq.rocketmq.proxy.remoting.activity;

import com.automq.rocketmq.common.trace.TraceHelper;
import com.automq.rocketmq.proxy.exception.ExceptionHandler;
import com.automq.rocketmq.proxy.model.ProxyContextExt;
import com.automq.rocketmq.proxy.processor.ExtendMessagingProcessor;
import com.automq.rocketmq.proxy.remoting.RemotingUtil;
import io.netty.channel.ChannelHandlerContext;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.Tracer;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.topic.TopicValidator;
import org.apache.rocketmq.proxy.common.ContextVariable;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.processor.QueueSelector;
import org.apache.rocketmq.proxy.remoting.activity.SendMessageActivity;
import org.apache.rocketmq.proxy.remoting.pipeline.RequestPipeline;
import org.apache.rocketmq.proxy.service.route.AddressableMessageQueue;
import org.apache.rocketmq.proxy.service.route.MessageQueueView;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RequestCode;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.apache.rocketmq.remoting.protocol.header.ConsumerSendMsgBackRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.SendMessageRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.SendMessageResponseHeader;

import static com.automq.rocketmq.proxy.remoting.RemotingUtil.REQUEST_NOT_FINISHED;

public class ExtendSendMessageActivity extends SendMessageActivity implements CommonRemotingBehavior {
    ExtendMessagingProcessor messagingProcessor;

    public ExtendSendMessageActivity(RequestPipeline requestPipeline,
        ExtendMessagingProcessor messagingProcessor) {
        super(requestPipeline, messagingProcessor);
        this.messagingProcessor = messagingProcessor;
    }

    @Override
    protected RemotingCommand processRequest0(ChannelHandlerContext ctx, RemotingCommand request,
        ProxyContext context) throws Exception {
        Optional<RemotingCommand> response = checkRequiredField(request);
        if (response.isPresent()) {
            return response.get();
        }

        switch (request.getCode()) {
            // The ExtendSendMessageActivity only support the bellow request codes.
            case RequestCode.SEND_MESSAGE, RequestCode.SEND_MESSAGE_V2, RequestCode.SEND_BATCH_MESSAGE, RequestCode.CONSUMER_SEND_MSG_BACK -> {
                return super.processRequest0(ctx, request, context);
            }
        }

        return RemotingUtil.codeNotSupportedResponse(request);
    }

    @Override
    protected RemotingCommand sendMessage(ChannelHandlerContext ctx, RemotingCommand request,
        ProxyContext context) throws Exception {
        ProxyContextExt contextExt = (ProxyContextExt) context;
        Tracer tracer = contextExt.tracer().get();
        Span rootSpan = tracer.spanBuilder("SendMessage")
            .setNoParent()
            .setSpanKind(SpanKind.SERVER)
            .setAttribute(ContextVariable.PROTOCOL_TYPE, contextExt.getProtocolType())
            .setAttribute(ContextVariable.ACTION, contextExt.getAction())
            .setAttribute(ContextVariable.CLIENT_ID, contextExt.getClientID())
            .startSpan();
        contextExt.attachSpan(rootSpan);

        // The parent class already checked the message type, added the transaction subscription.
        RemotingCommand superResponse = super.sendMessage(ctx, request, context);
        if (superResponse != null) {
            return superResponse;
        }

        String dstBrokerName = dstBrokerName(request);
        // Assert dstBrokerName != null since we have already checked the version.
        assert dstBrokerName != null;

        SendMessageRequestHeader requestHeader = SendMessageRequestHeader.parseRequestHeader(request);
        if (requestHeader == null) {
            // This is the default behavior of the apache rocketmq, just respect it.
            return null;
        }

        if (requestHeader.isBatch()) {
            // TODO: Support batch message in the future.
            return RemotingUtil.codeNotSupportedResponse(request);
        }

        // TODO: Support RETRY and DLQ message in the future.
        // Note that the client will send retry and dlq messages through the SEND_MESSAGE RPC.

        final RemotingCommand response = preCheck(ctx, request, requestHeader);

        if (response.getCode() != REQUEST_NOT_FINISHED) {
            return response;
        }

        final byte[] body = request.getBody();
        Message message = new Message(requestHeader.getTopic(), body);
        Map<String, String> originProperties = MessageDecoder.string2messageProperties(requestHeader.getProperties());
        message.setFlag(requestHeader.getFlag());
        MessageAccessor.setProperties(message, originProperties);
        String bornHost = "";
        try {
            bornHost = ((InetSocketAddress) ctx.channel().remoteAddress()).getAddress().getHostAddress();
        } catch (Exception ignore) {
        }
        message.getProperties().put(MessageConst.PROPERTY_BORN_HOST, bornHost);

        messagingProcessor.sendMessage(context,
                new SendMessageQueueSelector(dstBrokerName, requestHeader),
                // For v4 remoting protocol, we honor the producer group in the request header.
                requestHeader.getProducerGroup(),
                requestHeader.getSysFlag(),
                Collections.singletonList(message),
                context.getRemainingMs())
            .whenComplete((sendResults, throwable) -> {
                if (throwable != null) {
                    writeErrResponse(ctx, context, request, throwable);
                    return;
                }

                // Assert sendResults.size() == 1 since we doesn't support batch message yet.
                // TODO: Support batch message in the future.
                SendResult sendResult = sendResults.get(0);
                fillSendMessageResponse(response, sendResult);
                writeResponse(ctx, context, request, response, null);
            });

        // Return null to uplevel, the response will be sent back in the future.
        return null;
    }

    @Override
    protected RemotingCommand consumerSendMessage(ChannelHandlerContext ctx, RemotingCommand request,
        ProxyContext context) throws Exception {
        ProxyContextExt contextExt = (ProxyContextExt) context;
        Tracer tracer = contextExt.tracer().get();
        Span rootSpan = tracer.spanBuilder("ConsumerSendMessage")
            .setNoParent()
            .setSpanKind(SpanKind.SERVER)
            .setAttribute(ContextVariable.PROTOCOL_TYPE, contextExt.getProtocolType())
            .setAttribute(ContextVariable.ACTION, contextExt.getAction())
            .setAttribute(ContextVariable.CLIENT_ID, contextExt.getClientID())
            .startSpan();
        contextExt.attachSpan(rootSpan);

        ConsumerSendMsgBackRequestHeader requestHeader = (ConsumerSendMsgBackRequestHeader) request.decodeCommandCustomHeader(ConsumerSendMsgBackRequestHeader.class);
        messagingProcessor.getServiceManager()
            .getMessageService()
            .sendMessageBack(context, null, requestHeader.getOriginMsgId(), requestHeader, context.getRemainingMs())
            .whenComplete((finalResponse, error) -> {
                if (error != null) {
                    writeErrResponse(ctx, context, request, error);
                    return;
                }
                writeResponse(ctx, context, request, finalResponse, null);
            });

        return null;
    }

    @Override
    protected RemotingCommand request(ChannelHandlerContext ctx, RemotingCommand request, ProxyContext context,
        long timeoutMillis) throws Exception {
        // The parent class use this method to proxy the request to the broker.
        // We disable this behavior here.
        return null;
    }

    @Override
    protected ProxyContext createContext(ChannelHandlerContext ctx, RemotingCommand request) {
        return createExtendContext(super.createContext(ctx, request));
    }

    @Override
    protected void writeResponse(ChannelHandlerContext ctx, ProxyContext context, RemotingCommand request,
        RemotingCommand response, Throwable t) {
        recordRpcLatency(context, response);
        ProxyContextExt contextExt = (ProxyContextExt) context;
        Span rootSpan = contextExt.rootSpan();
        rootSpan.setAttribute("code", RemotingHelper.getResponseCodeDesc(response.getCode()));
        TraceHelper.endSpan(contextExt, rootSpan, t);
        super.writeResponse(ctx, context, request, response, t);
    }

    private RemotingCommand preCheck(ChannelHandlerContext ctx, RemotingCommand request,
        SendMessageRequestHeader requestHeader) {
        final RemotingCommand response = RemotingUtil.buildResponseCommand(
            request,
            REQUEST_NOT_FINISHED,
            SendMessageResponseHeader.class);

        // Consider moving the validation logic to the upstream.
        TopicValidator.ValidateTopicResult result = TopicValidator.validateTopic(requestHeader.getTopic());
        if (!result.isValid()) {
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark(result.getRemark());
            return response;
        }
        if (TopicValidator.isNotAllowedSendTopic(requestHeader.getTopic())) {
            response.setCode(ResponseCode.NO_PERMISSION);
            response.setRemark("Sending message to topic[" + requestHeader.getTopic() + "] is forbidden.");
            return response;
        }

        return response;
    }

    private void fillSendMessageResponse(RemotingCommand response, SendResult result) {
        final SendMessageResponseHeader responseHeader = (SendMessageResponseHeader) response.readCustomHeader();
        switch (result.getSendStatus()) {
            case SEND_OK -> {
                response.setCode(ResponseCode.SUCCESS);
                responseHeader.setMsgId(result.getMsgId());
                responseHeader.setQueueId(result.getMessageQueue().getQueueId());
                responseHeader.setQueueOffset(result.getQueueOffset());
                responseHeader.setTransactionId(result.getTransactionId());
            }
            case FLUSH_DISK_TIMEOUT -> response.setCode(ResponseCode.FLUSH_DISK_TIMEOUT);
            case FLUSH_SLAVE_TIMEOUT -> response.setCode(ResponseCode.FLUSH_SLAVE_TIMEOUT);
            case SLAVE_NOT_AVAILABLE -> response.setCode(ResponseCode.SLAVE_NOT_AVAILABLE);
            default -> response.setCode(ResponseCode.SYSTEM_ERROR);
        }
    }

    static class SendMessageQueueSelector implements QueueSelector {
        // The physical queue id and topic id have already been encoded in the broker name.
        private final String brokerName;
        private final SendMessageRequestHeader requestHeader;

        SendMessageQueueSelector(String bName, SendMessageRequestHeader header) {
            brokerName = bName;
            requestHeader = header;
        }

        @Override
        public AddressableMessageQueue select(ProxyContext ctx, MessageQueueView messageQueueView) {
            MessageQueue messageQueue = new MessageQueue(requestHeader.getTopic(), brokerName, requestHeader.getQueueId());
            return new AddressableMessageQueue(messageQueue, null);
        }
    }

    @Override
    protected void writeErrResponse(ChannelHandlerContext ctx, ProxyContext context, RemotingCommand request,
        Throwable t) {
        Optional<RemotingCommand> response = ExceptionHandler.convertToRemotingResponse(t);
        if (response.isPresent()) {
            writeResponse(ctx, context, request, response.get(), t);
            return;
        }

        super.writeErrResponse(ctx, context, request, t);
    }
}
