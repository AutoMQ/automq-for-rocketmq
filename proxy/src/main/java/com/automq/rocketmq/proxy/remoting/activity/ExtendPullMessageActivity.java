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
import com.automq.rocketmq.proxy.remoting.RemotingUtil;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.FileRegion;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.Tracer;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletionException;
import org.apache.rocketmq.broker.client.ConsumerGroupInfo;
import org.apache.rocketmq.broker.pagecache.ManyMessageTransfer;
import org.apache.rocketmq.client.consumer.PullStatus;
import org.apache.rocketmq.common.attribute.TopicMessageType;
import org.apache.rocketmq.common.help.FAQUrl;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.sysflag.PullSysFlag;
import org.apache.rocketmq.proxy.common.ContextVariable;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.processor.MessagingProcessor;
import org.apache.rocketmq.proxy.remoting.activity.PullMessageActivity;
import org.apache.rocketmq.proxy.remoting.pipeline.RequestPipeline;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.protocol.ForbiddenType;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RequestCode;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.apache.rocketmq.remoting.protocol.filter.FilterAPI;
import org.apache.rocketmq.remoting.protocol.header.PullMessageRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.PullMessageResponseHeader;
import org.apache.rocketmq.remoting.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.remoting.protocol.subscription.SubscriptionGroupConfig;
import org.apache.rocketmq.store.GetMessageResult;
import org.apache.rocketmq.store.SelectMappedBufferResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExtendPullMessageActivity extends PullMessageActivity implements CommonRemotingBehavior {
    public static final Logger LOGGER = LoggerFactory.getLogger(ExtendPullMessageActivity.class);

    public ExtendPullMessageActivity(RequestPipeline requestPipeline,
        MessagingProcessor messagingProcessor) {
        super(requestPipeline, messagingProcessor);
    }

    @Override
    protected RemotingCommand processRequest0(ChannelHandlerContext ctx, RemotingCommand request,
        ProxyContext context) throws Exception {
        Optional<RemotingCommand> response = checkRequiredField(request);
        if (response.isPresent()) {
            return response.get();
        }

        if (request.getCode() == RequestCode.PULL_MESSAGE || request.getCode() == RequestCode.LITE_PULL_MESSAGE) {
            return pullMessage(ctx, request, context);
        }

        // We don't call the parent processRequest0() here, the parent class is very simple, and we can avoid double decoding.
        return RemotingUtil.codeNotSupportedResponse(request);
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

    private RemotingCommand pullMessage(ChannelHandlerContext ctx, RemotingCommand request,
        ProxyContext context) throws RemotingCommandException {
        ProxyContextExt contextExt = (ProxyContextExt) context;
        Tracer tracer = contextExt.tracer().get();
        Span rootSpan = tracer.spanBuilder("PullMessage")
            .setNoParent()
            .setSpanKind(SpanKind.SERVER)
            .setAttribute(ContextVariable.PROTOCOL_TYPE, contextExt.getProtocolType())
            .setAttribute(ContextVariable.ACTION, contextExt.getAction())
            .setAttribute(ContextVariable.CLIENT_ID, contextExt.getClientID())
            .startSpan();
        contextExt.attachSpan(rootSpan);

        // Retrieve the request header.
        final PullMessageRequestHeader requestHeader = (PullMessageRequestHeader)
            request.decodeCommandCustomHeader(PullMessageRequestHeader.class);

        // Build the response.
        final RemotingCommand response = RemotingUtil.buildResponseCommand(request, ResponseCode.SUCCESS,
            PullMessageResponseHeader.class);
        final PullMessageResponseHeader responseHeader = (PullMessageResponseHeader) response.readCustomHeader();

        // Check the topic type.
        TopicMessageType type = messagingProcessor.getMetadataService().getTopicMessageType(context, requestHeader.getTopic());
        if (type == TopicMessageType.UNSPECIFIED) {
            response.setCode(ResponseCode.TOPIC_NOT_EXIST);
            response.setRemark(String.format("topic [%s] does not exist or message type is not specified, %s",
                requestHeader.getTopic(), FAQUrl.suggestTodo(FAQUrl.NO_TOPIC_ROUTE_INFO)));
            return response;
        }

        // Check the subscription group existence.
        SubscriptionGroupConfig groupConfig = messagingProcessor.getSubscriptionGroupConfig(context, requestHeader.getConsumerGroup());
        if (Objects.isNull(groupConfig)) {
            response.setCode(ResponseCode.SUBSCRIPTION_GROUP_NOT_EXIST);
            response.setRemark(String.format("subscription group [%s] does not exist, %s",
                requestHeader.getConsumerGroup(), FAQUrl.suggestTodo(FAQUrl.SUBSCRIPTION_GROUP_NOT_EXIST)));
            return response;
        }

        // Check the read permission.
        if (!groupConfig.isConsumeEnable()) {
            response.setCode(ResponseCode.NO_PERMISSION);
            responseHeader.setForbiddenType(ForbiddenType.GROUP_FORBIDDEN);
            response.setRemark("subscription group no permission, " + requestHeader.getConsumerGroup());
            return response;
        }

        String brokerName = dstBrokerName(request);
        assert brokerName != null;
        MessageQueue messageQueue = new MessageQueue(requestHeader.getTopic(), brokerName, requestHeader.getQueueId());

        SubscriptionData subscriptionData;
        final boolean hasSubscriptionFlag = PullSysFlag.hasSubscriptionFlag(requestHeader.getSysFlag());

        if (hasSubscriptionFlag) {
            try {
                subscriptionData = FilterAPI.build(
                    requestHeader.getTopic(), requestHeader.getSubscription(), requestHeader.getExpressionType()
                );
            } catch (Exception e) {
                LOGGER.warn("Parse the consumer's subscription[{}] failed, group: {}", requestHeader.getSubscription(),
                    requestHeader.getConsumerGroup());
                response.setCode(ResponseCode.SUBSCRIPTION_PARSE_FAILED);
                response.setRemark("parse the consumer's subscription failed");
                return response;
            }
        } else {
            ConsumerGroupInfo consumerGroupInfo = messagingProcessor.getConsumerGroupInfo(requestHeader.getConsumerGroup());
            if (null == consumerGroupInfo) {
                LOGGER.warn("the consumer's group info not exist, group: {}", requestHeader.getConsumerGroup());
                response.setCode(ResponseCode.SUBSCRIPTION_NOT_EXIST);
                response.setRemark("the consumer's group info not exist" + FAQUrl.suggestTodo(FAQUrl.SAME_GROUP_DIFFERENT_TOPIC));
                return response;
            }

            subscriptionData = consumerGroupInfo.findSubscriptionData(requestHeader.getTopic());
            if (null == subscriptionData) {
                LOGGER.warn("the consumer's subscription not exist, group: {}, topic:{}", requestHeader.getConsumerGroup(), requestHeader.getTopic());
                response.setCode(ResponseCode.SUBSCRIPTION_NOT_EXIST);
                response.setRemark("the consumer's subscription not exist" + FAQUrl.suggestTodo(FAQUrl.SAME_GROUP_DIFFERENT_TOPIC));
                return response;
            }

            if (subscriptionData.getSubVersion() < requestHeader.getSubVersion()) {
                LOGGER.warn("The broker's subscription is not latest, group: {} {}", requestHeader.getConsumerGroup(),
                    subscriptionData.getSubString());
                response.setCode(ResponseCode.SUBSCRIPTION_NOT_LATEST);
                response.setRemark("the consumer's subscription not latest");
                return response;
            }
        }

        messagingProcessor.pullMessage(
                context,
                messageQueue,
                requestHeader.getConsumerGroup(),
                requestHeader.getQueueOffset(),
                requestHeader.getMaxMsgNums(),
                requestHeader.getSysFlag(),
                requestHeader.getCommitOffset(),
                requestHeader.getSuspendTimeoutMillis(),
                subscriptionData,
                context.getRemainingMs())
            .whenComplete((pullResult, throwable) -> {
                if (throwable != null) {
                    writeErrResponse(ctx, context, request, throwable);
                    return;
                }

                responseHeader.setNextBeginOffset(pullResult.getNextBeginOffset());
                responseHeader.setMinOffset(pullResult.getMinOffset());
                responseHeader.setMaxOffset(pullResult.getMaxOffset());
                responseHeader.setSuggestWhichBrokerId(0L);
                responseHeader.setTopicSysFlag(0);
                responseHeader.setGroupSysFlag(0);

                switch (pullResult.getPullStatus()) {
                    case FOUND -> response.setCode(ResponseCode.SUCCESS);
                    case NO_MATCHED_MSG -> response.setCode(ResponseCode.PULL_RETRY_IMMEDIATELY);
                    case NO_NEW_MSG -> response.setCode(ResponseCode.PULL_NOT_FOUND);
                    case OFFSET_ILLEGAL -> {
                        LOGGER.warn("the pull request offset illegal, commitOffset: {}, requestOffset: {}, messageQueue: {}",
                            requestHeader.getCommitOffset(), requestHeader.getQueueOffset(), messageQueue);
                        response.setCode(ResponseCode.PULL_OFFSET_MOVED);
                    }
                }

                if (pullResult.getPullStatus() == PullStatus.FOUND) {
                    List<MessageExt> msgList = pullResult.getMsgFoundList();

                    GetMessageResult getMessageResult = new GetMessageResult();
                    for (MessageExt messageExt : msgList) {
                        byte[] payload;
                        try {
                            payload = MessageDecoder.encode(messageExt, false);
                        } catch (Exception e) {
                            // TODO: Rewrite the response for this case.
                            throw new CompletionException(e);
                        }
                        getMessageResult.addMessage(new SelectMappedBufferResult(0, ByteBuffer.wrap(payload), payload.length, null));
                    }
                    FileRegion fileRegion =
                        new ManyMessageTransfer(response.encodeHeader(getMessageResult.getBufferTotalSize()), getMessageResult);
                    ctx.writeAndFlush(fileRegion)
                        .addListener(future -> {
                            recordRpcLatency(context, response);
                            rootSpan.setAttribute("code", RemotingHelper.getResponseCodeDesc(response.getCode()));
                            TraceHelper.endSpan(contextExt, rootSpan, future.cause());
                            if (!future.isSuccess()) {
                                LOGGER.error("Write pull message response failed", future.cause());
                            }
                        });
                    return;
                }
                writeResponse(ctx, context, request, response, null);
            });

        return null;
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
