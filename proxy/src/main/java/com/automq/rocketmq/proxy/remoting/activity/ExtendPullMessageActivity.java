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

package com.automq.rocketmq.proxy.remoting.activity;

import com.automq.rocketmq.proxy.remoting.RemotingUtil;
import io.netty.channel.ChannelHandlerContext;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import org.apache.rocketmq.broker.client.ConsumerGroupInfo;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.client.consumer.PullStatus;
import org.apache.rocketmq.common.help.FAQUrl;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.sysflag.PullSysFlag;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.processor.MessagingProcessor;
import org.apache.rocketmq.proxy.remoting.activity.PullMessageActivity;
import org.apache.rocketmq.proxy.remoting.pipeline.RequestPipeline;
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
        RemotingCommand response = checkVersion(request);
        if (response != null) {
            return response;
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
        super.writeResponse(ctx, context, request, response, t);
    }

    private RemotingCommand pullMessage(ChannelHandlerContext ctx, RemotingCommand request, ProxyContext context) throws RemotingCommandException {
        // Retrieve the request header.
        final PullMessageRequestHeader requestHeader = (PullMessageRequestHeader)
            request.decodeCommandCustomHeader(PullMessageRequestHeader.class);

        // Build the response.
        final RemotingCommand response = RemotingUtil.buildResponseCommand(request, ResponseCode.SUCCESS,
            PullMessageResponseHeader.class);
        final PullMessageResponseHeader responseHeader = (PullMessageResponseHeader) response.readCustomHeader();

        SubscriptionGroupConfig groupConfig = messagingProcessor.getSubscriptionGroupConfig(context, requestHeader.getConsumerGroup());

        // Check the topic existence.

        // Check the subscription group existence.
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

        // TODO: Check the read permission of the topic.

        String brokerName = dstBrokerName(request);
        assert brokerName != null;
        MessageQueue messageQueue = new MessageQueue(requestHeader.getTopic(), brokerName, requestHeader.getQueueId());

        SubscriptionData subscriptionData = null;
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

        CompletableFuture<PullResult> pullCf = messagingProcessor.pullMessage(
            context,
            messageQueue,
            requestHeader.getConsumerGroup(),
            requestHeader.getQueueOffset(),
            requestHeader.getMaxMsgNums(),
            requestHeader.getSysFlag(),
            requestHeader.getCommitOffset(),
            requestHeader.getSuspendTimeoutMillis(),
            subscriptionData,
            context.getRemainingMs());

        pullCf.whenComplete((pullResult, throwable) -> {
            if (throwable != null) {
                writeErrResponse(ctx, context, request, throwable);
                return;
            }

            responseHeader.setNextBeginOffset(pullResult.getNextBeginOffset());
            responseHeader.setMinOffset(pullResult.getMinOffset());
            responseHeader.setMaxOffset(pullResult.getMaxOffset());

            switch (pullResult.getPullStatus()) {
                case FOUND -> response.setCode(ResponseCode.SUCCESS);
                case NO_MATCHED_MSG -> response.setCode(ResponseCode.PULL_RETRY_IMMEDIATELY);
                // TODO: For this code, we need suspend the request for long polling.
                case NO_NEW_MSG -> response.setCode(ResponseCode.PULL_NOT_FOUND);
                case OFFSET_ILLEGAL -> {
                    LOGGER.warn("the pull request offset illegal, commitOffset: {}, requestOffset: {}, messageQueue: {}",
                        requestHeader.getCommitOffset(), requestHeader.getQueueOffset(), messageQueue);
                    response.setCode(ResponseCode.PULL_OFFSET_MOVED);
                }
            }

            if (pullResult.getPullStatus() == PullStatus.FOUND) {
                List<MessageExt> msgList = pullResult.getMsgFoundList();
                // Encode all the messages
                List<ByteBuffer> buffers = new ArrayList<>();
                for (MessageExt msg : msgList) {
                    msg.setBody(null);
                    try {
                        byte[] payload = MessageDecoder.encode(msg, false);
                        buffers.add(ByteBuffer.wrap(payload));
                    } catch (Exception e) {
                        // TODO: Rewrite the response for this case.
                        throw new CompletionException(e);
                    }
                }

                // Merge all the messages into one buffer.
                // TODO: Encode the message list in a batch way.
                ByteBuffer mergedBuffer = ByteBuffer.allocate(buffers.stream().mapToInt(ByteBuffer::remaining).sum());
                for (ByteBuffer buffer : buffers) {
                    mergedBuffer.put(buffer);
                }

                response.setBody(mergedBuffer.array());
            }
            writeResponse(ctx, context, request, response);
        });

        return null;
    }
}
