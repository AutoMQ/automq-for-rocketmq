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

import com.automq.rocketmq.proxy.model.ProxyContextExt;
import com.automq.rocketmq.proxy.remoting.RemotingUtil;
import com.google.common.base.Strings;
import io.netty.channel.ChannelHandlerContext;
import java.util.Collections;
import java.util.Map;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.topic.TopicValidator;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.processor.MessagingProcessor;
import org.apache.rocketmq.proxy.processor.QueueSelector;
import org.apache.rocketmq.proxy.remoting.activity.SendMessageActivity;
import org.apache.rocketmq.proxy.remoting.pipeline.RequestPipeline;
import org.apache.rocketmq.proxy.service.route.AddressableMessageQueue;
import org.apache.rocketmq.proxy.service.route.MessageQueueView;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RequestCode;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.apache.rocketmq.remoting.protocol.header.SendMessageRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.SendMessageResponseHeader;

import static com.automq.rocketmq.proxy.remoting.RemotingUtil.REQUEST_NOT_FINISHED;

public class ExtendSendMessageActivity extends SendMessageActivity {
    public ExtendSendMessageActivity(RequestPipeline requestPipeline,
        MessagingProcessor messagingProcessor) {
        super(requestPipeline, messagingProcessor);
    }

    @Override
    protected RemotingCommand request(ChannelHandlerContext ctx, RemotingCommand request, ProxyContext context,
        long timeoutMillis) throws Exception {
        String dstBrokerName = dstBrokerName(request);
        if (Strings.isNullOrEmpty(dstBrokerName)) {
            return RemotingCommand.buildErrorResponse(ResponseCode.VERSION_NOT_SUPPORTED,
                "Request doesn't have field bname");
        }

        if (request.getCode() == RequestCode.CONSUMER_SEND_MSG_BACK) {
            return RemotingUtil.notSupportedResponse(request);
        }

        SendMessageRequestHeader requestHeader = SendMessageRequestHeader.parseRequestHeader(request);
        if (requestHeader == null) {
            // This is the default behavior of the apache rocketmq, just respect it.
            return null;
        }

        if (requestHeader.isBatch()) {
            // TODO: Support batch message in the future.
            return RemotingUtil.notSupportedResponse(request);
        }

        final RemotingCommand response = preCheck(ctx, request, requestHeader);

        if (response.getCode() != REQUEST_NOT_FINISHED) {
            return response;
        }

        final byte[] body = request.getBody();
        Message message = new Message(requestHeader.getTopic(), body);
        Map<String, String> oriProps = MessageDecoder.string2messageProperties(requestHeader.getProperties());
        message.setFlag(requestHeader.getFlag());
        MessageAccessor.setProperties(message, oriProps);

        // TODO: Do we need handle more properties here?

        messagingProcessor.sendMessage(context,
            new SendMessageQueueSelector(dstBrokerName, requestHeader),
            // The topic name here is already wrapped with namespace, it's safe to use it as ProducerGroup.
            // TODO: Consider using the producer group from the request header?
            requestHeader.getTopic(),
            requestHeader.getSysFlag(),
            Collections.singletonList(message),
            timeoutMillis).whenComplete((sendResults, throwable) -> {
                if (throwable != null) {
                    writeErrResponse(ctx, context, request, throwable);
                    return;
                }

                // Assert sendResults.size() == 1 since we doesn't support batch message yet.
                // TODO: Support batch message in the future.
                SendResult sendResult = sendResults.get(0);
                fillSendMessageResponse(response, sendResult);
                writeResponse(ctx, context, request, response);
            });

        // Return null to uplevel, the response will be sent back in the future.
        return null;
    }

    @Override
    protected ProxyContext createContext(ChannelHandlerContext ctx, RemotingCommand request) {
        return ProxyContextExt.create(super.createContext(ctx, request));
    }

    private String dstBrokerName(RemotingCommand request) {
        if (request.getCode() == RequestCode.SEND_MESSAGE_V2) {
            return request.getExtFields().get(BROKER_NAME_FIELD_FOR_SEND_MESSAGE_V2);
        } else {
            return request.getExtFields().get(BROKER_NAME_FIELD);
        }
    }

    private RemotingCommand preCheck(ChannelHandlerContext ctx, RemotingCommand request,
        SendMessageRequestHeader requestHeader) {
        final RemotingCommand response = RemotingCommand.createResponseCommand(SendMessageResponseHeader.class);
        response.setOpaque(request.getOpaque());
        response.setCode(REQUEST_NOT_FINISHED);

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
}
