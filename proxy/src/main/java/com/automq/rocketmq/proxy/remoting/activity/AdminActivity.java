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

import apache.rocketmq.controller.v1.AcceptTypes;
import apache.rocketmq.controller.v1.Code;
import apache.rocketmq.controller.v1.CreateTopicRequest;
import apache.rocketmq.controller.v1.MessageType;
import apache.rocketmq.controller.v1.Topic;
import apache.rocketmq.controller.v1.UpdateTopicRequest;
import com.automq.rocketmq.controller.exception.ControllerException;
import com.automq.rocketmq.controller.metadata.MetadataStore;
import com.automq.rocketmq.proxy.remoting.RemotingUtil;
import com.google.common.base.Strings;
import io.netty.channel.ChannelHandlerContext;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ThreadPoolExecutor;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.common.attribute.AttributeParser;
import org.apache.rocketmq.common.attribute.TopicMessageType;
import org.apache.rocketmq.common.topic.TopicValidator;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.common.utils.ExceptionUtils;
import org.apache.rocketmq.proxy.processor.MessagingProcessor;
import org.apache.rocketmq.proxy.remoting.activity.AbstractRemotingActivity;
import org.apache.rocketmq.proxy.remoting.pipeline.RequestPipeline;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.apache.rocketmq.remoting.protocol.header.CreateTopicRequestHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.rocketmq.common.TopicAttributes.TOPIC_MESSAGE_TYPE_ATTRIBUTE;
import static org.apache.rocketmq.remoting.protocol.RequestCode.UPDATE_AND_CREATE_SUBSCRIPTIONGROUP;
import static org.apache.rocketmq.remoting.protocol.RequestCode.UPDATE_AND_CREATE_TOPIC;

public class AdminActivity extends AbstractRemotingActivity implements CommonRemotingBehavior {
    public static final Logger LOGGER = LoggerFactory.getLogger(AdminActivity.class);
    private final MetadataStore metadataStore;
    public AdminActivity(RequestPipeline requestPipeline,
        MessagingProcessor messagingProcessor, MetadataStore metadataStore) {
        super(requestPipeline, messagingProcessor);
        this.metadataStore = metadataStore;
    }

    @Override
    protected RemotingCommand processRequest0(ChannelHandlerContext ctx, RemotingCommand request,
        ProxyContext context) throws Exception {
        return switch (request.getCode()) {
            case UPDATE_AND_CREATE_TOPIC -> updateAndCreateTopic(ctx, request, context);
            case UPDATE_AND_CREATE_SUBSCRIPTIONGROUP -> updateAndCreateSubscriptionGroup(ctx, request, context);
            default -> RemotingUtil.codeNotSupportedResponse(request);
        };
    }

    private RemotingCommand updateAndCreateSubscriptionGroup(ChannelHandlerContext ctx, RemotingCommand request,
        ProxyContext context) {
        return null;
    }

    private RemotingCommand updateAndCreateTopic(ChannelHandlerContext ctx, RemotingCommand request,
        ProxyContext context) throws RemotingCommandException {
        RemotingCommand response = RemotingUtil.buildResponseCommand(request, ResponseCode.SUCCESS);

        final CreateTopicRequestHeader requestHeader =
            (CreateTopicRequestHeader) request.decodeCommandCustomHeader(CreateTopicRequestHeader.class);

        LOGGER.info("Create or update topic {} by {} with {}", requestHeader.getTopic(), context.getRemoteAddress(),
            requestHeader);

        String topicName = requestHeader.getTopic();

        TopicValidator.ValidateTopicResult result = TopicValidator.validateTopic(topicName);
        if (!result.isValid()) {
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark(result.getRemark());
            return response;
        }

        if (TopicValidator.isSystemTopic(topicName)) {
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark("The topic[" + topicName + "] conflict with system topic.");
            return response;
        }

        int queueNums = Math.max(requestHeader.getReadQueueNums(), requestHeader.getWriteQueueNums());
        Map<String, String> attributes = AttributeParser.parseToMap(requestHeader.getAttributes());

        // Default to normal message type.
        TopicMessageType messageType = TopicMessageType.NORMAL;

        String typeVal = attributes.get(TOPIC_MESSAGE_TYPE_ATTRIBUTE.getName());
        if (!Strings.isNullOrEmpty(typeVal)) {
            messageType = TopicMessageType.valueOf(typeVal);
        }

        AcceptTypes.Builder builder = AcceptTypes.newBuilder();
        switch (messageType) {
            case FIFO -> builder.addTypes(MessageType.FIFO);
            case NORMAL -> builder.addTypes(MessageType.NORMAL);
            case DELAY -> builder.addTypes(MessageType.DELAY);
            case TRANSACTION -> builder.addTypes(MessageType.TRANSACTION);
            // For other types, set to unspecified.
            default -> builder.addTypes(MessageType.MESSAGE_TYPE_UNSPECIFIED);
        }

        CreateTopicRequest topicRequest = CreateTopicRequest
            .newBuilder()
            .setTopic(topicName)
            .setCount(queueNums)
            .setAcceptTypes(builder)
            .build();

        CompletableFuture<Long> topicCf = metadataStore.createTopic(topicRequest);

        // Convert to update topic request if the topic already exists.
        topicCf = topicCf.exceptionallyCompose(ex -> {
            Throwable t = ExceptionUtils.getRealException(ex);
            if (t instanceof ControllerException controllerException) {
                if (controllerException.getErrorCode() == Code.DUPLICATED_VALUE) {
                    return metadataStore.describeTopic(null, topicName).thenCompose(existingTopic -> {
                        UpdateTopicRequest updateRequest = UpdateTopicRequest
                            .newBuilder()
                            .setTopicId(existingTopic.getTopicId())
                            .setCount(queueNums)
                            .setAcceptTypes(builder)
                            .build();
                        return metadataStore.updateTopic(updateRequest).thenApply(Topic::getTopicId);
                    });
                }
            }
            // Rethrow the exception if it's not a duplicated topic error.
            throw new CompletionException(t);
        });

        topicCf.whenComplete((id, ex) -> {
            if (ex != null) {
                LOGGER.error("Failed to create topic {}.", topicName, ex);
                response.setCode(ResponseCode.SYSTEM_ERROR);
                response.setRemark(ex.getMessage());
                return;
            }
            LOGGER.info("Topic {}/{} created or updated by {}", topicName, id, requestHeader);
            writeResponse(ctx, context, request, response);
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
        super.writeResponse(ctx, context, request, response, t);
    }
}
