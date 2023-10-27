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

package com.automq.rocketmq.proxy.remoting;

import com.automq.rocketmq.controller.metadata.MetadataStore;
import com.automq.rocketmq.proxy.remoting.activity.AdminActivity;
import com.automq.rocketmq.proxy.remoting.activity.ExtendConsumerManagerActivity;
import com.automq.rocketmq.proxy.remoting.activity.ExtendPullMessageActivity;
import com.automq.rocketmq.proxy.remoting.activity.ExtendSendMessageActivity;
import io.netty.channel.ChannelHandlerContext;
import java.util.concurrent.ThreadPoolExecutor;
import org.apache.rocketmq.proxy.processor.MessagingProcessor;
import org.apache.rocketmq.proxy.remoting.pipeline.RequestPipeline;
import org.apache.rocketmq.remoting.RemotingServer;
import org.apache.rocketmq.remoting.netty.NettyRequestProcessor;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RequestCode;

public class RemotingProtocolServer extends org.apache.rocketmq.proxy.remoting.RemotingProtocolServer {
    private RequestPipeline requestPipeline;
    private final MetadataStore metadataStore;
    public RemotingProtocolServer(MessagingProcessor messagingProcessor, MetadataStore metadataStore) {
        super(messagingProcessor);
        this.metadataStore = metadataStore;

        // Extend some request code to support more features.
        extendRequestCode();

        // Disable some features.
        narrowRequestCode();

        // Replace some request code to use our own implementation.
        replaceRequestCode();
    }

    /**
     * S3RocketMQ will override some implementation of the remoting activities, replace them here.
     */
    private void replaceRequestCode() {
        RemotingServer remotingServer = this.defaultRemotingServer;
        ExtendSendMessageActivity sendMessageActivity = new ExtendSendMessageActivity(requestPipeline, messagingProcessor);
        remotingServer.registerProcessor(RequestCode.SEND_MESSAGE, sendMessageActivity, this.defaultExecutor);
        remotingServer.registerProcessor(RequestCode.SEND_MESSAGE_V2, sendMessageActivity, this.sendMessageExecutor);
        remotingServer.registerProcessor(RequestCode.SEND_BATCH_MESSAGE, sendMessageActivity, this.sendMessageExecutor);
        remotingServer.registerProcessor(RequestCode.CONSUMER_SEND_MSG_BACK, sendMessageActivity, sendMessageExecutor);

        ExtendConsumerManagerActivity consumerManagerActivity = new ExtendConsumerManagerActivity(requestPipeline, messagingProcessor);
        remotingServer.registerProcessor(RequestCode.UPDATE_CONSUMER_OFFSET, consumerManagerActivity, this.updateOffsetExecutor);
        remotingServer.registerProcessor(RequestCode.ACK_MESSAGE, consumerManagerActivity, this.updateOffsetExecutor);
        remotingServer.registerProcessor(RequestCode.CHANGE_MESSAGE_INVISIBLETIME, consumerManagerActivity, this.updateOffsetExecutor);
        remotingServer.registerProcessor(RequestCode.GET_CONSUMER_CONNECTION_LIST, consumerManagerActivity, this.updateOffsetExecutor);

        remotingServer.registerProcessor(RequestCode.GET_CONSUMER_LIST_BY_GROUP, consumerManagerActivity, this.defaultExecutor);
        remotingServer.registerProcessor(RequestCode.GET_MAX_OFFSET, consumerManagerActivity, this.defaultExecutor);
        remotingServer.registerProcessor(RequestCode.GET_MIN_OFFSET, consumerManagerActivity, this.defaultExecutor);
        remotingServer.registerProcessor(RequestCode.QUERY_CONSUMER_OFFSET, consumerManagerActivity, this.defaultExecutor);
        remotingServer.registerProcessor(RequestCode.SEARCH_OFFSET_BY_TIMESTAMP, consumerManagerActivity, this.defaultExecutor);
        remotingServer.registerProcessor(RequestCode.LOCK_BATCH_MQ, consumerManagerActivity, this.defaultExecutor);
        remotingServer.registerProcessor(RequestCode.UNLOCK_BATCH_MQ, consumerManagerActivity, this.defaultExecutor);

        ExtendPullMessageActivity pullMessageActivity = new ExtendPullMessageActivity(requestPipeline, messagingProcessor);
        remotingServer.registerProcessor(RequestCode.PULL_MESSAGE, pullMessageActivity, this.pullMessageExecutor);
        remotingServer.registerProcessor(RequestCode.LITE_PULL_MESSAGE, pullMessageActivity, this.pullMessageExecutor);
    }

    /**
     * Narrow the request code to disable some features, like PopMessageRequest.
     */
    private void narrowRequestCode() {
        RemotingServer remotingServer = this.defaultRemotingServer;
        NettyRequestProcessor rejectionProcessor = new DefaultRejectionProcessor();
        remotingServer.registerDefaultProcessor(rejectionProcessor, this.defaultExecutor);
        remotingServer.registerProcessor(RequestCode.POP_MESSAGE, rejectionProcessor, this.defaultExecutor);
        remotingServer.registerProcessor(RequestCode.ACK_MESSAGE, rejectionProcessor, this.defaultExecutor);
        remotingServer.registerProcessor(RequestCode.CHANGE_MESSAGE_INVISIBLETIME, rejectionProcessor, this.defaultExecutor);
    }

    /**
     * Extend the request code to support more features, like CreateTopicRequest.
     */
    private void extendRequestCode() {
        RemotingServer remotingServer = this.defaultRemotingServer;

        AdminActivity adminActivity = new AdminActivity(requestPipeline, messagingProcessor, metadataStore);
        remotingServer.registerProcessor(RequestCode.UPDATE_AND_CREATE_TOPIC, adminActivity, this.defaultExecutor);
        remotingServer.registerProcessor(RequestCode.UPDATE_AND_CREATE_SUBSCRIPTIONGROUP, adminActivity, this.defaultExecutor);
    }

    @Override
    protected RequestPipeline createRequestPipeline() {
        // Cache the request pipeline, to avoid creating it multiple times.
        // We may extend pipeline here in the future.
        requestPipeline = super.createRequestPipeline();
        return requestPipeline;
    }

    static class DefaultRejectionProcessor implements NettyRequestProcessor {
        @Override
        public RemotingCommand processRequest(ChannelHandlerContext context,
            RemotingCommand command) {
            return RemotingUtil.codeNotSupportedResponse(command);
        }

        @Override
        public boolean rejectRequest() {
            return false;
        }
    }
}
