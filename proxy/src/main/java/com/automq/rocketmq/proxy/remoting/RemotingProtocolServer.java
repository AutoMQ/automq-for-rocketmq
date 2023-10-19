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

import com.automq.rocketmq.proxy.remoting.activity.ExtendSendMessageActivity;
import io.netty.channel.ChannelHandlerContext;
import org.apache.rocketmq.proxy.processor.MessagingProcessor;
import org.apache.rocketmq.proxy.remoting.pipeline.RequestPipeline;
import org.apache.rocketmq.remoting.RemotingServer;
import org.apache.rocketmq.remoting.netty.NettyRequestProcessor;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RequestCode;

public class RemotingProtocolServer extends org.apache.rocketmq.proxy.remoting.RemotingProtocolServer {
    private RequestPipeline requestPipeline;
    public RemotingProtocolServer(MessagingProcessor messagingProcessor) {
        super(messagingProcessor);

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
            return RemotingUtil.notSupportedResponse(command);
        }

        @Override
        public boolean rejectRequest() {
            return false;
        }
    }
}
