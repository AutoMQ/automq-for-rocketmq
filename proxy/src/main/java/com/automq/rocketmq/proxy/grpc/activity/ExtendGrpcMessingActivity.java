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

import org.apache.rocketmq.proxy.grpc.v2.DefaultGrpcMessingActivity;
import org.apache.rocketmq.proxy.processor.MessagingProcessor;

public class ExtendGrpcMessingActivity extends DefaultGrpcMessingActivity {
    public ExtendGrpcMessingActivity(MessagingProcessor messagingProcessor) {
        super(messagingProcessor);
    }

    @Override
    protected void init(MessagingProcessor messagingProcessor) {
        super.init(messagingProcessor);

        this.routeActivity = new ExtendRouteActivity(messagingProcessor, grpcClientSettingsManager, grpcChannelManager);
        this.sendMessageActivity = new ExtendSendMessageActivity(messagingProcessor, grpcClientSettingsManager, grpcChannelManager);
        this.receiveMessageActivity = new ExtendReceiveMessageActivity(messagingProcessor, receiptHandleProcessor, grpcClientSettingsManager, grpcChannelManager);
    }
}
