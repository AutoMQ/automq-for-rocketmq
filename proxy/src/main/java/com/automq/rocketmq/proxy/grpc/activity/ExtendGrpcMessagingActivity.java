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

import com.automq.rocketmq.proxy.service.SuspendRequestService;
import org.apache.rocketmq.proxy.grpc.v2.DefaultGrpcMessingActivity;
import org.apache.rocketmq.proxy.processor.MessagingProcessor;

public class ExtendGrpcMessagingActivity extends DefaultGrpcMessingActivity {
    public ExtendGrpcMessagingActivity(MessagingProcessor messagingProcessor) {
        super(messagingProcessor);
    }

    @Override
    protected void init(MessagingProcessor messagingProcessor) {
        super.init(messagingProcessor);

        this.routeActivity = new ExtendRouteActivity(messagingProcessor, grpcClientSettingsManager, grpcChannelManager);
        this.sendMessageActivity = new ExtendSendMessageActivity(messagingProcessor, grpcClientSettingsManager, grpcChannelManager);
        this.receiveMessageActivity = new ExtendReceiveMessageActivity(messagingProcessor, receiptHandleProcessor, grpcClientSettingsManager, grpcChannelManager);
        appendStartAndShutdown(SuspendRequestService.getInstance());
    }
}
