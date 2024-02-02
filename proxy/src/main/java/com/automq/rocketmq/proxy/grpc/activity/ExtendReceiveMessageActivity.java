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

import apache.rocketmq.v2.ReceiveMessageResponse;
import com.automq.rocketmq.proxy.grpc.v2.consumer.ExtendReceiveMessageResponseStreamWriter;
import com.automq.rocketmq.proxy.model.ProxyContextExt;
import io.grpc.stub.StreamObserver;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.grpc.v2.channel.GrpcChannelManager;
import org.apache.rocketmq.proxy.grpc.v2.common.GrpcClientSettingsManager;
import org.apache.rocketmq.proxy.grpc.v2.consumer.ReceiveMessageActivity;
import org.apache.rocketmq.proxy.grpc.v2.consumer.ReceiveMessageResponseStreamWriter;
import org.apache.rocketmq.proxy.processor.MessagingProcessor;
import org.apache.rocketmq.proxy.processor.ReceiptHandleProcessor;

public class ExtendReceiveMessageActivity extends ReceiveMessageActivity {
    public ExtendReceiveMessageActivity(MessagingProcessor messagingProcessor,
        ReceiptHandleProcessor receiptHandleProcessor,
        GrpcClientSettingsManager grpcClientSettingsManager,
        GrpcChannelManager grpcChannelManager) {
        super(messagingProcessor, receiptHandleProcessor, grpcClientSettingsManager, grpcChannelManager);
    }

    @Override
    protected ReceiveMessageResponseStreamWriter createWriter(ProxyContext ctx,
        StreamObserver<ReceiveMessageResponse> responseObserver) {
        return new ExtendReceiveMessageResponseStreamWriter((ProxyContextExt) ctx, messagingProcessor, responseObserver);
    }
}
