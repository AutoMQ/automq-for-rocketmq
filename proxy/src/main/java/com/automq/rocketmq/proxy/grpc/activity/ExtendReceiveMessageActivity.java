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
