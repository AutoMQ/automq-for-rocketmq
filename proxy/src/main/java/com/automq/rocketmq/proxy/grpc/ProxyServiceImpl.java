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

package com.automq.rocketmq.proxy.grpc;

import apache.rocketmq.common.v1.Code;
import apache.rocketmq.proxy.v1.ProxyServiceGrpc;
import apache.rocketmq.proxy.v1.ResetConsumeOffsetByTimestampRequest;
import apache.rocketmq.proxy.v1.ResetConsumeOffsetRequest;
import apache.rocketmq.proxy.v1.ResetConsumeOffsetReply;
import apache.rocketmq.proxy.v1.Status;
import com.automq.rocketmq.proxy.service.ExtendMessageService;
import com.google.protobuf.TextFormat;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;

public class ProxyServiceImpl extends ProxyServiceGrpc.ProxyServiceImplBase {
    private static final Logger LOGGER = org.slf4j.LoggerFactory.getLogger(ProxyServiceImpl.class);

    private final ExtendMessageService messageService;

    public ProxyServiceImpl(ExtendMessageService messageService) {
        this.messageService = messageService;
    }

    @Override
    public void resetConsumeOffset(ResetConsumeOffsetRequest request,
        StreamObserver<ResetConsumeOffsetReply> responseObserver) {
        LOGGER.info("Reset consume offset request received: {}", TextFormat.shortDebugString(request));
        messageService.resetConsumeOffset(request.getTopic(), request.getQueueId(), request.getGroup(), request.getNewConsumeOffset())
            .whenComplete((v, e) -> {
                if (e != null) {
                    responseObserver.onError(e);
                    return;
                }
                ResetConsumeOffsetReply reply = ResetConsumeOffsetReply.newBuilder()
                    .setStatus(Status
                        .newBuilder()
                        .setCode(Code.OK)
                        .build())
                    .build();
                responseObserver.onNext(reply);
                responseObserver.onCompleted();
            });
    }

    @Override
    public void resetConsumeOffsetByTimestamp(ResetConsumeOffsetByTimestampRequest request,
        StreamObserver<ResetConsumeOffsetReply> responseObserver) {
        LOGGER.info("Reset consume offset by timestamp request received: {}", TextFormat.shortDebugString(request));
        messageService.resetConsumeOffsetByTimestamp(request.getTopic(), request.getQueueId(), request.getGroup(), request.getTimestamp())
            .whenComplete((v, e) -> {
                if (e != null) {
                    responseObserver.onError(e);
                    return;
                }
                ResetConsumeOffsetReply reply = ResetConsumeOffsetReply.newBuilder()
                    .setStatus(Status
                        .newBuilder()
                        .setCode(Code.OK)
                        .build())
                    .build();
                responseObserver.onNext(reply);
                responseObserver.onCompleted();
            });
    }
}
