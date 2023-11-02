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

package com.automq.rocketmq.proxy.grpc.client;

import apache.rocketmq.common.v1.Code;
import apache.rocketmq.proxy.v1.ProxyServiceGrpc;
import apache.rocketmq.proxy.v1.ResetConsumeOffsetByTimestampRequest;
import apache.rocketmq.proxy.v1.ResetConsumeOffsetReply;
import apache.rocketmq.proxy.v1.ResetConsumeOffsetRequest;
import com.automq.rocketmq.common.config.GrpcClientConfig;
import com.automq.rocketmq.proxy.grpc.ProxyClient;
import com.google.common.base.Strings;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.protobuf.Duration;
import io.grpc.Channel;
import io.grpc.Grpc;
import io.grpc.InsecureChannelCredentials;
import io.grpc.ManagedChannel;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.proxy.common.ProxyException;
import org.apache.rocketmq.proxy.common.ProxyExceptionCode;

public class GrpcProxyClient implements ProxyClient {

    private final GrpcClientConfig clientConfig;

    private final ConcurrentHashMap<String, ProxyServiceGrpc.ProxyServiceFutureStub> stubs;

    public GrpcProxyClient(GrpcClientConfig clientConfig) {
        this.clientConfig = clientConfig;
        stubs = new ConcurrentHashMap<>();
    }

    private ProxyServiceGrpc.ProxyServiceFutureStub getOrCreateStubForTarget(String target) {
        if (Strings.isNullOrEmpty(target)) {
            throw new IllegalArgumentException("target is null or empty");
        }

        if (!stubs.containsKey(target)) {
            ManagedChannel channel = Grpc.newChannelBuilder(target, InsecureChannelCredentials.create())
                .build();
            ProxyServiceGrpc.ProxyServiceFutureStub stub = ProxyServiceGrpc.newFutureStub(channel);
            stubs.putIfAbsent(target, stub);
        }
        Duration timeout = clientConfig.rpcTimeout();
        long rpcTimeout = TimeUnit.SECONDS.toMillis(timeout.getSeconds())
            + TimeUnit.NANOSECONDS.toMillis(timeout.getNanos());
        return stubs.get(target).withDeadlineAfter(rpcTimeout, TimeUnit.MILLISECONDS);
    }

    @Override
    public CompletableFuture<Void> resetConsumeOffset(String target, ResetConsumeOffsetRequest request) {
        ProxyServiceGrpc.ProxyServiceFutureStub stub = getOrCreateStubForTarget(target);

        CompletableFuture<Void> cf = new CompletableFuture<>();
        Futures.addCallback(stub.resetConsumeOffset(request),
            new FutureCallback<>() {
                @Override
                public void onSuccess(ResetConsumeOffsetReply result) {
                    if (result.getStatus().getCode() == Code.OK) {
                        cf.complete(null);
                    } else {
                        cf.completeExceptionally(new ProxyException(ProxyExceptionCode.INTERNAL_SERVER_ERROR, result.getStatus().getMessage()));
                    }
                }

                @Override
                public void onFailure(Throwable t) {
                    cf.completeExceptionally(t);
                }
            }, MoreExecutors.directExecutor());
        return cf;
    }

    @Override
    public CompletableFuture<Void> resetConsumeOffsetByTimestamp(String target,
        ResetConsumeOffsetByTimestampRequest request) {
        ProxyServiceGrpc.ProxyServiceFutureStub stub = getOrCreateStubForTarget(target);

        CompletableFuture<Void> cf = new CompletableFuture<>();
        Futures.addCallback(stub.resetConsumeOffsetByTimestamp(request),
            new FutureCallback<>() {
                @Override
                public void onSuccess(ResetConsumeOffsetReply result) {
                    if (result.getStatus().getCode() == Code.OK) {
                        cf.complete(null);
                    } else {
                        cf.completeExceptionally(new ProxyException(ProxyExceptionCode.INTERNAL_SERVER_ERROR, result.getStatus().getMessage()));
                    }
                }

                @Override
                public void onFailure(Throwable t) {
                    cf.completeExceptionally(t);
                }
            }, MoreExecutors.directExecutor());
        return cf;
    }

    @Override
    public void close() throws IOException {
        for (Map.Entry<String, ProxyServiceGrpc.ProxyServiceFutureStub> entry : stubs.entrySet()) {
            Channel channel = entry.getValue().getChannel();
            if (channel instanceof ManagedChannel managedChannel) {
                managedChannel.shutdownNow();
            }
        }

        for (Map.Entry<String, ProxyServiceGrpc.ProxyServiceFutureStub> entry : stubs.entrySet()) {
            Channel channel = entry.getValue().getChannel();
            if (channel instanceof ManagedChannel managedChannel) {
                try {
                    managedChannel.awaitTermination(3, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }
}
