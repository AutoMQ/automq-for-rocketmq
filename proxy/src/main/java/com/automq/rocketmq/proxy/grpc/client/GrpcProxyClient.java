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

package com.automq.rocketmq.proxy.grpc.client;

import apache.rocketmq.common.v1.Code;
import apache.rocketmq.proxy.v1.ConsumerClientConnection;
import apache.rocketmq.proxy.v1.ConsumerClientConnectionReply;
import apache.rocketmq.proxy.v1.ConsumerClientConnectionRequest;
import apache.rocketmq.proxy.v1.ProducerClientConnection;
import apache.rocketmq.proxy.v1.ProducerClientConnectionReply;
import apache.rocketmq.proxy.v1.ProducerClientConnectionRequest;
import apache.rocketmq.proxy.v1.ProxyServiceGrpc;
import apache.rocketmq.proxy.v1.PutMessageCommand;
import apache.rocketmq.proxy.v1.QueueStats;
import apache.rocketmq.proxy.v1.RelayReply;
import apache.rocketmq.proxy.v1.RelayRequest;
import apache.rocketmq.proxy.v1.ResetConsumeOffsetByTimestampRequest;
import apache.rocketmq.proxy.v1.ResetConsumeOffsetReply;
import apache.rocketmq.proxy.v1.ResetConsumeOffsetRequest;
import apache.rocketmq.proxy.v1.Status;
import apache.rocketmq.proxy.v1.TopicStatsReply;
import apache.rocketmq.proxy.v1.TopicStatsRequest;
import com.automq.rocketmq.common.config.GrpcClientConfig;
import com.automq.rocketmq.common.model.generated.FlatMessage;
import com.automq.rocketmq.proxy.grpc.ProxyClient;
import com.google.common.base.Strings;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.protobuf.ByteString;
import com.google.protobuf.Duration;
import io.grpc.Channel;
import io.grpc.Grpc;
import io.grpc.InsecureChannelCredentials;
import io.grpc.ManagedChannel;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.proxy.common.ProxyException;
import org.apache.rocketmq.proxy.common.ProxyExceptionCode;

public class GrpcProxyClient implements ProxyClient {

    private final GrpcClientConfig clientConfig;

    private final ConcurrentHashMap<String, ProxyServiceGrpc.ProxyServiceFutureStub> stubMap;

    public GrpcProxyClient(GrpcClientConfig clientConfig) {
        this.clientConfig = clientConfig;
        stubMap = new ConcurrentHashMap<>();
    }

    private ProxyServiceGrpc.ProxyServiceFutureStub getOrCreateStubForTarget(String target) {
        if (Strings.isNullOrEmpty(target)) {
            throw new IllegalArgumentException("target is null or empty");
        }

        if (!stubMap.containsKey(target)) {
            ManagedChannel channel = Grpc.newChannelBuilder(target, InsecureChannelCredentials.create())
                .build();
            ProxyServiceGrpc.ProxyServiceFutureStub stub = ProxyServiceGrpc.newFutureStub(channel);
            stubMap.putIfAbsent(target, stub);
        }
        Duration timeout = clientConfig.rpcTimeout();
        long rpcTimeout = TimeUnit.SECONDS.toMillis(timeout.getSeconds())
            + TimeUnit.NANOSECONDS.toMillis(timeout.getNanos());
        return stubMap.get(target).withDeadlineAfter(rpcTimeout, TimeUnit.MILLISECONDS);
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
    public CompletableFuture<List<QueueStats>> getTopicStats(String target, TopicStatsRequest request) {
        ProxyServiceGrpc.ProxyServiceFutureStub stub = getOrCreateStubForTarget(target);

        CompletableFuture<List<QueueStats>> future = new CompletableFuture<>();
        Futures.addCallback(stub.topicStats(request),
            new FutureCallback<>() {
                @Override
                public void onSuccess(TopicStatsReply result) {
                    if (result.getStatus().getCode() == Code.OK) {
                        future.complete(result.getQueueStatsList());
                    } else {
                        future.completeExceptionally(new ProxyException(ProxyExceptionCode.INTERNAL_SERVER_ERROR, result.getStatus().getMessage()));
                    }
                }

                @Override
                public void onFailure(Throwable t) {
                    future.completeExceptionally(t);
                }
            }, MoreExecutors.directExecutor());
        return future;
    }

    @Override
    public CompletableFuture<List<ProducerClientConnection>> producerClientConnection(String target,
        ProducerClientConnectionRequest request) {
        ProxyServiceGrpc.ProxyServiceFutureStub stub = getOrCreateStubForTarget(target);

        CompletableFuture<List<ProducerClientConnection>> future = new CompletableFuture<>();
        Futures.addCallback(stub.producerClientConnection(request),
            new FutureCallback<>() {
                @Override
                public void onSuccess(ProducerClientConnectionReply result) {
                    if (result.getStatus().getCode() == Code.OK) {
                        future.complete(result.getConnectionList());
                    } else {
                        future.completeExceptionally(new ProxyException(ProxyExceptionCode.INTERNAL_SERVER_ERROR, result.getStatus().getMessage()));
                    }
                }

                @Override
                public void onFailure(Throwable t) {
                    future.completeExceptionally(t);
                }
            }, MoreExecutors.directExecutor());
        return future;
    }

    @Override
    public CompletableFuture<List<ConsumerClientConnection>> consumerClientConnection(String target,
        ConsumerClientConnectionRequest request) {
        ProxyServiceGrpc.ProxyServiceFutureStub stub = getOrCreateStubForTarget(target);

        CompletableFuture<List<ConsumerClientConnection>> future = new CompletableFuture<>();
        Futures.addCallback(stub.consumerClientConnection(request),
            new FutureCallback<>() {
                @Override
                public void onSuccess(ConsumerClientConnectionReply result) {
                    if (result.getStatus().getCode() == Code.OK) {
                        future.complete(result.getConnectionList());
                    } else {
                        future.completeExceptionally(new ProxyException(ProxyExceptionCode.INTERNAL_SERVER_ERROR, result.getStatus().getMessage()));
                    }
                }

                @Override
                public void onFailure(Throwable t) {
                    future.completeExceptionally(t);
                }
            }, MoreExecutors.directExecutor());
        return future;
    }

    @Override
    public CompletableFuture<Status> relayMessage(String target, FlatMessage message) {
        ProxyServiceGrpc.ProxyServiceFutureStub stub = getOrCreateStubForTarget(target);

        ByteString flatMessage = ByteString.copyFrom(message.getByteBuffer());
        PutMessageCommand command = PutMessageCommand.newBuilder().setFlatMessage(flatMessage).build();
        RelayRequest request = RelayRequest.newBuilder().setPutMessageCommand(command).build();

        CompletableFuture<Status> future = new CompletableFuture<>();
        Futures.addCallback(stub.relay(request),
            new FutureCallback<>() {
                @Override
                public void onSuccess(RelayReply result) {
                    future.complete(result.getStatus());
                }

                @Override
                public void onFailure(Throwable t) {
                    future.completeExceptionally(t);
                }
            }, MoreExecutors.directExecutor());
        return future;
    }

    @Override
    public void close() {
        for (Map.Entry<String, ProxyServiceGrpc.ProxyServiceFutureStub> entry : stubMap.entrySet()) {
            Channel channel = entry.getValue().getChannel();
            if (channel instanceof ManagedChannel managedChannel) {
                managedChannel.shutdownNow();
            }
        }

        for (Map.Entry<String, ProxyServiceGrpc.ProxyServiceFutureStub> entry : stubMap.entrySet()) {
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
