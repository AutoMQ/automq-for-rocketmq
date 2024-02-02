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

package com.automq.rocketmq.controller.client;

import apache.rocketmq.controller.v1.CloseStreamReply;
import apache.rocketmq.controller.v1.CloseStreamRequest;
import apache.rocketmq.controller.v1.Cluster;
import apache.rocketmq.controller.v1.CommitOffsetReply;
import apache.rocketmq.controller.v1.CommitOffsetRequest;
import apache.rocketmq.controller.v1.ConsumerGroup;
import apache.rocketmq.controller.v1.CreateGroupReply;
import apache.rocketmq.controller.v1.CreateGroupRequest;
import apache.rocketmq.controller.v1.CreateTopicReply;
import apache.rocketmq.controller.v1.CreateTopicRequest;
import apache.rocketmq.controller.v1.DeleteGroupReply;
import apache.rocketmq.controller.v1.DeleteGroupRequest;
import apache.rocketmq.controller.v1.DeleteTopicReply;
import apache.rocketmq.controller.v1.DeleteTopicRequest;
import apache.rocketmq.controller.v1.DescribeClusterReply;
import apache.rocketmq.controller.v1.DescribeClusterRequest;
import apache.rocketmq.controller.v1.DescribeGroupReply;
import apache.rocketmq.controller.v1.DescribeGroupRequest;
import apache.rocketmq.controller.v1.DescribeStreamReply;
import apache.rocketmq.controller.v1.DescribeStreamRequest;
import apache.rocketmq.controller.v1.DescribeTopicReply;
import apache.rocketmq.controller.v1.DescribeTopicRequest;
import apache.rocketmq.controller.v1.HeartbeatReply;
import apache.rocketmq.controller.v1.HeartbeatRequest;
import apache.rocketmq.controller.v1.ListGroupReply;
import apache.rocketmq.controller.v1.ListGroupRequest;
import apache.rocketmq.controller.v1.ListOpenStreamsReply;
import apache.rocketmq.controller.v1.ListOpenStreamsRequest;
import apache.rocketmq.controller.v1.ListTopicsReply;
import apache.rocketmq.controller.v1.ListTopicsRequest;
import apache.rocketmq.controller.v1.MessageQueue;
import apache.rocketmq.controller.v1.NodeRegistrationReply;
import apache.rocketmq.controller.v1.NodeRegistrationRequest;
import apache.rocketmq.common.v1.Code;
import apache.rocketmq.controller.v1.ControllerServiceGrpc;
import apache.rocketmq.controller.v1.NotifyMessageQueuesAssignableReply;
import apache.rocketmq.controller.v1.NotifyMessageQueuesAssignableRequest;
import apache.rocketmq.controller.v1.OpenStreamReply;
import apache.rocketmq.controller.v1.OpenStreamRequest;
import apache.rocketmq.controller.v1.ReassignMessageQueueReply;
import apache.rocketmq.controller.v1.ReassignMessageQueueRequest;
import apache.rocketmq.controller.v1.StreamDescription;
import apache.rocketmq.controller.v1.StreamMetadata;
import apache.rocketmq.controller.v1.TerminateNodeReply;
import apache.rocketmq.controller.v1.TerminateNodeRequest;
import apache.rocketmq.controller.v1.Topic;
import apache.rocketmq.controller.v1.UpdateGroupReply;
import apache.rocketmq.controller.v1.UpdateGroupRequest;
import apache.rocketmq.controller.v1.UpdateTopicReply;
import apache.rocketmq.controller.v1.UpdateTopicRequest;
import com.automq.rocketmq.common.config.GrpcClientConfig;
import com.automq.rocketmq.controller.ControllerClient;
import com.automq.rocketmq.common.exception.ControllerException;
import com.automq.rocketmq.metadata.dao.Node;
import com.google.common.base.Strings;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.protobuf.Duration;
import com.google.protobuf.TextFormat;
import io.grpc.Channel;
import io.grpc.Grpc;
import io.grpc.InsecureChannelCredentials;
import io.grpc.ManagedChannel;

import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nonnull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GrpcControllerClient implements ControllerClient {

    private static final Logger LOGGER = LoggerFactory.getLogger(GrpcControllerClient.class);

    private final GrpcClientConfig clientConfig;

    private final ConcurrentMap<String, ControllerServiceGrpc.ControllerServiceFutureStub> stubs;

    public GrpcControllerClient(GrpcClientConfig clientConfig) {
        this.clientConfig = clientConfig;
        stubs = new ConcurrentHashMap<>();
    }

    private ControllerServiceGrpc.ControllerServiceFutureStub getOrCreateStubForTarget(String target)
        throws ControllerException {
        if (Strings.isNullOrEmpty(target)) {
            throw new ControllerException(Code.NO_LEADER_VALUE, "Target address to leader controller is null or empty");
        }

        if (!stubs.containsKey(target)) {
            ManagedChannel channel = Grpc.newChannelBuilder(target, InsecureChannelCredentials.create())
                .build();
            ControllerServiceGrpc.ControllerServiceFutureStub stub = ControllerServiceGrpc.newFutureStub(channel);
            stubs.putIfAbsent(target, stub);
        }
        Duration timeout = clientConfig.rpcTimeout();
        long rpcTimeout = TimeUnit.SECONDS.toMillis(timeout.getSeconds())
            + TimeUnit.NANOSECONDS.toMillis(timeout.getNanos());
        return stubs.get(target).withDeadlineAfter(rpcTimeout, TimeUnit.MILLISECONDS);
    }

    @Override
    public CompletableFuture<Cluster> describeCluster(String target, DescribeClusterRequest request) {
        ControllerServiceGrpc.ControllerServiceFutureStub stub;
        try {
            stub = getOrCreateStubForTarget(target);
        } catch (ControllerException e) {
            return CompletableFuture.failedFuture(e);
        }

        CompletableFuture<Cluster> future = new CompletableFuture<>();
        Futures.addCallback(stub.describeCluster(request),
            new FutureCallback<>() {
                @Override
                public void onSuccess(DescribeClusterReply result) {
                    if (result.getStatus().getCode() == Code.OK) {
                        future.complete(result.getCluster());
                    } else {
                        future.completeExceptionally(new ControllerException(result.getStatus().getCodeValue(),
                            result.getStatus().getMessage()));
                    }
                }

                @Override
                public void onFailure(@Nonnull Throwable t) {
                    future.completeExceptionally(t);
                }
            }, MoreExecutors.directExecutor());
        return future;
    }

    public CompletableFuture<Node> registerBroker(String target, String name, String address,
        String instanceId) {
        ControllerServiceGrpc.ControllerServiceFutureStub stub;
        try {
            stub = getOrCreateStubForTarget(target);
        } catch (ControllerException e) {
            return CompletableFuture.failedFuture(e);
        }
        NodeRegistrationRequest request = NodeRegistrationRequest.newBuilder()
            .setBrokerName(name)
            .setAddress(address)
            .setInstanceId(instanceId)
            .build();

        CompletableFuture<Node> future = new CompletableFuture<>();

        Futures.addCallback(stub.registerNode(request), new FutureCallback<>() {
            @Override
            public void onSuccess(NodeRegistrationReply reply) {
                if (reply.getStatus().getCode() == Code.OK) {
                    Node node = new Node();
                    node.setName(name);
                    node.setAddress(address);
                    node.setInstanceId(instanceId);

                    node.setEpoch(reply.getEpoch());
                    node.setId(reply.getId());
                    future.complete(node);
                } else {
                    future.completeExceptionally(new ControllerException(reply.getStatus().getCode().getNumber(),
                        reply.getStatus().getMessage()));
                }
            }

            @Override
            public void onFailure(@Nonnull Throwable t) {
                future.completeExceptionally(new ControllerException(Code.INTERRUPTED_VALUE, t));
            }
        }, MoreExecutors.directExecutor());

        return future;
    }

    @Override
    public CompletableFuture<Long> createTopic(String target, CreateTopicRequest request) {
        ControllerServiceGrpc.ControllerServiceFutureStub stub;
        try {
            stub = getOrCreateStubForTarget(target);
        } catch (ControllerException e) {
            return CompletableFuture.failedFuture(e);
        }

        String topicName = request.getTopic();
        CompletableFuture<Long> future = new CompletableFuture<>();
        Futures.addCallback(stub.createTopic(request), new FutureCallback<>() {
            @Override
            public void onSuccess(CreateTopicReply result) {
                LOGGER.info("Leader has created topic for {} with topic-id={}", topicName, result.getTopicId());
                switch (result.getStatus().getCode()) {
                    case OK -> future.complete(result.getTopicId());
                    case DUPLICATED -> {
                        String msg = String.format("Topic name '%s' has been taken", request.getTopic());
                        ControllerException e = new ControllerException(Code.DUPLICATED_VALUE, msg);
                        future.completeExceptionally(e);
                    }
                    default -> {
                        ControllerException e = new ControllerException(Code.INTERRUPTED_VALUE, "Internal error");
                        future.completeExceptionally(e);
                    }
                }
            }

            @Override
            public void onFailure(@Nonnull Throwable t) {
                future.completeExceptionally(t);
                LOGGER.error("Leader node failed to create topic on behalf", t);
            }
        }, MoreExecutors.directExecutor());
        return future;
    }

    @Override
    public void terminateNode(String target, TerminateNodeRequest request,
        StreamObserver<TerminateNodeReply> observer) {
        ManagedChannel channel = Grpc.newChannelBuilder(target, InsecureChannelCredentials.create())
            .build();
        ControllerServiceGrpc.ControllerServiceStub stub = ControllerServiceGrpc.newStub(channel);
        stub.withDeadlineAfter(60, TimeUnit.SECONDS)
            .terminateNode(request, observer);
    }

    @Override
    public CompletableFuture<Topic> updateTopic(String target, UpdateTopicRequest request) {
        ControllerServiceGrpc.ControllerServiceFutureStub stub;
        try {
            stub = getOrCreateStubForTarget(target);
        } catch (ControllerException e) {
            return CompletableFuture.failedFuture(e);
        }
        CompletableFuture<Topic> future = new CompletableFuture<>();
        Futures.addCallback(stub.updateTopic(request), new FutureCallback<>() {
            @Override
            public void onSuccess(UpdateTopicReply result) {
                if (result.getStatus().getCode() == Code.OK) {
                    future.complete(result.getTopic());
                } else {
                    future.completeExceptionally(
                        new ControllerException(result.getStatus().getCodeValue(), result.getStatus().getMessage()));
                }
            }

            @Override
            public void onFailure(@Nonnull Throwable t) {
                future.completeExceptionally(t);
            }
        }, MoreExecutors.directExecutor());
        return future;
    }

    @Override
    public CompletableFuture<Void> deleteTopic(String target, long topicId) {
        ControllerServiceGrpc.ControllerServiceFutureStub stub;
        try {
            stub = getOrCreateStubForTarget(target);
        } catch (ControllerException e) {
            return CompletableFuture.failedFuture(e);
        }

        DeleteTopicRequest request = DeleteTopicRequest.newBuilder().setTopicId(topicId).build();

        CompletableFuture<Void> future = new CompletableFuture<>();

        Futures.addCallback(stub.deleteTopic(request), new FutureCallback<>() {
            @Override
            public void onSuccess(DeleteTopicReply result) {
                switch (result.getStatus().getCode()) {
                    case OK -> future.complete(null);
                    case NOT_FOUND -> future.completeExceptionally(new ControllerException(Code.NOT_FOUND_VALUE,
                        "Topic to delete is not found"));
                    default ->
                        future.completeExceptionally(new ControllerException(result.getStatus().getCode().getNumber(),
                            result.getStatus().getMessage()));
                }
            }

            @Override
            public void onFailure(@Nonnull Throwable t) {
                future.completeExceptionally(t);
            }
        }, MoreExecutors.directExecutor());

        return future;
    }

    @Override
    public CompletableFuture<Topic> describeTopic(String target, Long topicId, String topicName) {

        ControllerServiceGrpc.ControllerServiceFutureStub stub;
        try {
            stub = getOrCreateStubForTarget(target);
        } catch (ControllerException e) {
            return CompletableFuture.failedFuture(e);
        }

        DescribeTopicRequest.Builder builder = DescribeTopicRequest.newBuilder();
        if (null != topicId && topicId > 0) {
            builder.setTopicId(topicId);
        }

        if (!Strings.isNullOrEmpty(topicName)) {
            builder.setTopicName(topicName.trim());
        }

        CompletableFuture<Topic> future = new CompletableFuture<>();
        Futures.addCallback(stub.describeTopic(builder.build()), new FutureCallback<>() {
            @Override
            public void onSuccess(DescribeTopicReply result) {
                if (result.getStatus().getCode() == Code.OK) {
                    future.complete(result.getTopic());
                } else {
                    future.completeExceptionally(
                        new ControllerException(result.getStatus().getCodeValue(), result.getStatus().getMessage()));
                }
            }

            @Override
            public void onFailure(@Nonnull Throwable t) {
                future.completeExceptionally(t);
            }
        }, MoreExecutors.directExecutor());
        return future;
    }

    @Override
    public void listTopics(String target, ListTopicsRequest request, StreamObserver<ListTopicsReply> observer) {
        ManagedChannel channel = Grpc.newChannelBuilder(target, InsecureChannelCredentials.create())
            .build();
        ControllerServiceGrpc.ControllerServiceStub stub = ControllerServiceGrpc.newStub(channel);
        stub.withDeadlineAfter(60, TimeUnit.SECONDS)
            .listTopics(request, observer);
    }

    @Override
    public CompletableFuture<Void> heartbeat(String target, int nodeId, long epoch, boolean goingAway) {

        ControllerServiceGrpc.ControllerServiceFutureStub stub;
        try {
            stub = getOrCreateStubForTarget(target);
        } catch (ControllerException e) {
            return CompletableFuture.failedFuture(e);
        }

        HeartbeatRequest request = HeartbeatRequest
            .newBuilder()
            .setId(nodeId)
            .setEpoch(epoch)
            .setGoingAway(goingAway)
            .build();
        CompletableFuture<Void> future = new CompletableFuture<>();
        Futures.addCallback(stub.heartbeat(request), new FutureCallback<>() {
            @Override
            public void onSuccess(HeartbeatReply result) {
                if (result.getStatus().getCode() == Code.OK) {
                    future.complete(null);
                } else {
                    future.completeExceptionally(
                        new ControllerException(result.getStatus().getCodeValue(), result.getStatus().getMessage()));
                }
            }

            @Override
            public void onFailure(@Nonnull Throwable t) {
                future.completeExceptionally(t);
            }
        }, MoreExecutors.directExecutor());
        return future;
    }

    @Override
    public CompletableFuture<Void> reassignMessageQueue(String target, long topicId, int queueId,
        int dstNodeId) {

        ControllerServiceGrpc.ControllerServiceFutureStub stub;
        try {
            stub = getOrCreateStubForTarget(target);
        } catch (ControllerException e) {
            return CompletableFuture.failedFuture(e);
        }

        CompletableFuture<Void> future = new CompletableFuture<>();

        ReassignMessageQueueRequest request = ReassignMessageQueueRequest.newBuilder()
            .setQueue(MessageQueue.newBuilder().setTopicId(topicId).setQueueId(queueId).build())
            .setDstNodeId(dstNodeId)
            .build();

        Futures.addCallback(stub.reassignMessageQueue(request), new FutureCallback<>() {
            @Override
            public void onSuccess(ReassignMessageQueueReply result) {
                if (result.getStatus().getCode() == Code.OK) {
                    future.complete(null);
                } else {
                    future.completeExceptionally(new ControllerException(result.getStatus().getCodeValue(), result.getStatus().getMessage()));
                }
            }

            @Override
            public void onFailure(@Nonnull Throwable t) {
                future.completeExceptionally(t);
            }
        }, MoreExecutors.directExecutor());

        return future;
    }

    @Override
    public CompletableFuture<Void> notifyQueueClose(String target, long topicId, int queueId) {

        ControllerServiceGrpc.ControllerServiceFutureStub stub;
        try {
            stub = getOrCreateStubForTarget(target);
        } catch (ControllerException e) {
            return CompletableFuture.failedFuture(e);
        }

        CompletableFuture<Void> future = new CompletableFuture<>();

        NotifyMessageQueuesAssignableRequest request = NotifyMessageQueuesAssignableRequest.newBuilder()
            .addQueues(MessageQueue.newBuilder()
                .setTopicId(topicId)
                .setQueueId(queueId).build())
            .build();

        Futures.addCallback(stub.notifyMessageQueueAssignable(request),
            new FutureCallback<>() {
                @Override
                public void onSuccess(NotifyMessageQueuesAssignableReply result) {
                    if (result.getStatus().getCode() == Code.OK) {
                        future.complete(null);
                    } else {
                        future.completeExceptionally(
                            new ControllerException(result.getStatus().getCodeValue(), result.getStatus().getMessage())
                        );
                    }
                }

                @Override
                public void onFailure(@Nonnull Throwable t) {
                    future.completeExceptionally(t);
                }
            }, MoreExecutors.directExecutor());

        return future;
    }

    @Override
    public CompletableFuture<Long> createGroup(String target, CreateGroupRequest request) {
        ControllerServiceGrpc.ControllerServiceFutureStub stub;
        try {
            stub = getOrCreateStubForTarget(target);
        } catch (ControllerException e) {
            return CompletableFuture.failedFuture(e);
        }

        CompletableFuture<Long> future = new CompletableFuture<>();
        Futures.addCallback(stub.createGroup(request), new FutureCallback<>() {
            @Override
            public void onSuccess(CreateGroupReply result) {
                switch (result.getStatus().getCode()) {
                    case OK -> future.complete(result.getGroupId());
                    case DUPLICATED -> {
                        LOGGER.info("Group name {} has been taken", request.getName());
                        ControllerException e = new ControllerException(result.getStatus().getCodeValue(),
                            result.getStatus().getMessage());
                        future.completeExceptionally(e);
                    }
                    case BAD_REQUEST -> {
                        LOGGER.info("CreateGroup request is rejected. Reason: {}", result.getStatus().getMessage());
                        ControllerException e = new ControllerException(result.getStatus().getCodeValue(),
                            result.getStatus().getMessage());
                        future.completeExceptionally(e);
                    }
                    default -> {
                        LOGGER.warn("Unexpected error while creating group {}", request.getName());
                        ControllerException e = new ControllerException(result.getStatus().getCodeValue(),
                            result.getStatus().getMessage());
                        future.completeExceptionally(e);
                    }
                }
            }

            @Override
            public void onFailure(@Nonnull Throwable t) {
                future.completeExceptionally(t);
            }
        }, MoreExecutors.directExecutor());
        return future;
    }

    @Override
    public CompletableFuture<ConsumerGroup> describeGroup(String target, String groupName) {
        ControllerServiceGrpc.ControllerServiceFutureStub stub;
        try {
            stub = getOrCreateStubForTarget(target);
        } catch (ControllerException e) {
            return CompletableFuture.failedFuture(e);
        }

        DescribeGroupRequest request = DescribeGroupRequest.newBuilder()
            .setName(groupName).build();

        CompletableFuture<ConsumerGroup> future = new CompletableFuture<>();

        Futures.addCallback(stub.describeGroup(request), new FutureCallback<>() {
            @Override
            public void onSuccess(DescribeGroupReply result) {
                switch (result.getStatus().getCode()) {
                    case OK -> future.complete(result.getGroup());
                    case NOT_FOUND -> {
                        LOGGER.info("Group[{}] is not found", groupName);
                        future.completeExceptionally(new ControllerException(result.getStatus().getCodeValue(), result.getStatus().getMessage()));
                    }
                    default -> {
                        LOGGER.error("Unexpected describe group response: {}", TextFormat.shortDebugString(result));
                        future.completeExceptionally(new ControllerException(result.getStatus().getCodeValue(), result.getStatus().getMessage()));
                    }
                }
            }

            @Override
            public void onFailure(@Nonnull Throwable t) {
                future.completeExceptionally(t);
            }
        }, MoreExecutors.directExecutor());
        return future;
    }

    @Override
    public CompletableFuture<Void> updateGroup(String target, UpdateGroupRequest request) {
        ControllerServiceGrpc.ControllerServiceFutureStub stub;
        try {
            stub = getOrCreateStubForTarget(target);
        } catch (ControllerException e) {
            return CompletableFuture.failedFuture(e);
        }

        CompletableFuture<Void> future = new CompletableFuture<>();
        Futures.addCallback(stub.updateGroup(request), new FutureCallback<>() {
            @Override
            public void onSuccess(UpdateGroupReply result) {
                if (result.getStatus().getCode() == Code.OK) {
                    future.complete(null);
                } else {
                    future.completeExceptionally(new ControllerException(result.getStatus().getCodeValue(), result.getStatus().getMessage()));
                }
            }

            @Override
            public void onFailure(@Nonnull Throwable t) {
                future.completeExceptionally(t);
            }
        }, MoreExecutors.directExecutor());
        return future;
    }

    @Override
    public CompletableFuture<Void> deleteGroup(String target, long groupId) {
        ControllerServiceGrpc.ControllerServiceFutureStub stub;
        try {
            stub = getOrCreateStubForTarget(target);
        } catch (ControllerException e) {
            return CompletableFuture.failedFuture(e);
        }

        DeleteGroupRequest request = DeleteGroupRequest.newBuilder().setId(groupId).build();

        CompletableFuture<Void> future = new CompletableFuture<>();

        Futures.addCallback(stub.deleteGroup(request), new FutureCallback<>() {
            @Override
            public void onSuccess(DeleteGroupReply result) {
                switch (result.getStatus().getCode()) {
                    case OK -> future.complete(null);
                    case NOT_FOUND -> {
                        LOGGER.info("Group[group-id{}] is not found", groupId);
                        future.completeExceptionally(new ControllerException(result.getStatus().getCodeValue(), result.getStatus().getMessage()));
                    }
                    default -> {
                        LOGGER.error("Unexpected delete group response: {}", TextFormat.shortDebugString(result));
                        future.completeExceptionally(new ControllerException(result.getStatus().getCodeValue(), result.getStatus().getMessage()));
                    }
                }
            }

            @Override
            public void onFailure(@Nonnull Throwable t) {
                future.completeExceptionally(t);
            }
        }, MoreExecutors.directExecutor());
        return future;
    }

    @Override
    public void listGroups(String target, ListGroupRequest request, StreamObserver<ListGroupReply> observer) {
        ManagedChannel channel = Grpc.newChannelBuilder(target, InsecureChannelCredentials.create())
            .build();
        ControllerServiceGrpc.ControllerServiceStub stub = ControllerServiceGrpc.newStub(channel);
        stub.withDeadlineAfter(60, TimeUnit.SECONDS)
            .listGroups(request, observer);
    }

    @Override
    public CompletableFuture<Void> commitOffset(String target, long groupId, long topicId, int queueId,
        long offset) {

        ControllerServiceGrpc.ControllerServiceFutureStub stub;
        try {
            stub = getOrCreateStubForTarget(target);
        } catch (ControllerException e) {
            return CompletableFuture.failedFuture(e);
        }

        CommitOffsetRequest request = CommitOffsetRequest.newBuilder()
            .setGroupId(groupId)
            .setQueue(MessageQueue.newBuilder()
                .setTopicId(topicId)
                .setQueueId(queueId).build())
            .setOffset(offset)
            .build();

        CompletableFuture<Void> future = new CompletableFuture<>();
        Futures.addCallback(stub.commitOffset(request), new FutureCallback<>() {
            @Override
            public void onSuccess(CommitOffsetReply result) {
                if (result.getStatus().getCode() == Code.OK) {
                    future.complete(null);
                } else {
                    future.completeExceptionally(
                        new ControllerException(result.getStatus().getCodeValue(), result.getStatus().getMessage()));
                }
            }

            @Override
            public void onFailure(@Nonnull Throwable t) {
                future.completeExceptionally(t);
            }
        }, MoreExecutors.directExecutor());
        return future;
    }

    @Override
    public CompletableFuture<StreamMetadata> openStream(String target,
        OpenStreamRequest request) {
        ControllerServiceGrpc.ControllerServiceFutureStub stub;
        try {
            stub = getOrCreateStubForTarget(target);
        } catch (ControllerException e) {
            return CompletableFuture.failedFuture(e);
        }

        CompletableFuture<StreamMetadata> future = new CompletableFuture<>();
        Futures.addCallback(stub.openStream(request), new FutureCallback<>() {
            @Override
            public void onSuccess(OpenStreamReply result) {
                if (result.getStatus().getCode() == Code.OK) {
                    future.complete(result.getStreamMetadata());
                } else {
                    future.completeExceptionally(new ControllerException(result.getStatus().getCodeValue(),
                        result.getStatus().getMessage()));
                }
            }

            @Override
            public void onFailure(@Nonnull Throwable t) {
                future.completeExceptionally(t);
            }
        }, MoreExecutors.directExecutor());

        return future;
    }

    @Override
    public CompletableFuture<Void> closeStream(String target,
        CloseStreamRequest request) {
        ControllerServiceGrpc.ControllerServiceFutureStub stub;
        try {
            stub = getOrCreateStubForTarget(target);
        } catch (ControllerException e) {
            return CompletableFuture.failedFuture(e);
        }
        CompletableFuture<Void> future = new CompletableFuture<>();
        Futures.addCallback(stub.closeStream(request), new FutureCallback<>() {
            @Override
            public void onSuccess(CloseStreamReply result) {
                if (result.getStatus().getCode() == Code.OK) {
                    future.complete(null);
                } else {
                    future.completeExceptionally(new ControllerException(result.getStatus().getCodeValue(),
                        result.getStatus().getMessage()));
                }
            }

            @Override
            public void onFailure(@Nonnull Throwable t) {
                future.completeExceptionally(t);
            }
        }, MoreExecutors.directExecutor());
        return future;
    }

    @Override
    public CompletableFuture<List<StreamMetadata>> listOpenStreams(String target, ListOpenStreamsRequest request) {
        ControllerServiceGrpc.ControllerServiceFutureStub stub;
        try {
            stub = getOrCreateStubForTarget(target);
        } catch (ControllerException e) {
            return CompletableFuture.failedFuture(e);
        }

        CompletableFuture<List<StreamMetadata>> future = new CompletableFuture<>();
        Futures.addCallback(stub.listOpenStreams(request), new FutureCallback<>() {
            @Override
            public void onSuccess(ListOpenStreamsReply result) {
                if (result.getStatus().getCode() == Code.OK) {
                    future.complete(result.getStreamMetadataList());
                } else {
                    future.completeExceptionally(new ControllerException(result.getStatus().getCodeValue(),
                        result.getStatus().getMessage()));
                }
            }

            @Override
            public void onFailure(@Nonnull Throwable t) {
                future.completeExceptionally(t);
            }
        }, MoreExecutors.directExecutor());
        return future;
    }

    @Override
    public CompletableFuture<StreamDescription> describeStream(String target, DescribeStreamRequest request) {
        ControllerServiceGrpc.ControllerServiceFutureStub stub;
        try {
            stub = getOrCreateStubForTarget(target);
        } catch (ControllerException e) {
            return CompletableFuture.failedFuture(e);
        }
        CompletableFuture<StreamDescription> future = new CompletableFuture<>();
        Futures.addCallback(stub.describeStream(request), new FutureCallback<>() {
            @Override
            public void onSuccess(DescribeStreamReply result) {
                if (result.getStatus().getCode() == Code.OK) {
                    future.complete(result.getDescription());
                } else {
                    future.completeExceptionally(new ControllerException(result.getStatus().getCodeValue(),
                        result.getStatus().getMessage()));
                }
            }

            @Override
            public void onFailure(@Nonnull Throwable t) {
                future.completeExceptionally(t);
            }
        }, MoreExecutors.directExecutor());
        return future;
    }

    @Override
    public void close() throws IOException {
        for (Map.Entry<String, ControllerServiceGrpc.ControllerServiceFutureStub> entry : stubs.entrySet()) {
            Channel channel = entry.getValue().getChannel();
            if (channel instanceof ManagedChannel managedChannel) {
                managedChannel.shutdownNow();
            }
        }

        for (Map.Entry<String, ControllerServiceGrpc.ControllerServiceFutureStub> entry : stubs.entrySet()) {
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
