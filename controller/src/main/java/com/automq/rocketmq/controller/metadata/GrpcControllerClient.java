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

package com.automq.rocketmq.controller.metadata;

import apache.rocketmq.controller.v1.CloseStreamReply;
import apache.rocketmq.controller.v1.CloseStreamRequest;
import apache.rocketmq.controller.v1.CommitOffsetReply;
import apache.rocketmq.controller.v1.CommitOffsetRequest;
import apache.rocketmq.controller.v1.CommitStreamObjectReply;
import apache.rocketmq.controller.v1.CommitStreamObjectRequest;
import apache.rocketmq.controller.v1.CommitWALObjectReply;
import apache.rocketmq.controller.v1.CommitWALObjectRequest;
import apache.rocketmq.controller.v1.CreateGroupReply;
import apache.rocketmq.controller.v1.CreateGroupRequest;
import apache.rocketmq.controller.v1.CreateTopicReply;
import apache.rocketmq.controller.v1.CreateTopicRequest;
import apache.rocketmq.controller.v1.DeleteTopicReply;
import apache.rocketmq.controller.v1.DeleteTopicRequest;
import apache.rocketmq.controller.v1.DescribeTopicReply;
import apache.rocketmq.controller.v1.DescribeTopicRequest;
import apache.rocketmq.controller.v1.HeartbeatReply;
import apache.rocketmq.controller.v1.HeartbeatRequest;
import apache.rocketmq.controller.v1.ListOpenStreamsReply;
import apache.rocketmq.controller.v1.ListOpenStreamsRequest;
import apache.rocketmq.controller.v1.MessageQueue;
import apache.rocketmq.controller.v1.NodeRegistrationReply;
import apache.rocketmq.controller.v1.NodeRegistrationRequest;
import apache.rocketmq.controller.v1.Code;
import apache.rocketmq.controller.v1.ControllerServiceGrpc;
import apache.rocketmq.controller.v1.NotifyMessageQueuesAssignableReply;
import apache.rocketmq.controller.v1.NotifyMessageQueuesAssignableRequest;
import apache.rocketmq.controller.v1.OpenStreamReply;
import apache.rocketmq.controller.v1.OpenStreamRequest;
import apache.rocketmq.controller.v1.PrepareS3ObjectsReply;
import apache.rocketmq.controller.v1.PrepareS3ObjectsRequest;
import apache.rocketmq.controller.v1.ReassignMessageQueueReply;
import apache.rocketmq.controller.v1.ReassignMessageQueueRequest;
import apache.rocketmq.controller.v1.Topic;
import apache.rocketmq.controller.v1.TrimStreamReply;
import apache.rocketmq.controller.v1.TrimStreamRequest;
import apache.rocketmq.controller.v1.UpdateTopicReply;
import apache.rocketmq.controller.v1.UpdateTopicRequest;
import com.automq.rocketmq.controller.exception.ControllerException;
import com.automq.rocketmq.controller.metadata.database.dao.Node;
import com.google.common.base.Strings;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.MoreExecutors;
import io.grpc.Channel;
import io.grpc.Grpc;
import io.grpc.InsecureChannelCredentials;
import io.grpc.ManagedChannel;

import java.io.IOException;
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

    private final ConcurrentMap<String, ControllerServiceGrpc.ControllerServiceFutureStub> stubs;

    public GrpcControllerClient() {
        stubs = new ConcurrentHashMap<>();
    }

    private ControllerServiceGrpc.ControllerServiceFutureStub buildStubForTarget(
        String target) throws ControllerException {
        if (Strings.isNullOrEmpty(target)) {
            throw new ControllerException(Code.NO_LEADER_VALUE, "Target address to leader controller is null or empty");
        }

        if (!stubs.containsKey(target)) {
            ManagedChannel channel = Grpc.newChannelBuilder(target, InsecureChannelCredentials.create())
                .build();
            ControllerServiceGrpc.ControllerServiceFutureStub stub = ControllerServiceGrpc.newFutureStub(channel);
            stubs.putIfAbsent(target, stub);
        }
        return stubs.get(target);
    }

    public CompletableFuture<Node> registerBroker(String target, String name, String address,
        String instanceId) {
        NodeRegistrationRequest request = NodeRegistrationRequest.newBuilder()
            .setBrokerName(name)
            .setAddress(address)
            .setInstanceId(instanceId)
            .build();

        CompletableFuture<Node> future = new CompletableFuture<>();

        try {
            Futures.addCallback(this.buildStubForTarget(target).registerNode(request), new FutureCallback<>() {
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
        } catch (ControllerException e) {
            future.completeExceptionally(e);
        }

        return future;
    }

    @Override
    public CompletableFuture<Long> createTopic(String target, CreateTopicRequest request)
        throws ControllerException {
        ControllerServiceGrpc.ControllerServiceFutureStub stub = this.buildStubForTarget(target);
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
    public CompletableFuture<Topic> updateTopic(String target, UpdateTopicRequest request) {
        CompletableFuture<Topic> future = new CompletableFuture<>();
        try {
            Futures.addCallback(buildStubForTarget(target).updateTopic(request), new FutureCallback<>() {
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
        } catch (ControllerException e) {
            future.completeExceptionally(e);
        }
        return future;
    }

    @Override
    public CompletableFuture<Void> deleteTopic(String target, long topicId) throws ControllerException {
        ControllerServiceGrpc.ControllerServiceFutureStub stub = this.buildStubForTarget(target);
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
    public CompletableFuture<Topic> describeTopic(String target, Long topicId,
        String topicName) {

        DescribeTopicRequest request = DescribeTopicRequest.newBuilder()
            .setTopicId(topicId)
            .setTopicName(topicName)
            .build();

        CompletableFuture<Topic> future = new CompletableFuture<>();
        try {
            Futures.addCallback(buildStubForTarget(target).describeTopic(request), new FutureCallback<>() {
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
        } catch (ControllerException e) {
            future.completeExceptionally(e);
        }
        return future;
    }

    @Override
    public CompletableFuture<Void> heartbeat(String target, int nodeId, long epoch,
        boolean goingAway) throws ControllerException {
        buildStubForTarget(target);
        
        ControllerServiceGrpc.ControllerServiceFutureStub stub = stubs.get(target);
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
        CompletableFuture<Void> future = new CompletableFuture<>();

        ReassignMessageQueueRequest request = ReassignMessageQueueRequest.newBuilder()
            .setQueue(MessageQueue.newBuilder().setTopicId(topicId).setQueueId(queueId).build())
            .setDstNodeId(dstNodeId)
            .build();

        try {
            Futures.addCallback(buildStubForTarget(target).reassignMessageQueue(request), new FutureCallback<>() {
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
        } catch (ControllerException e) {
            future.completeExceptionally(e);
        }

        return future;
    }

    @Override
    public CompletableFuture<Void> notifyQueueClose(String target, long topicId, int queueId) throws ControllerException {
        CompletableFuture<Void> future = new CompletableFuture<>();

        NotifyMessageQueuesAssignableRequest request = NotifyMessageQueuesAssignableRequest.newBuilder()
            .addQueues(MessageQueue.newBuilder()
                .setTopicId(topicId)
                .setQueueId(queueId).build())
            .build();

        Futures.addCallback(buildStubForTarget(target).notifyMessageQueueAssignable(request),
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
    public CompletableFuture<CreateGroupReply> createGroup(String target,
        CreateGroupRequest request) throws ControllerException {

        CompletableFuture<CreateGroupReply> future = new CompletableFuture<>();
        Futures.addCallback(this.buildStubForTarget(target).createGroup(request), new FutureCallback<>() {
            @Override
            public void onSuccess(CreateGroupReply result) {
                switch (result.getStatus().getCode()) {
                    case OK -> {
                        future.complete(result);
                    }
                    case DUPLICATED -> {
                        LOGGER.info("Group name {} has been taken", request.getName());
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
    public CompletableFuture<Void> commitOffset(String target, long groupId, long topicId, int queueId,
        long offset) {
        CommitOffsetRequest request = CommitOffsetRequest.newBuilder()
            .setGroupId(groupId)
            .setQueue(MessageQueue.newBuilder()
                .setTopicId(topicId)
                .setQueueId(queueId).build())
            .setOffset(offset)
            .build();

        CompletableFuture<Void> future = new CompletableFuture<>();
        try {
            Futures.addCallback(buildStubForTarget(target).commitOffset(request), new FutureCallback<>() {
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
        } catch (ControllerException e) {
            future.completeExceptionally(e);
        }
        return future;
    }

    @Override
    public CompletableFuture<OpenStreamReply> openStream(String target,
        OpenStreamRequest request) {
        CompletableFuture<OpenStreamReply> future = new CompletableFuture<>();
        try {
            Futures.addCallback(this.buildStubForTarget(target).openStream(request), new FutureCallback<>() {
                @Override
                public void onSuccess(OpenStreamReply result) {
                    future.complete(result);
                }

                @Override
                public void onFailure(@Nonnull Throwable t) {
                    future.completeExceptionally(t);
                }
            }, MoreExecutors.directExecutor());
        } catch (ControllerException e) {
            future.completeExceptionally(e);
        }

        return future;
    }

    @Override
    public CompletableFuture<CloseStreamReply> closeStream(String target,
        CloseStreamRequest request) {
        CompletableFuture<CloseStreamReply> future = new CompletableFuture<>();
        try {
            Futures.addCallback(this.buildStubForTarget(target).closeStream(request), new FutureCallback<>() {
                @Override
                public void onSuccess(CloseStreamReply result) {
                    future.complete(result);
                }

                @Override
                public void onFailure(@Nonnull Throwable t) {
                    future.completeExceptionally(t);
                }
            }, MoreExecutors.directExecutor());
        } catch (ControllerException e) {
            future.completeExceptionally(e);
        }
        return future;
    }

    @Override
    public CompletableFuture<ListOpenStreamsReply> listOpenStreams(String target, ListOpenStreamsRequest request) {
        CompletableFuture<ListOpenStreamsReply> future = new CompletableFuture<>();
        try {
            Futures.addCallback(buildStubForTarget(target).listOpenStreams(request), new FutureCallback<>() {
                @Override
                public void onSuccess(ListOpenStreamsReply result) {
                    future.complete(result);
                }

                @Override
                public void onFailure(@Nonnull Throwable t) {
                    future.completeExceptionally(t);
                }
            }, MoreExecutors.directExecutor());
        } catch (ControllerException e) {
            future.completeExceptionally(e);
        }
        return future;
    }

    @Override
    public CompletableFuture<TrimStreamReply> trimStream(String target,
        TrimStreamRequest request) {
        CompletableFuture<TrimStreamReply> future = new CompletableFuture<>();
        try {
            Futures.addCallback(this.buildStubForTarget(target).trimStream(request), new FutureCallback<>() {
                @Override
                public void onSuccess(TrimStreamReply result) {
                    future.complete(result);
                }

                @Override
                public void onFailure(@Nonnull Throwable t) {
                    future.completeExceptionally(t);
                }
            }, MoreExecutors.directExecutor());
        } catch (ControllerException e) {
            future.completeExceptionally(e);
        }
        return future;
    }

    @Override
    public CompletableFuture<PrepareS3ObjectsReply> prepareS3Objects(String target,
        PrepareS3ObjectsRequest request) {
        CompletableFuture<PrepareS3ObjectsReply> future = new CompletableFuture<>();
        try {
            Futures.addCallback(this.buildStubForTarget(target).prepareS3Objects(request), new FutureCallback<>() {
                @Override
                public void onSuccess(PrepareS3ObjectsReply result) {
                    future.complete(result);
                }

                @Override
                public void onFailure(@Nonnull Throwable t) {
                    future.completeExceptionally(t);
                }
            }, MoreExecutors.directExecutor());
        } catch (ControllerException e) {
            future.completeExceptionally(e);
        }
        return future;
    }

    @Override
    public CompletableFuture<CommitStreamObjectReply> commitStreamObject(String target, CommitStreamObjectRequest request) {
        CompletableFuture<CommitStreamObjectReply> future = new CompletableFuture<>();
        try {
            Futures.addCallback(this.buildStubForTarget(target).commitStreamObject(request), new FutureCallback<>() {
                @Override
                public void onSuccess(CommitStreamObjectReply result) {
                    future.complete(result);
                }

                @Override
                public void onFailure(@Nonnull Throwable t) {
                    future.completeExceptionally(t);
                }
            }, MoreExecutors.directExecutor());
        } catch (ControllerException e) {
            future.completeExceptionally(e);
        }
        return future;
    }

    @Override
    public CompletableFuture<CommitWALObjectReply> commitWALObject(String target, CommitWALObjectRequest request) {
        CompletableFuture<CommitWALObjectReply> future = new CompletableFuture<>();
        try {
            Futures.addCallback(this.buildStubForTarget(target).commitWALObject(request), new FutureCallback<>() {
                @Override
                public void onSuccess(CommitWALObjectReply result) {
                    future.complete(result);
                }

                @Override
                public void onFailure(@Nonnull Throwable t) {
                    future.completeExceptionally(t);
                }
            }, MoreExecutors.directExecutor());
        } catch (ControllerException e) {
            future.completeExceptionally(e);
        }
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
