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

package com.automq.rocketmq.controller.server;

import apache.rocketmq.controller.v1.CloseStreamReply;
import apache.rocketmq.controller.v1.CloseStreamRequest;
import apache.rocketmq.common.v1.Code;
import apache.rocketmq.controller.v1.CommitOffsetReply;
import apache.rocketmq.controller.v1.CommitOffsetRequest;
import apache.rocketmq.controller.v1.ConsumerGroup;
import apache.rocketmq.controller.v1.ControllerServiceGrpc;
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
import apache.rocketmq.controller.v1.ListMessageQueueReassignmentsReply;
import apache.rocketmq.controller.v1.ListMessageQueueReassignmentsRequest;
import apache.rocketmq.controller.v1.ListOpenStreamsRequest;
import apache.rocketmq.controller.v1.ListOpenStreamsReply;
import apache.rocketmq.controller.v1.ListTopicsReply;
import apache.rocketmq.controller.v1.ListTopicsRequest;
import apache.rocketmq.controller.v1.MessageQueue;
import apache.rocketmq.controller.v1.NodeRegistrationReply;
import apache.rocketmq.controller.v1.NodeRegistrationRequest;
import apache.rocketmq.controller.v1.NodeUnregistrationReply;
import apache.rocketmq.controller.v1.NodeUnregistrationRequest;
import apache.rocketmq.controller.v1.NotifyMessageQueuesAssignableReply;
import apache.rocketmq.controller.v1.NotifyMessageQueuesAssignableRequest;
import apache.rocketmq.controller.v1.OpenStreamReply;
import apache.rocketmq.controller.v1.OpenStreamRequest;
import apache.rocketmq.controller.v1.ReassignMessageQueueReply;
import apache.rocketmq.controller.v1.ReassignMessageQueueRequest;
import apache.rocketmq.controller.v1.Status;
import apache.rocketmq.controller.v1.SubscriptionMode;
import apache.rocketmq.controller.v1.TerminateNodeReply;
import apache.rocketmq.controller.v1.TerminateNodeRequest;
import apache.rocketmq.controller.v1.TerminationStage;
import apache.rocketmq.controller.v1.Topic;
import apache.rocketmq.controller.v1.UpdateGroupReply;
import apache.rocketmq.controller.v1.UpdateGroupRequest;
import apache.rocketmq.controller.v1.UpdateTopicReply;
import apache.rocketmq.controller.v1.UpdateTopicRequest;
import com.automq.rocketmq.common.PrefixThreadFactory;
import com.automq.rocketmq.controller.MetadataStore;
import com.automq.rocketmq.common.exception.ControllerException;
import com.automq.rocketmq.controller.server.tasks.TerminationStageTask;
import com.google.protobuf.TextFormat;
import io.grpc.Context;
import io.grpc.stub.StreamObserver;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ControllerServiceImpl extends ControllerServiceGrpc.ControllerServiceImplBase {

    private static final Logger LOGGER = LoggerFactory.getLogger(ControllerServiceImpl.class);

    private final MetadataStore metadataStore;

    private final ScheduledExecutorService executorService;

    public ControllerServiceImpl(MetadataStore metadataStore) {
        this.metadataStore = metadataStore;
        executorService = Executors.newSingleThreadScheduledExecutor(new PrefixThreadFactory("ControllerService_"));
    }

    @Override
    public void describeCluster(DescribeClusterRequest request, StreamObserver<DescribeClusterReply> responseObserver) {
        LOGGER.debug("Received {}", TextFormat.shortDebugString(request));
        metadataStore.describeCluster(request)
            .whenComplete((c, e) -> {
                if (null != e) {
                    responseObserver.onError(e);
                    return;
                }
                DescribeClusterReply reply = DescribeClusterReply.newBuilder()
                    .setCluster(c)
                    .setStatus(Status.newBuilder().setCode(Code.OK).build())
                    .build();
                responseObserver.onNext(reply);
                responseObserver.onCompleted();
            });
    }

    @Override
    public void registerNode(NodeRegistrationRequest request,
        StreamObserver<NodeRegistrationReply> responseObserver) {
        metadataStore.registerBrokerNode(request.getBrokerName(), request.getAddress(),
                request.getInstanceId())
            .whenComplete((res, e) -> {
                if (null != e) {
                    if (e.getCause() instanceof ControllerException ex) {
                        NodeRegistrationReply reply = NodeRegistrationReply.newBuilder()
                            .setStatus(Status.newBuilder()
                                .setCode(Code.forNumber(ex.getErrorCode()))
                                .setMessage(e.getMessage()).build())
                            .build();
                        responseObserver.onNext(reply);
                        responseObserver.onCompleted();
                    } else if (e instanceof ControllerException ex) {
                        NodeRegistrationReply reply = NodeRegistrationReply.newBuilder()
                            .setStatus(Status.newBuilder()
                                .setCode(Code.forNumber(ex.getErrorCode()))
                                .setMessage(e.getMessage()).build())
                            .build();
                        responseObserver.onNext(reply);
                        responseObserver.onCompleted();
                    } else {
                        responseObserver.onError(e);
                    }
                } else {
                    NodeRegistrationReply reply = NodeRegistrationReply.newBuilder()
                        .setStatus(Status.newBuilder().setCode(Code.OK).build())
                        .setId(res.getId())
                        .setEpoch(res.getEpoch())
                        .build();
                    responseObserver.onNext(reply);
                    responseObserver.onCompleted();
                }
            });
    }

    @Override
    public void unregisterNode(NodeUnregistrationRequest request,
        StreamObserver<NodeUnregistrationReply> responseObserver) {
        super.unregisterNode(request, responseObserver);
    }

    @Override
    public void heartbeat(HeartbeatRequest request,
        StreamObserver<HeartbeatReply> responseObserver) {
        LOGGER.debug("Received HeartbeatRequest {}", TextFormat.shortDebugString(request));

        metadataStore.keepAlive(request.getId(), request.getEpoch(), request.getGoingAway());

        Status status = Status.newBuilder().setCode(Code.OK).build();
        HeartbeatReply reply = HeartbeatReply.newBuilder().setStatus(status).build();
        responseObserver.onNext(reply);
        responseObserver.onCompleted();
    }

    @Override
    public void createTopic(CreateTopicRequest request, StreamObserver<CreateTopicReply> responseObserver) {
        LOGGER.trace("Received CreateTopicRequest {}", TextFormat.shortDebugString(request));
        if (request.getCount() <= 0) {
            CreateTopicReply reply = CreateTopicReply.newBuilder()
                .setStatus(Status.newBuilder().setCode(Code.BAD_REQUEST).setMessage("Topic queue number needs to be positive").build())
                .build();
            responseObserver.onNext(reply);
            responseObserver.onCompleted();
            return;
        }

        this.metadataStore.createTopic(request)
            .whenCompleteAsync((topicId, e) -> {
                if (null != e) {
                    if (e instanceof ControllerException ex) {
                        CreateTopicReply reply = CreateTopicReply.newBuilder()
                            .setStatus(Status.newBuilder()
                                .setCode(Code.forNumber(ex.getErrorCode()))
                                .setMessage(ex.getMessage())
                                .build())
                            .build();
                        responseObserver.onNext(reply);
                        responseObserver.onCompleted();
                    } else {
                        responseObserver.onError(e);
                    }
                } else {
                    CreateTopicReply reply = CreateTopicReply.newBuilder()
                        .setStatus(Status.newBuilder().setCode(Code.OK).build())
                        .setTopicId(topicId)
                        .build();
                    responseObserver.onNext(reply);
                    responseObserver.onCompleted();
                }
            });
    }

    @Override
    public void describeTopic(DescribeTopicRequest request, StreamObserver<DescribeTopicReply> responseObserver) {
        Long topicId = null;
        if (request.getTopicId() > 0) {
            topicId = request.getTopicId();
        }
        metadataStore.describeTopic(topicId, request.getTopicName()).whenCompleteAsync((topic, e) -> {
            if (null != e) {
                if (e.getCause() instanceof ControllerException ex) {
                    DescribeTopicReply reply = DescribeTopicReply.newBuilder()
                        .setStatus(Status.newBuilder()
                            .setCode(Code.forNumber(ex.getErrorCode()))
                            .setMessage(e.getMessage()).build())
                        .build();
                    responseObserver.onNext(reply);
                    responseObserver.onCompleted();
                    return;
                }

                responseObserver.onError(e);
                return;
            }

            DescribeTopicReply reply = DescribeTopicReply.newBuilder()
                .setTopic(topic)
                .setStatus(Status.newBuilder().setCode(Code.OK).build())
                .build();
            responseObserver.onNext(reply);
            responseObserver.onCompleted();
        });
    }

    @Override
    public void listTopics(ListTopicsRequest request, StreamObserver<ListTopicsReply> responseObserver) {
        metadataStore.listTopics().whenComplete(((topics, e) -> {
            if (null != e) {
                if (e instanceof ControllerException ex) {
                    ListTopicsReply reply = ListTopicsReply.newBuilder()
                        .setStatus(Status.newBuilder()
                            .setCode(Code.forNumber(ex.getErrorCode()))
                            .setMessage(e.getMessage()).build())
                        .build();
                    responseObserver.onNext(reply);
                    responseObserver.onCompleted();
                } else {
                    responseObserver.onError(e);
                }
            } else {
                for (Topic topic : topics) {
                    ListTopicsReply reply = ListTopicsReply.newBuilder()
                        .setStatus(Status.newBuilder().setCode(Code.OK).build())
                        .setTopic(topic)
                        .build();
                    responseObserver.onNext(reply);
                }
                responseObserver.onCompleted();
            }
        }));
    }

    @Override
    public void updateTopic(UpdateTopicRequest request, StreamObserver<UpdateTopicReply> responseObserver) {
        this.metadataStore.updateTopic(request)
            .whenComplete((res, e) -> {
                if (null != e) {
                    responseObserver.onError(e);
                } else {
                    UpdateTopicReply reply = UpdateTopicReply.newBuilder()
                        .setStatus(
                            Status.newBuilder()
                                .setCode(Code.OK)
                                .build()
                        ).setTopic(res)
                        .build();
                    responseObserver.onNext(reply);
                    responseObserver.onCompleted();
                }
            });
    }

    @Override
    public void deleteTopic(DeleteTopicRequest request, StreamObserver<DeleteTopicReply> responseObserver) {
        this.metadataStore.deleteTopic(request.getTopicId()).whenComplete((res, e) -> {
            if (null != e) {
                if (e.getCause() instanceof ControllerException ex) {
                    DeleteTopicReply reply = DeleteTopicReply.newBuilder()
                        .setStatus(Status.newBuilder().setCode(Code.forNumber(ex.getErrorCode())).build()
                        ).build();
                    responseObserver.onNext(reply);
                    responseObserver.onCompleted();
                    return;
                }
                responseObserver.onError(e);
            } else {
                DeleteTopicReply reply = DeleteTopicReply.newBuilder()
                    .setStatus(Status.newBuilder().setCode(Code.OK).build()
                    ).build();
                responseObserver.onNext(reply);
                responseObserver.onCompleted();
            }
        });
    }

    @Override
    public void reassignMessageQueue(ReassignMessageQueueRequest request,
        StreamObserver<ReassignMessageQueueReply> responseObserver) {
        metadataStore.reassignMessageQueue(request.getQueue().getTopicId(), request.getQueue().getQueueId(), request.getDstNodeId()).whenComplete((res, e) -> {
            if (null != e) {
                responseObserver.onError(e);
            } else {
                ReassignMessageQueueReply reply = ReassignMessageQueueReply.newBuilder()
                    .setStatus(Status.newBuilder().setCode(Code.OK).build()).build();
                responseObserver.onNext(reply);
                responseObserver.onCompleted();
            }
        });
    }

    @Override
    public void notifyMessageQueueAssignable(NotifyMessageQueuesAssignableRequest request,
        StreamObserver<NotifyMessageQueuesAssignableReply> responseObserver) {
        AtomicInteger successCount = new AtomicInteger(0);
        for (MessageQueue messageQueue : request.getQueuesList()) {
            this.metadataStore.markMessageQueueAssignable(messageQueue.getTopicId(), messageQueue.getQueueId()).whenComplete((res, e) -> {
                if (null != e) {
                    if (e instanceof ControllerException ex) {
                        NotifyMessageQueuesAssignableReply reply = NotifyMessageQueuesAssignableReply.newBuilder()
                            .setStatus(Status.newBuilder()
                                .setCode(Code.forNumber(ex.getErrorCode()))
                                .setMessage(e.getMessage()).build())
                            .build();
                        responseObserver.onNext(reply);
                        responseObserver.onCompleted();
                    } else {
                        responseObserver.onError(e);
                    }
                } else {
                    successCount.incrementAndGet();
                }
            });
        }
        if (successCount.get() == request.getQueuesList().size()) {
            NotifyMessageQueuesAssignableReply reply = NotifyMessageQueuesAssignableReply.newBuilder()
                .setStatus(Status.newBuilder().setCode(Code.OK).build()).build();
            responseObserver.onNext(reply);
            responseObserver.onCompleted();
        }
    }

    @Override
    public void listMessageQueueReassignments(ListMessageQueueReassignmentsRequest request,
        StreamObserver<ListMessageQueueReassignmentsReply> responseObserver) {
        super.listMessageQueueReassignments(request, responseObserver);
    }

    @Override
    public void commitOffset(CommitOffsetRequest request, StreamObserver<CommitOffsetReply> responseObserver) {
        metadataStore.commitOffset(request.getGroupId(),
                request.getQueue().getTopicId(),
                request.getQueue().getQueueId(),
                request.getOffset())
            .whenComplete((res, e) -> {
                if (null != e) {
                    if (e instanceof ControllerException ex) {
                        CommitOffsetReply reply = CommitOffsetReply.newBuilder()
                            .setStatus(Status.newBuilder()
                                .setCode(Code.forNumber(ex.getErrorCode()))
                                .setMessage(e.getMessage()).build())
                            .build();
                        responseObserver.onNext(reply);
                        responseObserver.onCompleted();
                    } else {
                        responseObserver.onError(e);
                    }
                } else {
                    CommitOffsetReply reply = CommitOffsetReply.newBuilder()
                        .setStatus(Status.newBuilder().setCode(Code.OK).build())
                        .build();
                    responseObserver.onNext(reply);
                    responseObserver.onCompleted();
                }
            });
    }

    @Override
    public void createGroup(CreateGroupRequest request, StreamObserver<CreateGroupReply> responseObserver) {
        if (request.getSubMode() == SubscriptionMode.SUB_MODE_UNSPECIFIED) {
            CreateGroupReply reply = CreateGroupReply.newBuilder()
                .setStatus(Status.newBuilder().setCode(Code.BAD_REQUEST).setMessage("SubMode is required").build())
                .build();
            responseObserver.onNext(reply);
            responseObserver.onCompleted();
            return;
        }
        metadataStore.createGroup(request)
            .whenComplete((groupId, e) -> {
                if (null != e) {
                    if (e instanceof ControllerException ex) {
                        CreateGroupReply reply = CreateGroupReply.newBuilder()
                            .setStatus(Status.newBuilder()
                                .setCode(Code.forNumber(ex.getErrorCode()))
                                .setMessage(ex.getMessage())
                                .build())
                            .build();
                        responseObserver.onNext(reply);
                        responseObserver.onCompleted();
                    } else {
                        responseObserver.onError(e);
                    }
                } else {
                    CreateGroupReply reply = CreateGroupReply.newBuilder()
                        .setGroupId(groupId)
                        .setStatus(Status.newBuilder().setCode(Code.OK).build())
                        .build();
                    responseObserver.onNext(reply);
                    responseObserver.onCompleted();
                }
            });
    }

    @Override
    public void describeGroup(DescribeGroupRequest request, StreamObserver<DescribeGroupReply> responseObserver) {
        Long groupId = null;
        if (request.getId() > 0) {
            groupId = request.getId();
        }
        metadataStore.describeGroup(groupId, request.getName()).whenComplete(((group, e) -> {
            if (null != e) {
                if (e.getCause() instanceof ControllerException ex) {
                    DescribeGroupReply reply = DescribeGroupReply.newBuilder()
                        .setStatus(Status.newBuilder()
                            .setCode(Code.forNumber(ex.getErrorCode()))
                            .setMessage(ex.getMessage()).build())
                        .build();
                    responseObserver.onNext(reply);
                    responseObserver.onCompleted();
                    return;
                }

                responseObserver.onError(e);
                return;
            }

            DescribeGroupReply reply = DescribeGroupReply.newBuilder()
                .setGroup(group)
                .setStatus(Status.newBuilder().setCode(Code.OK).build())
                .build();
            responseObserver.onNext(reply);
            responseObserver.onCompleted();
        }));
    }

    @Override
    public void updateGroup(UpdateGroupRequest request, StreamObserver<UpdateGroupReply> responseObserver) {
        metadataStore.updateGroup(request).whenComplete((res, e) -> {
            if (null != e) {
                responseObserver.onError(e);
                return;
            }
            UpdateGroupReply reply = UpdateGroupReply.newBuilder()
                .setStatus(Status.newBuilder().setCode(Code.OK).build())
                .build();
            responseObserver.onNext(reply);
            responseObserver.onCompleted();
        });
    }

    @Override
    public void deleteGroup(DeleteGroupRequest request, StreamObserver<DeleteGroupReply> responseObserver) {
        metadataStore.deleteGroup(request.getId()).whenComplete((res, e) -> {
            if (null != e) {
                if (e.getCause() instanceof ControllerException ex) {
                    DeleteGroupReply reply = DeleteGroupReply.newBuilder()
                        .setStatus(Status.newBuilder()
                            .setCode(Code.forNumber(ex.getErrorCode()))
                            .setMessage(ex.getMessage())
                            .build())
                        .build();
                    responseObserver.onNext(reply);
                    responseObserver.onCompleted();
                    return;
                }
                responseObserver.onError(e);
                return;
            }

            responseObserver.onNext(DeleteGroupReply.newBuilder()
                .setStatus(Status.newBuilder().setCode(Code.OK).build()).build());
            responseObserver.onCompleted();
        });
    }

    @Override
    public void listGroups(ListGroupRequest request, StreamObserver<ListGroupReply> responseObserver) {
        metadataStore.listGroups().whenComplete(((groups, e) -> {
            if (null != e) {
                responseObserver.onError(e);
                return;
            }

            for (ConsumerGroup group : groups) {
                ListGroupReply reply = ListGroupReply.newBuilder()
                    .setStatus(Status.newBuilder().setCode(Code.OK).build())
                    .setGroup(group)
                    .build();
                responseObserver.onNext(reply);
            }
            responseObserver.onCompleted();
        }));
    }

    @Override
    public void openStream(OpenStreamRequest request, StreamObserver<OpenStreamReply> responseObserver) {
        metadataStore.openStream(request.getStreamId(), request.getStreamEpoch(), request.getBrokerId()).whenComplete((metadata, e) -> {
            if (null != e) {
                if (e instanceof ControllerException ex) {
                    OpenStreamReply reply = OpenStreamReply.newBuilder()
                        .setStatus(Status.newBuilder()
                            .setCode(Code.forNumber(ex.getErrorCode()))
                            .setMessage(e.getMessage()).build())
                        .build();
                    responseObserver.onNext(reply);
                    responseObserver.onCompleted();
                    return;
                }
                responseObserver.onError(e);
            }
            OpenStreamReply reply = OpenStreamReply.newBuilder()
                .setStreamMetadata(metadata)
                .setStatus(Status.newBuilder().setCode(Code.OK).build())
                .build();
            responseObserver.onNext(reply);
            responseObserver.onCompleted();
        });
    }

    @Override
    public void closeStream(CloseStreamRequest request, StreamObserver<CloseStreamReply> responseObserver) {
        metadataStore.closeStream(request.getStreamId(), request.getStreamEpoch(), request.getBrokerId()).whenComplete((res, e) -> {
            if (null != e) {
                if (e instanceof ControllerException ex) {
                    CloseStreamReply reply = CloseStreamReply.newBuilder()
                        .setStatus(Status.newBuilder()
                            .setCode(Code.forNumber(ex.getErrorCode()))
                            .setMessage(e.getMessage()).build())
                        .build();
                    responseObserver.onNext(reply);
                    responseObserver.onCompleted();
                } else {
                    responseObserver.onError(e);
                }
            } else {
                CloseStreamReply reply = CloseStreamReply.newBuilder()
                    .setStatus(Status.newBuilder().setCode(Code.OK).build())
                    .build();
                responseObserver.onNext(reply);
                responseObserver.onCompleted();
            }
        });
    }

    @Override
    public void listOpenStreams(ListOpenStreamsRequest request,
        StreamObserver<ListOpenStreamsReply> responseObserver) {
        metadataStore.listOpenStreams(request.getBrokerId())
            .whenComplete((metadataList, e) -> {
                if (null != e) {
                    responseObserver.onError(e);
                    return;
                }
                ListOpenStreamsReply reply = ListOpenStreamsReply.newBuilder()
                    .setStatus(Status.newBuilder()
                        .setCode(Code.OK)
                        .build())
                    .addAllStreamMetadata(metadataList)
                    .build();
                responseObserver.onNext(reply);
                responseObserver.onCompleted();
            });
    }

    @Override
    public void terminateNode(TerminateNodeRequest request, StreamObserver<TerminateNodeReply> responseObserver) {
        if (request.getNodeId() != metadataStore.config().nodeId()) {
            String message = String.format("Target node-id=%d, actual node-id=%d",
                request.getNodeId(), metadataStore.config().nodeId());
            TerminateNodeReply reply = TerminateNodeReply.newBuilder()
                .setStatus(Status.newBuilder()
                    .setCode(Code.BAD_REQUEST).setMessage(message).build())
                .build();
            responseObserver.onNext(reply);
            responseObserver.onCompleted();
            return;
        }

        TerminationStage stage = metadataStore.fireClose();
        TerminateNodeReply reply = TerminateNodeReply.newBuilder()
            .setStatus(Status.newBuilder().setCode(Code.OK).build())
            .setStage(stage)
            .build();
        responseObserver.onNext(reply);

        Context context = Context.current();
        Runnable task = new TerminationStageTask(metadataStore, executorService, context, responseObserver);
        this.executorService.schedule(task, 1, TimeUnit.SECONDS);
    }

    @Override
    public void describeStream(DescribeStreamRequest request, StreamObserver<DescribeStreamReply> responseObserver) {
        metadataStore.describeStream(request).whenComplete((res, e) -> {
            if (null != e) {
                responseObserver.onError(e);
                return;
            }
            responseObserver.onNext(res);
            responseObserver.onCompleted();
        });
    }
}
