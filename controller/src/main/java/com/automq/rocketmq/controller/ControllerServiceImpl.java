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

package com.automq.rocketmq.controller;

import apache.rocketmq.controller.v1.CloseStreamReply;
import apache.rocketmq.controller.v1.CloseStreamRequest;
import apache.rocketmq.controller.v1.Code;
import apache.rocketmq.controller.v1.CommitOffsetReply;
import apache.rocketmq.controller.v1.CommitOffsetRequest;
import apache.rocketmq.controller.v1.CommitStreamObjectReply;
import apache.rocketmq.controller.v1.CommitStreamObjectRequest;
import apache.rocketmq.controller.v1.CommitWALObjectReply;
import apache.rocketmq.controller.v1.CommitWALObjectRequest;
import apache.rocketmq.controller.v1.ControllerServiceGrpc;
import apache.rocketmq.controller.v1.CreateGroupReply;
import apache.rocketmq.controller.v1.CreateGroupRequest;
import apache.rocketmq.controller.v1.CreateRetryStreamReply;
import apache.rocketmq.controller.v1.CreateRetryStreamRequest;
import apache.rocketmq.controller.v1.CreateTopicReply;
import apache.rocketmq.controller.v1.CreateTopicRequest;
import apache.rocketmq.controller.v1.DeleteTopicReply;
import apache.rocketmq.controller.v1.DeleteTopicRequest;
import apache.rocketmq.controller.v1.DescribeTopicReply;
import apache.rocketmq.controller.v1.DescribeTopicRequest;
import apache.rocketmq.controller.v1.HeartbeatReply;
import apache.rocketmq.controller.v1.HeartbeatRequest;
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
import apache.rocketmq.controller.v1.PrepareS3ObjectsReply;
import apache.rocketmq.controller.v1.PrepareS3ObjectsRequest;
import apache.rocketmq.controller.v1.ReassignMessageQueueReply;
import apache.rocketmq.controller.v1.ReassignMessageQueueRequest;
import apache.rocketmq.controller.v1.Status;
import apache.rocketmq.controller.v1.Topic;
import apache.rocketmq.controller.v1.TrimStreamReply;
import apache.rocketmq.controller.v1.TrimStreamRequest;
import apache.rocketmq.controller.v1.UpdateTopicReply;
import apache.rocketmq.controller.v1.UpdateTopicRequest;
import com.automq.rocketmq.controller.exception.ControllerException;
import com.automq.rocketmq.controller.metadata.MetadataStore;
import com.google.protobuf.TextFormat;
import io.grpc.stub.StreamObserver;

import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ControllerServiceImpl extends ControllerServiceGrpc.ControllerServiceImplBase {

    private static final Logger LOGGER = LoggerFactory.getLogger(ControllerServiceImpl.class);

    private final MetadataStore metadataStore;

    public ControllerServiceImpl(MetadataStore metadataStore) {
        this.metadataStore = metadataStore;
    }

    @Override
    public void registerNode(NodeRegistrationRequest request,
        StreamObserver<NodeRegistrationReply> responseObserver) {
        try {
            metadataStore.registerBrokerNode(request.getBrokerName(), request.getAddress(),
                request.getInstanceId()).whenComplete((res, e) -> {
                    if (null != e) {
                        if (e instanceof ControllerException ex) {
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
        } catch (ControllerException e) {
            responseObserver.onError(e);
        }
    }

    @Override
    public void unregisterNode(NodeUnregistrationRequest request,
        StreamObserver<NodeUnregistrationReply> responseObserver) {
        super.unregisterNode(request, responseObserver);
    }

    @Override
    public void heartbeat(HeartbeatRequest request,
        StreamObserver<HeartbeatReply> responseObserver) {
        LOGGER.trace("Received HeartbeatRequest {}", TextFormat.shortDebugString(request));

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

        try {
            this.metadataStore.createTopic(request.getTopic(), request.getCount(), request.getAcceptMessageTypesList()).whenCompleteAsync((topicId, e) -> {
                if (null != e) {
                    responseObserver.onError(e);
                } else {
                    CreateTopicReply reply = CreateTopicReply.newBuilder()
                        .setStatus(Status.newBuilder().setCode(Code.OK).build())
                        .setTopicId(topicId)
                        .build();
                    responseObserver.onNext(reply);
                    responseObserver.onCompleted();
                }
            });
        } catch (ControllerException e) {
            if (Code.DUPLICATED_VALUE == e.getErrorCode()) {
                CreateTopicReply reply = CreateTopicReply.newBuilder()
                    .setStatus(Status.newBuilder()
                        .setCode(Code.DUPLICATED)
                        .setMessage(String.format("%s is already taken", request.getTopic()))
                        .build())
                    .build();
                responseObserver.onNext(reply);
                responseObserver.onCompleted();
                return;
            }
            responseObserver.onError(e);
        }
    }

    @Override
    public void describeTopic(DescribeTopicRequest request, StreamObserver<DescribeTopicReply> responseObserver) {
        try {
            metadataStore.describeTopic(request.getTopicId(), request.getTopicName()).whenCompleteAsync((topic, e) -> {
                if (null != e) {
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
        } catch (ControllerException e) {
            DescribeTopicReply reply = DescribeTopicReply.newBuilder()
                .setStatus(Status.newBuilder()
                    .setCode(Code.forNumber(e.getErrorCode())).setMessage(e.getMessage()).build()).build();
            responseObserver.onNext(reply);
            responseObserver.onCompleted();
        }
    }

    @Override
    public void listAllTopics(ListTopicsRequest request, StreamObserver<ListTopicsReply> responseObserver) {
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
        try {
            this.metadataStore.updateTopic(request.getTopicId(), request.getName(), request.getAcceptMessageTypesList()).whenComplete((res, e) -> {
                if (null != e) {
                    responseObserver.onError(e);
                } else {
                    UpdateTopicReply reply = UpdateTopicReply.newBuilder()
                        .setStatus(
                            Status.newBuilder()
                                .setCode(Code.OK)
                                .build()
                            )
                        .setTopic(res)
                        .build();
                    responseObserver.onNext(reply);
                    responseObserver.onCompleted();
                }
            });
        } catch (ControllerException e) {
            if (e.getErrorCode() == Code.NOT_FOUND_VALUE) {
                UpdateTopicReply reply = UpdateTopicReply.newBuilder()
                    .setStatus(Status.newBuilder().setCode(Code.NOT_FOUND).build()
                    ).build();
                responseObserver.onNext(reply);
                responseObserver.onCompleted();
                return;
            }
            responseObserver.onError(e);
        }
    }

    @Override
    public void deleteTopic(DeleteTopicRequest request, StreamObserver<DeleteTopicReply> responseObserver) {
        try {
            this.metadataStore.deleteTopic(request.getTopicId()).whenComplete((res, e) -> {
                if (null != e) {
                    responseObserver.onError(e);
                } else {
                    DeleteTopicReply reply = DeleteTopicReply.newBuilder()
                        .setStatus(Status.newBuilder().setCode(Code.OK).build()
                        ).build();
                    responseObserver.onNext(reply);
                    responseObserver.onCompleted();
                }
            });
        } catch (ControllerException e) {
            if (e.getErrorCode() == Code.NOT_FOUND_VALUE) {
                DeleteTopicReply reply = DeleteTopicReply.newBuilder()
                    .setStatus(Status.newBuilder().setCode(Code.NOT_FOUND).build()
                    ).build();
                responseObserver.onNext(reply);
                responseObserver.onCompleted();
                return;
            }
            responseObserver.onError(e);
        }
    }

    @Override
    public void reassignMessageQueue(ReassignMessageQueueRequest request,
        StreamObserver<ReassignMessageQueueReply> responseObserver) {
        try {
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
        } catch (ControllerException e) {
            ReassignMessageQueueReply reply = ReassignMessageQueueReply.newBuilder()
                .setStatus(Status.newBuilder()
                    .setCode(Code.forNumber(e.getErrorCode()))
                    .setMessage(e.getMessage()).build())
                .build();
            responseObserver.onNext(reply);
            responseObserver.onCompleted();
        }
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
            request.getOffset()).whenComplete((res, e) -> {
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
        try {
            metadataStore.createGroup(request.getName(), request.getMaxRetryAttempt(), request.getGroupType(),
                request.getDeadLetterTopicId()).whenComplete((groupId, e) -> {
                    if (null != e) {
                        responseObserver.onError(e);
                    } else {
                        CreateGroupReply reply = CreateGroupReply.newBuilder()
                            .setGroupId(groupId)
                            .setStatus(Status.newBuilder().setCode(Code.OK).build())
                            .build();
                        responseObserver.onNext(reply);
                        responseObserver.onCompleted();
                    }
                });
        } catch (ControllerException e) {
            CreateGroupReply reply = CreateGroupReply.newBuilder()
                .setStatus(Status.newBuilder()
                    .setCode(Code.forNumber(e.getErrorCode()))
                    .setMessage(e.getMessage()).build())
                .build();
            responseObserver.onNext(reply);
            responseObserver.onCompleted();
        }
    }

    @Override
    public void createRetryStream(CreateRetryStreamRequest request,
        StreamObserver<CreateRetryStreamReply> responseObserver) {
        try {
            metadataStore.getOrCreateRetryStream(request.getGroupName(), request.getQueue().getTopicId(), request.getQueue().getQueueId()).whenComplete((streamId, e) -> {
                if (null != e) {
                    responseObserver.onError(e);
                } else {
                    CreateRetryStreamReply reply = CreateRetryStreamReply.newBuilder()
                        .setStatus(Status.newBuilder().setCode(Code.OK).build())
                        .setStreamId(streamId)
                        .build();
                    responseObserver.onNext(reply);
                    responseObserver.onCompleted();
                }
            });
        } catch (ControllerException e) {
            CreateRetryStreamReply reply = CreateRetryStreamReply.newBuilder()
                .setStatus(Status.newBuilder()
                    .setCode(Code.forNumber(e.getErrorCode()))
                    .setMessage(e.getMessage()).build())
                .build();
            responseObserver.onNext(reply);
            responseObserver.onCompleted();
        }
    }

    @Override
    public void openStream(OpenStreamRequest request, StreamObserver<OpenStreamReply> responseObserver) {
        metadataStore.openStream(request.getStreamId(), request.getStreamEpoch()).whenComplete((metadata, e) -> {
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
        metadataStore.closeStream(request.getStreamId(), request.getStreamEpoch()).whenComplete((res, e) -> {
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
    public void trimStream(TrimStreamRequest request, StreamObserver<TrimStreamReply> responseObserver) {
        try {
            metadataStore.trimStream(request.getStreamId(), request.getStreamEpoch(), request.getNewStartOffset()).whenComplete((res, e) -> {
                if (null != e) {
                    responseObserver.onError(e);
                } else {
                    TrimStreamReply reply = TrimStreamReply.newBuilder()
                        .setStatus(Status.newBuilder().setCode(Code.OK).build())
                        .build();
                    responseObserver.onNext(reply);
                    responseObserver.onCompleted();
                }
            });
        } catch (ControllerException e) {
            TrimStreamReply reply = TrimStreamReply.newBuilder()
                .setStatus(Status.newBuilder()
                    .setCode(Code.forNumber(e.getErrorCode()))
                    .setMessage(e.getMessage()).build())
                .build();
            responseObserver.onNext(reply);
            responseObserver.onCompleted();
        }
    }

    @Override
    public void listOpenStreams(ListOpenStreamsRequest request,
        StreamObserver<ListOpenStreamsReply> responseObserver) {
        metadataStore.listOpenStreams(request.getBrokerId(), request.getBrokerEpoch())
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
    public void prepareS3Objects(PrepareS3ObjectsRequest request,
        StreamObserver<PrepareS3ObjectsReply> responseObserver) {
        metadataStore.prepareS3Objects(request.getPreparedCount(), (int) request.getTimeToLiveMinutes())
            .whenComplete((objectId, e) -> {
                if (null != e) {
                    responseObserver.onError(e);
                    return;
                }

                PrepareS3ObjectsReply reply = PrepareS3ObjectsReply.newBuilder()
                    .setStatus(Status.newBuilder()
                        .setCode(Code.OK).build())
                    .setFirstObjectId(objectId)
                    .build();
                responseObserver.onNext(reply);
                responseObserver.onCompleted();
            });
    }

    @Override
    public void commitWALObject(CommitWALObjectRequest
        request, StreamObserver<CommitWALObjectReply> responseObserver) {
        try {
            metadataStore.commitWalObject(request.getS3WalObject(), request.getS3StreamObjectsList(), request.getCompactedObjectIdsList())
                .whenComplete((res, e) -> {
                    if (null != e) {
                        responseObserver.onError(e);
                    } else {
                        CommitWALObjectReply reply = CommitWALObjectReply.newBuilder()
                            .setStatus(Status.newBuilder().setCode(Code.OK).build())
                            .build();
                        responseObserver.onNext(reply);
                        responseObserver.onCompleted();
                    }
                });
        } catch (ControllerException e) {
            CommitWALObjectReply reply = CommitWALObjectReply.newBuilder()
                .setStatus(Status.newBuilder()
                    .setCode(Code.forNumber(e.getErrorCode()))
                    .setMessage(e.getMessage()).build())
                .build();
            responseObserver.onNext(reply);
            responseObserver.onCompleted();
        }
    }

    @SuppressWarnings("checkstyle:Indentation")
    @Override
    public void commitStreamObject(CommitStreamObjectRequest request,
        StreamObserver<CommitStreamObjectReply> responseObserver) {

        try {
            metadataStore.commitStreamObject(request.getS3StreamObject(), request.getCompactedObjectIdsList())
                .whenComplete((res, e) -> {
                    if (null != e) {
                        responseObserver.onError(e);
                    } else {
                        CommitStreamObjectReply reply = CommitStreamObjectReply.newBuilder()
                            .setStatus(Status.newBuilder().setCode(Code.OK).build())
                            .build();
                        responseObserver.onNext(reply);
                        responseObserver.onCompleted();
                    }
                });
        } catch (ControllerException e) {
            CommitStreamObjectReply reply = CommitStreamObjectReply.newBuilder()
                .setStatus(Status.newBuilder()
                    .setCode(Code.forNumber(e.getErrorCode()))
                    .setMessage(e.getMessage()).build())
                .build();
            responseObserver.onNext(reply);
            responseObserver.onCompleted();
        }
    }
}
