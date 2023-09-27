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
import apache.rocketmq.controller.v1.ListOpeningStreamsReply;
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
import apache.rocketmq.controller.v1.StreamMetadata;
import apache.rocketmq.controller.v1.Topic;
import apache.rocketmq.controller.v1.TrimStreamReply;
import apache.rocketmq.controller.v1.TrimStreamRequest;
import apache.rocketmq.controller.v1.UpdateTopicRequest;
import com.automq.rocketmq.controller.exception.ControllerException;
import com.automq.rocketmq.controller.metadata.MetadataStore;
import com.automq.rocketmq.controller.metadata.database.dao.Node;
import com.google.protobuf.TextFormat;
import io.grpc.stub.StreamObserver;

import java.util.List;

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
            Node node = metadataStore.registerBrokerNode(request.getBrokerName(), request.getAddress(),
                request.getInstanceId());
            NodeRegistrationReply reply = NodeRegistrationReply.newBuilder()
                .setStatus(Status.newBuilder().setCode(Code.OK).build())
                .setId(node.getId())
                .setEpoch(node.getEpoch())
                .build();
            responseObserver.onNext(reply);
            responseObserver.onCompleted();
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
            long topicId = this.metadataStore.createTopic(request.getTopic(), request.getCount());
            CreateTopicReply reply = CreateTopicReply.newBuilder()
                .setStatus(Status.newBuilder().setCode(Code.OK).build())
                .setTopicId(topicId)
                .build();
            responseObserver.onNext(reply);
            responseObserver.onCompleted();
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
            Topic topic = metadataStore.describeTopic(request.getTopicId(), request.getTopicName());
            DescribeTopicReply reply;
            if (null != topic) {
                reply = DescribeTopicReply.newBuilder()
                    .setTopic(topic)
                    .setStatus(Status.newBuilder().setCode(Code.OK).build())
                    .build();
            } else {
                reply = DescribeTopicReply.newBuilder()
                    .setStatus(Status.newBuilder().setCode(Code.NOT_FOUND).build())
                    .build();
            }
            responseObserver.onNext(reply);
            responseObserver.onCompleted();
        } catch (ControllerException e) {
            responseObserver.onError(e);
        }

    }

    @Override
    public void listAllTopics(ListTopicsRequest request, StreamObserver<ListTopicsReply> responseObserver) {
        try {
            List<Topic> topics = metadataStore.listTopics();
            for (Topic topic : topics) {
                ListTopicsReply reply = ListTopicsReply.newBuilder()
                    .setStatus(Status.newBuilder().setCode(Code.OK).build())
                    .setTopic(topic)
                    .build();
                responseObserver.onNext(reply);
            }
            responseObserver.onCompleted();
        } catch (ControllerException e) {
            responseObserver.onError(e);
        }
    }

    @Override
    public void updateTopic(UpdateTopicRequest request, StreamObserver<Topic> responseObserver) {
        super.updateTopic(request, responseObserver);
    }

    @Override
    public void deleteTopic(DeleteTopicRequest request, StreamObserver<DeleteTopicReply> responseObserver) {
        try {
            this.metadataStore.deleteTopic(request.getTopicId());
            DeleteTopicReply reply = DeleteTopicReply.newBuilder()
                .setStatus(Status.newBuilder().setCode(Code.OK).build()
                ).build();
            responseObserver.onNext(reply);
            responseObserver.onCompleted();
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
            metadataStore.reassignMessageQueue(request.getQueue().getTopicId(), request.getQueue().getQueueId(), request.getDstNodeId());
            ReassignMessageQueueReply reply = ReassignMessageQueueReply.newBuilder()
                .setStatus(Status.newBuilder().setCode(Code.OK).build()).build();
            responseObserver.onNext(reply);
            responseObserver.onCompleted();
        } catch (ControllerException e) {
            responseObserver.onError(e);
        }
    }

    @Override
    public void notifyMessageQueueAssignable(NotifyMessageQueuesAssignableRequest request,
        StreamObserver<NotifyMessageQueuesAssignableReply> responseObserver) {
        try {
            for (MessageQueue messageQueue : request.getQueuesList()) {
                this.metadataStore.markMessageQueueAssignable(messageQueue.getTopicId(), messageQueue.getQueueId());
            }
            NotifyMessageQueuesAssignableReply reply = NotifyMessageQueuesAssignableReply.newBuilder()
                .setStatus(Status.newBuilder().setCode(Code.OK).build()).build();
            responseObserver.onNext(reply);
            responseObserver.onCompleted();
        } catch (ControllerException e) {
            responseObserver.onError(e);
        }
    }

    @Override
    public void listMessageQueueReassignments(ListMessageQueueReassignmentsRequest request,
        StreamObserver<ListMessageQueueReassignmentsReply> responseObserver) {
        super.listMessageQueueReassignments(request, responseObserver);
    }

    @Override
    public void commitOffset(CommitOffsetRequest request, StreamObserver<CommitOffsetReply> responseObserver) {
        try {
            metadataStore.commitOffset(request.getGroupId(),
                request.getQueue().getTopicId(),
                request.getQueue().getQueueId(),
                request.getOffset());
            CommitOffsetReply reply = CommitOffsetReply.newBuilder()
                .setStatus(Status.newBuilder().setCode(Code.OK).build())
                .build();
            responseObserver.onNext(reply);
            responseObserver.onCompleted();
        } catch (ControllerException e) {
            responseObserver.onError(e);
        }
    }

    @Override
    public void createGroup(CreateGroupRequest request, StreamObserver<CreateGroupReply> responseObserver) {

        try {
            long groupId = metadataStore.createGroup(request.getName(), request.getMaxRetryAttempt(),
                request.getGroupType(), request.getDeadLetterTopicId());
            CreateGroupReply reply = CreateGroupReply.newBuilder()
                .setGroupId(groupId)
                .setStatus(Status.newBuilder().setCode(Code.OK).build())
                .build();
            responseObserver.onNext(reply);
            responseObserver.onCompleted();
        } catch (ControllerException e) {
            responseObserver.onError(e);
        }
    }

    @Override
    public void createRetryStream(CreateRetryStreamRequest request,
        StreamObserver<CreateRetryStreamReply> responseObserver) {
        try {
            long streamId = metadataStore.getOrCreateRetryStream(request.getGroupName(), request.getQueue().getTopicId(), request.getQueue().getQueueId());
            CreateRetryStreamReply reply = CreateRetryStreamReply.newBuilder()
                .setStatus(Status.newBuilder().setCode(Code.OK).build())
                .setStreamId(streamId)
                .build();
            responseObserver.onNext(reply);
            responseObserver.onCompleted();
        } catch (ControllerException e) {
            responseObserver.onError(e);
        }
    }

    @Override
    public void openStream(OpenStreamRequest request, StreamObserver<OpenStreamReply> responseObserver) {
        try {
            StreamMetadata metadata = metadataStore.openStream(request.getStreamId(), request.getStreamEpoch());
            OpenStreamReply reply = OpenStreamReply.newBuilder()
                .setStreamMetadata(metadata)
                .setStatus(Status.newBuilder().setCode(Code.OK).build())
                .build();
            responseObserver.onNext(reply);
            responseObserver.onCompleted();
        } catch (ControllerException e) {
            OpenStreamReply reply = OpenStreamReply.newBuilder()
                .setStatus(Status.newBuilder()
                    .setCode(Code.forNumber(e.getErrorCode()))
                    .setMessage(e.getMessage()).build())
                .build();
            responseObserver.onNext(reply);
            responseObserver.onCompleted();
        } catch (Throwable e) {
            responseObserver.onError(e);
        }
    }

    @Override
    public void closeStream(CloseStreamRequest request, StreamObserver<CloseStreamReply> responseObserver) {
        try {
            metadataStore.closeStream(request.getStreamId(), request.getStreamEpoch());
            CloseStreamReply reply = CloseStreamReply.newBuilder()
                .setStatus(Status.newBuilder().setCode(Code.OK).build())
                .build();
            responseObserver.onNext(reply);
            responseObserver.onCompleted();
        } catch (ControllerException e) {
            CloseStreamReply reply = CloseStreamReply.newBuilder()
                .setStatus(Status.newBuilder()
                    .setCode(Code.forNumber(e.getErrorCode()))
                    .setMessage(e.getMessage()).build())
                .build();
            responseObserver.onNext(reply);
            responseObserver.onCompleted();
        } catch (Throwable e) {
            responseObserver.onError(e);
        }
    }

    @Override
    public void trimStream(TrimStreamRequest request, StreamObserver<TrimStreamReply> responseObserver) {
        try {
            metadataStore.trimStream(request.getStreamId(), request.getStreamEpoch(), request.getNewStartOffset());
            TrimStreamReply reply = TrimStreamReply.newBuilder()
                .setStatus(Status.newBuilder().setCode(Code.OK).build())
                .build();
            responseObserver.onNext(reply);
            responseObserver.onCompleted();
        } catch (ControllerException e) {
            TrimStreamReply reply = TrimStreamReply.newBuilder()
                .setStatus(Status.newBuilder()
                    .setCode(Code.forNumber(e.getErrorCode()))
                    .setMessage(e.getMessage()).build())
                .build();
            responseObserver.onNext(reply);
            responseObserver.onCompleted();
        } catch (Throwable e) {
            responseObserver.onError(e);
        }
    }

    @Override
    public void listOpenStreams(ListOpenStreamsRequest request,
        StreamObserver<ListOpeningStreamsReply> responseObserver) {
        super.listOpenStreams(request, responseObserver);
    }

    @Override
    public void prepareS3Objects(PrepareS3ObjectsRequest request,
        StreamObserver<PrepareS3ObjectsReply> responseObserver) {
        super.prepareS3Objects(request, responseObserver);
    }

    @Override
    public void commitWALObject(CommitWALObjectRequest
        request, StreamObserver<CommitWALObjectReply> responseObserver) {
        super.commitWALObject(request, responseObserver);
    }

    @Override
    public void commitStreamObject(CommitStreamObjectRequest request,
        StreamObserver<CommitStreamObjectReply> responseObserver) {
        super.commitStreamObject(request, responseObserver);
    }
}
