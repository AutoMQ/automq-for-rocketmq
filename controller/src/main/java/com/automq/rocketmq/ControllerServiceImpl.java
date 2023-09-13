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

package com.automq.rocketmq;

import apache.rocketmq.controller.v1.AddOssSegmentReply;
import apache.rocketmq.controller.v1.AddOssSegmentRequest;
import apache.rocketmq.controller.v1.BrokerHeartbeatReply;
import apache.rocketmq.controller.v1.BrokerHeartbeatRequest;
import apache.rocketmq.controller.v1.BrokerRegistrationReply;
import apache.rocketmq.controller.v1.BrokerRegistrationRequest;
import apache.rocketmq.controller.v1.BrokerUnregistrationReply;
import apache.rocketmq.controller.v1.BrokerUnregistrationRequest;
import apache.rocketmq.controller.v1.Code;
import apache.rocketmq.controller.v1.CommitOffsetReply;
import apache.rocketmq.controller.v1.CommitOffsetRequest;
import apache.rocketmq.controller.v1.ControllerServiceGrpc;
import apache.rocketmq.controller.v1.CreateTopicReply;
import apache.rocketmq.controller.v1.CreateTopicRequest;
import apache.rocketmq.controller.v1.DeleteOssSegmentReply;
import apache.rocketmq.controller.v1.DeleteOssSegmentRequest;
import apache.rocketmq.controller.v1.DeleteTopicReply;
import apache.rocketmq.controller.v1.DeleteTopicRequest;
import apache.rocketmq.controller.v1.DescribeTopicReply;
import apache.rocketmq.controller.v1.DescribeTopicRequest;
import apache.rocketmq.controller.v1.ListMessageQueueReassignmentsReply;
import apache.rocketmq.controller.v1.ListMessageQueueReassignmentsRequest;
import apache.rocketmq.controller.v1.ListOssSegmentsRequest;
import apache.rocketmq.controller.v1.ListOssSegmentsResponse;
import apache.rocketmq.controller.v1.ListTopicMessageQueueAssignmentsReply;
import apache.rocketmq.controller.v1.ListTopicMessageQueueAssignmentsRequest;
import apache.rocketmq.controller.v1.ListTopicsReply;
import apache.rocketmq.controller.v1.ListTopicsRequest;
import apache.rocketmq.controller.v1.ReassignMessageQueueReply;
import apache.rocketmq.controller.v1.ReassignMessageQueueRequest;
import apache.rocketmq.controller.v1.Status;
import apache.rocketmq.controller.v1.Topic;
import apache.rocketmq.controller.v1.UpdateTopicRequest;
import com.google.protobuf.TextFormat;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ControllerServiceImpl extends ControllerServiceGrpc.ControllerServiceImplBase {

    private static final Logger LOGGER = LoggerFactory.getLogger(ControllerServiceImpl.class);

    @Override
    public void registerBroker(BrokerRegistrationRequest request, StreamObserver<BrokerRegistrationReply> responseObserver) {
        super.registerBroker(request, responseObserver);
    }

    @Override
    public void unregisterBroker(BrokerUnregistrationRequest request, StreamObserver<BrokerUnregistrationReply> responseObserver) {
        super.unregisterBroker(request, responseObserver);
    }

    @Override
    public void processBrokerHeartbeat(BrokerHeartbeatRequest request,
                                       StreamObserver<BrokerHeartbeatReply> responseObserver) {
        if (null != request) {
            LOGGER.trace("Received BrokerHeartbeatRequest {}", TextFormat.shortDebugString(request));
        }

        Status status = Status.newBuilder().setCode(Code.OK).build();
        BrokerHeartbeatReply reply = BrokerHeartbeatReply.newBuilder().setStatus(status).build();
        responseObserver.onNext(reply);
        responseObserver.onCompleted();
    }

    @Override
    public void createTopic(CreateTopicRequest request, StreamObserver<CreateTopicReply> responseObserver) {
        super.createTopic(request, responseObserver);
    }

    @Override
    public void describeTopic(DescribeTopicRequest request, StreamObserver<DescribeTopicReply> responseObserver) {
        super.describeTopic(request, responseObserver);
    }

    @Override
    public void listAllTopics(ListTopicsRequest request, StreamObserver<ListTopicsReply> responseObserver) {
        super.listAllTopics(request, responseObserver);
    }

    @Override
    public void updateTopic(UpdateTopicRequest request, StreamObserver<Topic> responseObserver) {
        super.updateTopic(request, responseObserver);
    }

    @Override
    public void deleteTopic(DeleteTopicRequest request, StreamObserver<DeleteTopicReply> responseObserver) {
        super.deleteTopic(request, responseObserver);
    }

    @Override
    public void listTopicMessageQueues(ListTopicMessageQueueAssignmentsRequest request, StreamObserver<ListTopicMessageQueueAssignmentsReply> responseObserver) {
        super.listTopicMessageQueues(request, responseObserver);
    }

    @Override
    public void reassignMessageQueue(ReassignMessageQueueRequest request, StreamObserver<ReassignMessageQueueReply> responseObserver) {
        super.reassignMessageQueue(request, responseObserver);
    }

    @Override
    public void listMessageQueueReassignments(ListMessageQueueReassignmentsRequest request, StreamObserver<ListMessageQueueReassignmentsReply> responseObserver) {
        super.listMessageQueueReassignments(request, responseObserver);
    }

    @Override
    public void addOssSegment(AddOssSegmentRequest request, StreamObserver<AddOssSegmentReply> responseObserver) {
        super.addOssSegment(request, responseObserver);
    }

    @Override
    public void listOssSegments(ListOssSegmentsRequest request, StreamObserver<ListOssSegmentsResponse> responseObserver) {
        super.listOssSegments(request, responseObserver);
    }

    @Override
    public void deleteOssSegment(DeleteOssSegmentRequest request, StreamObserver<DeleteOssSegmentReply> responseObserver) {
        super.deleteOssSegment(request, responseObserver);
    }

    @Override
    public void commitOffset(CommitOffsetRequest request, StreamObserver<CommitOffsetReply> responseObserver) {
        super.commitOffset(request, responseObserver);
    }
}
