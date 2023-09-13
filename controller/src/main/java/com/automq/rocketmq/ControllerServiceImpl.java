package com.automq.rocketmq;

import apache.rocketmq.controller.v1.*;
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
