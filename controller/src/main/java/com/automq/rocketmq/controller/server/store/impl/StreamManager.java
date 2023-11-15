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

package com.automq.rocketmq.controller.server.store.impl;

import apache.rocketmq.common.v1.Code;
import apache.rocketmq.controller.v1.CloseStreamRequest;
import apache.rocketmq.controller.v1.DescribeStreamReply;
import apache.rocketmq.controller.v1.DescribeStreamRequest;
import apache.rocketmq.controller.v1.GroupStatus;
import apache.rocketmq.controller.v1.ListOpenStreamsReply;
import apache.rocketmq.controller.v1.ListOpenStreamsRequest;
import apache.rocketmq.controller.v1.OpenStreamReply;
import apache.rocketmq.controller.v1.OpenStreamRequest;
import apache.rocketmq.controller.v1.Status;
import apache.rocketmq.controller.v1.StreamMetadata;
import apache.rocketmq.controller.v1.StreamRole;
import apache.rocketmq.controller.v1.StreamState;
import com.automq.rocketmq.common.exception.ControllerException;
import com.automq.rocketmq.controller.MetadataStore;
import com.automq.rocketmq.metadata.dao.Group;
import com.automq.rocketmq.metadata.dao.GroupCriteria;
import com.automq.rocketmq.metadata.dao.QueueAssignment;
import com.automq.rocketmq.metadata.dao.Range;
import com.automq.rocketmq.metadata.dao.S3Object;
import com.automq.rocketmq.metadata.dao.S3ObjectCriteria;
import com.automq.rocketmq.metadata.dao.Stream;
import com.automq.rocketmq.metadata.dao.StreamCriteria;
import com.automq.rocketmq.metadata.mapper.GroupMapper;
import com.automq.rocketmq.metadata.mapper.QueueAssignmentMapper;
import com.automq.rocketmq.metadata.mapper.RangeMapper;
import com.automq.rocketmq.metadata.mapper.S3ObjectMapper;
import com.automq.rocketmq.metadata.mapper.S3StreamObjectMapper;
import com.automq.rocketmq.metadata.mapper.StreamMapper;
import com.google.common.util.concurrent.MoreExecutors;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.stream.Collectors;
import org.apache.ibatis.session.SqlSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StreamManager {
    private static final Logger LOGGER = LoggerFactory.getLogger(TopicManager.class);

    private final MetadataStore metadataStore;

    public StreamManager(MetadataStore metadataStore) {
        this.metadataStore = metadataStore;
    }

    public CompletableFuture<StreamMetadata> getStream(long topicId, int queueId, Long groupId, StreamRole streamRole) {
        return CompletableFuture.supplyAsync(() -> {
            try (SqlSession session = metadataStore.openSession()) {
                StreamMapper streamMapper = session.getMapper(StreamMapper.class);
                StreamCriteria criteria = StreamCriteria.newBuilder()
                    .withTopicId(topicId)
                    .withQueueId(queueId)
                    .withGroupId(groupId)
                    .build();
                List<Stream> streams = streamMapper.byCriteria(criteria)
                    .stream()
                    .filter(stream -> stream.getStreamRole() == streamRole).toList();
                if (streams.isEmpty()) {
                    if (streamRole == StreamRole.STREAM_ROLE_RETRY) {
                        QueueAssignmentMapper assignmentMapper = session.getMapper(QueueAssignmentMapper.class);
                        List<QueueAssignment> assignments = assignmentMapper
                            .list(topicId, null, null, null, null)
                            .stream().filter(assignment -> assignment.getQueueId() == queueId)
                            .toList();

                        // Verify assignment and queue are OK
                        if (assignments.isEmpty()) {
                            String msg = String.format("Queue assignment for topic-id=%d queue-id=%d is not found",
                                topicId, queueId);
                            throw new CompletionException(new ControllerException(Code.NOT_FOUND_VALUE, msg));
                        }

                        if (assignments.size() != 1) {
                            String msg = String.format("%d queue assignments for topic-id=%d queue-id=%d is found",
                                assignments.size(), topicId, queueId);
                            throw new CompletionException(new ControllerException(Code.ILLEGAL_STATE_VALUE, msg));
                        }
                        QueueAssignment assignment = assignments.get(0);
                        switch (assignment.getStatus()) {
                            case ASSIGNMENT_STATUS_YIELDING -> {
                                String msg = String.format("Queue[topic-id=%d queue-id=%d] is under migration. " +
                                    "Please create retry stream later", topicId, queueId);
                                throw new CompletionException(new ControllerException(Code.ILLEGAL_STATE_VALUE, msg));
                            }
                            case ASSIGNMENT_STATUS_DELETED -> {
                                String msg = String.format("Queue[topic-id=%d queue-id=%d] has been deleted",
                                    topicId, queueId);
                                throw new CompletionException(new ControllerException(Code.ILLEGAL_STATE_VALUE, msg));
                            }
                            case ASSIGNMENT_STATUS_ASSIGNED -> {
                                // OK
                            }
                            default -> {
                                String msg = String.format("Status of Queue[topic-id=%d queue-id=%d] is unsupported",
                                    topicId, queueId);
                                throw new CompletionException(new ControllerException(Code.INTERNAL_VALUE, msg));
                            }
                        }

                        // Verify Group exists.
                        GroupMapper groupMapper = session.getMapper(GroupMapper.class);
                        List<Group> groups = groupMapper.byCriteria(GroupCriteria.newBuilder()
                            .setGroupId(groupId)
                            .setStatus(GroupStatus.GROUP_STATUS_ACTIVE)
                            .build());
                        if (groups.size() != 1) {
                            String msg = String.format("Group[group-id=%d] is not found", groupId);
                            throw new CompletionException(new ControllerException(Code.NOT_FOUND_VALUE, msg));
                        }

                        int nodeId = assignment.getDstNodeId();
                        long streamId = metadataStore.topicManager().createStream(streamMapper, topicId, queueId, groupId, streamRole, nodeId);
                        session.commit();

                        Stream stream = streamMapper.getByStreamId(streamId);
                        return StreamMetadata.newBuilder()
                            .setStreamId(streamId)
                            .setEpoch(stream.getEpoch())
                            .setRangeId(stream.getRangeId())
                            .setStartOffset(stream.getStartOffset())
                            // Stream is uninitialized, its end offset is definitely 0.
                            .setEndOffset(0)
                            .setState(stream.getState())
                            .build();
                    }

                    // For other types of streams, creation is explicit.
                    ControllerException e = new ControllerException(Code.NOT_FOUND_VALUE,
                        String.format("Stream for topic-id=%d, queue-id=%d, stream-role=%s is not found", topicId, queueId, streamRole.name()));
                    throw new CompletionException(e);
                } else {
                    Stream stream = streams.get(0);
                    long endOffset = 0;
                    switch (stream.getState()) {
                        case UNINITIALIZED -> {
                        }
                        case DELETED -> {
                            ControllerException e = new ControllerException(Code.NOT_FOUND_VALUE,
                                String.format("Stream for topic-id=%d, queue-id=%d, stream-role=%s has been deleted",
                                    topicId, queueId, streamRole.name()));
                            throw new CompletionException(e);
                        }
                        case CLOSING, OPEN, CLOSED -> {
                            RangeMapper rangeMapper = session.getMapper(RangeMapper.class);
                            Range range = rangeMapper.get(stream.getRangeId(), stream.getId(), null);
                            if (null == range) {
                                LOGGER.error("Expected range[range-id={}] of stream[topic-id={}, queue-id={}, " +
                                        "stream-id={}, stream-state={}, role={}] is NOT found", stream.getRangeId(),
                                    stream.getTopicId(), stream.getQueueId(), stream.getId(), stream.getState(),
                                    stream.getStreamRole());
                            }
                            assert null != range;
                            endOffset = range.getEndOffset();
                        }
                    }
                    return StreamMetadata.newBuilder()
                        .setStreamId(stream.getId())
                        .setEpoch(stream.getEpoch())
                        .setRangeId(stream.getRangeId())
                        .setStartOffset(stream.getStartOffset())
                        .setEndOffset(endOffset)
                        .setState(stream.getState())
                        .build();
                }
            }
        }, metadataStore.asyncExecutor());
    }

    public CompletableFuture<StreamMetadata> openStream(long streamId, long epoch, int nodeId) {
        CompletableFuture<StreamMetadata> future = new CompletableFuture<>();
        for (; ; ) {
            if (metadataStore.isLeader()) {
                try (SqlSession session = metadataStore.openSession()) {
                    if (!metadataStore.maintainLeadershipWithSharedLock(session)) {
                        continue;
                    }
                    StreamMapper streamMapper = session.getMapper(StreamMapper.class);
                    RangeMapper rangeMapper = session.getMapper(RangeMapper.class);
                    Stream stream = streamMapper.getByStreamId(streamId);
                    // Verify target stream exists
                    if (null == stream || stream.getState() == StreamState.DELETED) {
                        ControllerException e = new ControllerException(Code.NOT_FOUND_VALUE,
                            String.format("Stream[stream-id=%d] is not found", streamId)
                        );
                        future.completeExceptionally(e);
                        return future;
                    }

                    // Verify stream owner is correct
                    switch (stream.getState()) {
                        case CLOSING -> {
                            // nodeId should be equal to stream.srcNodeId
                            if (nodeId != stream.getSrcNodeId()) {
                                LOGGER.warn("State of Stream[stream-id={}] is {}. Current owner should be {}, while {} " +
                                        "is attempting to open. Fenced!", stream.getId(), stream.getState(), stream.getSrcNodeId(),
                                    nodeId);
                                ControllerException e = new ControllerException(Code.FENCED_VALUE, "Node does not match");
                                future.completeExceptionally(e);
                                return future;
                            }
                        }
                        case OPEN, CLOSED, UNINITIALIZED -> {
                            // nodeId should be equal to stream.dstNodeId
                            if (nodeId != stream.getDstNodeId()) {
                                LOGGER.warn("State of Stream[stream-id={}] is {}. Its current owner is {}, {} is attempting to open. Fenced!",
                                    streamId, stream.getState(), stream.getDstNodeId(), nodeId);
                                ControllerException e = new ControllerException(Code.FENCED_VALUE, "Node does not match");
                                future.completeExceptionally(e);
                                return future;
                            }
                        }
                    }

                    // Verify epoch
                    if (epoch != stream.getEpoch()) {
                        LOGGER.warn("Epoch of Stream[stream-id={}] is {}, while the open stream request epoch is {}",
                            streamId, stream.getEpoch(), epoch);
                        ControllerException e = new ControllerException(Code.FENCED_VALUE, "Epoch of stream is deprecated");
                        future.completeExceptionally(e);
                        return future;
                    }

                    // Verify that current stream state allows open ops
                    switch (stream.getState()) {
                        case CLOSING, OPEN -> {
                            LOGGER.warn("Stream[stream-id={}] is already OPEN with epoch={}", streamId, stream.getEpoch());
                            Range range = rangeMapper.get(stream.getRangeId(), streamId, null);
                            StreamMetadata metadata = StreamMetadata.newBuilder()
                                .setStreamId(streamId)
                                .setEpoch(epoch)
                                .setRangeId(stream.getRangeId())
                                .setStartOffset(stream.getStartOffset())
                                .setEndOffset(range.getEndOffset())
                                .setState(stream.getState())
                                .build();
                            future.complete(metadata);
                            return future;
                        }
                        case UNINITIALIZED, CLOSED -> {
                        }
                        default -> {
                            String msg = String.format("State of Stream[stream-id=%d] is %s, which is not supported",
                                stream.getId(), stream.getState());
                            future.completeExceptionally(new ControllerException(Code.ILLEGAL_STATE_VALUE, msg));
                            return future;
                        }
                    }

                    // Now that the request is valid, update the stream's epoch and create a new range for this broker

                    // If stream.state == uninitialized, its stream.rangeId will be -1;
                    // If stream.state == closed, stream.rangeId will be the previous one;

                    // get new range's start offset
                    long startOffset;
                    if (StreamState.UNINITIALIZED == stream.getState()) {
                        // default regard this range is the first range in stream, use 0 as start offset
                        startOffset = 0;
                    } else {
                        assert StreamState.CLOSED == stream.getState();
                        Range prevRange = rangeMapper.get(stream.getRangeId(), streamId, null);
                        // if stream is closed, use previous range's end offset as start offset
                        startOffset = prevRange.getEndOffset();
                    }

                    // Increase stream epoch
                    stream.setEpoch(epoch + 1);
                    // Increase range-id
                    stream.setRangeId(stream.getRangeId() + 1);
                    stream.setStartOffset(stream.getStartOffset());
                    stream.setState(StreamState.OPEN);
                    streamMapper.update(stream);

                    // Create a new range for the stream
                    Range range = new Range();
                    range.setStreamId(streamId);
                    range.setNodeId(stream.getDstNodeId());
                    range.setStartOffset(startOffset);
                    range.setEndOffset(startOffset);
                    range.setEpoch(epoch + 1);
                    range.setRangeId(stream.getRangeId());
                    rangeMapper.create(range);
                    LOGGER.info("Node[node-id={}] opens stream [stream-id={}] with epoch={}",
                        metadataStore.config().nodeId(), streamId, epoch + 1);
                    // Commit transaction
                    session.commit();

                    // Build open stream response
                    StreamMetadata metadata = StreamMetadata.newBuilder()
                        .setStreamId(streamId)
                        .setEpoch(epoch + 1)
                        .setRangeId(stream.getRangeId())
                        .setStartOffset(stream.getStartOffset())
                        .setEndOffset(range.getEndOffset())
                        .setState(StreamState.OPEN)
                        .build();
                    future.complete(metadata);
                    return future;
                } catch (Throwable e) {
                    LOGGER.error("Unexpected exception raised while open stream", e);
                    future.completeExceptionally(e);
                    return future;
                }
            } else {
                Optional<String> leaderAddress = metadataStore.electionService().leaderAddress();
                if (leaderAddress.isEmpty()) {
                    return CompletableFuture.failedFuture(new ControllerException(Code.NO_LEADER_VALUE, "No leader is elected yet"));
                }
                OpenStreamRequest request = OpenStreamRequest.newBuilder()
                    .setStreamId(streamId)
                    .setStreamEpoch(epoch)
                    .build();
                return metadataStore.controllerClient().openStream(leaderAddress.get(), request)
                    .thenApply(OpenStreamReply::getStreamMetadata);
            }
        }
    }

    public CompletableFuture<Void> closeStream(long streamId, long streamEpoch, int nodeId) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        for (; ; ) {
            if (metadataStore.isLeader()) {
                try (SqlSession session = metadataStore.openSession()) {
                    if (!metadataStore.maintainLeadershipWithSharedLock(session)) {
                        continue;
                    }
                    StreamMapper streamMapper = session.getMapper(StreamMapper.class);

                    Stream stream = streamMapper.getByStreamId(streamId);

                    // Verify resource existence
                    if (null == stream) {
                        ControllerException e = new ControllerException(Code.NOT_FOUND_VALUE,
                            String.format("Stream[stream-id=%d] is not found", streamId));
                        future.completeExceptionally(e);
                        break;
                    }

                    // Verify stream owner
                    switch (stream.getState()) {
                        case CLOSING -> {
                            if (nodeId != stream.getSrcNodeId()) {
                                LOGGER.warn("State of Stream[stream-id={}] is {}, stream.srcNodeId={} while close stream request from Node[node-id={}]. Fenced",
                                    streamId, stream.getState(), stream.getDstNodeId(), nodeId);
                                ControllerException e = new ControllerException(Code.FENCED_VALUE,
                                    "Close stream op is fenced by non-owner");
                                future.completeExceptionally(e);
                            }
                        }
                        case OPEN -> {
                            if (nodeId != stream.getDstNodeId()) {
                                LOGGER.warn("dst-node-id of stream {} is {}, fencing close stream request from Node[node-id={}]",
                                    streamId, stream.getDstNodeId(), nodeId);
                                ControllerException e = new ControllerException(Code.FENCED_VALUE,
                                    "Close stream op is fenced by non-owner");
                                future.completeExceptionally(e);
                            }
                        }
                    }

                    // Verify epoch
                    if (streamEpoch != stream.getEpoch()) {
                        ControllerException e = new ControllerException(Code.FENCED_VALUE, "Stream epoch is deprecated");
                        future.completeExceptionally(e);
                        break;
                    }

                    // Make closeStream reentrant
                    if (stream.getState() == StreamState.CLOSED) {
                        future.complete(null);
                        break;
                    }

                    // Flag state as closed
                    stream.setState(StreamState.CLOSED);
                    streamMapper.update(stream);
                    session.commit();
                    future.complete(null);
                    break;
                }
            } else {
                Optional<String> leaderAddress = metadataStore.electionService().leaderAddress();
                if (leaderAddress.isEmpty()) {
                    return CompletableFuture.failedFuture(new ControllerException(Code.NO_LEADER_VALUE, "No leader is elected yet"));
                }

                CloseStreamRequest request = CloseStreamRequest.newBuilder()
                    .setStreamId(streamId)
                    .setStreamEpoch(streamEpoch)
                    .build();
                metadataStore.controllerClient().closeStream(leaderAddress.get(), request).whenComplete(((reply, e) -> {
                    if (null != e) {
                        future.completeExceptionally(e);
                    } else {
                        future.complete(null);
                    }
                }));
                break;
            }
        }
        return future;
    }

    public CompletableFuture<List<StreamMetadata>> getStreams(List<Long> streamIds) {
        if (null == streamIds || streamIds.isEmpty()) {
            return CompletableFuture.completedFuture(new ArrayList<>());
        }

        return CompletableFuture.supplyAsync(() -> {
            try (SqlSession session = metadataStore.openSession()) {
                StreamMapper streamMapper = session.getMapper(StreamMapper.class);
                StreamCriteria criteria = StreamCriteria.newBuilder().addBatchStreamIds(streamIds).build();
                List<Stream> streams = streamMapper.byCriteria(criteria);
                return buildStreamMetadata(streams, session);
            }
        }, metadataStore.asyncExecutor());
    }

    public CompletableFuture<List<StreamMetadata>> listOpenStreams(int nodeId) {
        CompletableFuture<List<StreamMetadata>> future = new CompletableFuture<>();
        for (; ; ) {
            if (metadataStore.isLeader()) {
                try (SqlSession session = metadataStore.openSession()) {
                    if (!metadataStore.maintainLeadershipWithSharedLock(session)) {
                        continue;
                    }
                    StreamMapper streamMapper = session.getMapper(StreamMapper.class);
                    StreamCriteria criteria = StreamCriteria.newBuilder()
                        .withDstNodeId(nodeId)
                        .withState(StreamState.OPEN)
                        .build();
                    List<StreamMetadata> streams = buildStreamMetadata(streamMapper.byCriteria(criteria), session);
                    future.complete(streams);
                    break;
                }
            } else {
                Optional<String> leaderAddress = metadataStore.electionService().leaderAddress();
                if (leaderAddress.isEmpty()) {
                    return CompletableFuture.failedFuture(new ControllerException(Code.NO_LEADER_VALUE, "No leader is elected yet"));
                }
                ListOpenStreamsRequest request = ListOpenStreamsRequest.newBuilder()
                    .setBrokerId(nodeId)
                    .build();
                return metadataStore.controllerClient().listOpenStreams(leaderAddress.get(), request)
                    .thenApply((ListOpenStreamsReply::getStreamMetadataList));
            }
        }
        return future;
    }

    private List<StreamMetadata> buildStreamMetadata(List<Stream> streams, SqlSession session) {
        RangeMapper rangeMapper = session.getMapper(RangeMapper.class);
        return streams.stream()
            .map(stream -> {
                int rangeId = stream.getRangeId();
                Range range = rangeMapper.get(rangeId, stream.getId(), null);
                return StreamMetadata.newBuilder()
                    .setStreamId(stream.getId())
                    .setStartOffset(stream.getStartOffset())
                    .setEndOffset(null == range ? 0 : range.getEndOffset())
                    .setEpoch(stream.getEpoch())
                    .setState(stream.getState())
                    .setRangeId(stream.getRangeId())
                    .build();
            })
            .toList();
    }

    public CompletableFuture<DescribeStreamReply> describeStream(DescribeStreamRequest request) {
        return CompletableFuture.supplyAsync(() -> {
            try (SqlSession session = metadataStore.openSession()) {
                StreamMapper streamMapper = session.getMapper(StreamMapper.class);
                Stream stream = streamMapper.getByStreamId(request.getStreamId());
                if (null != stream) {
                    DescribeStreamReply.Builder builder = DescribeStreamReply.newBuilder()
                        .setStatus(Status.newBuilder().setCode(Code.OK)
                            .build());

                    StreamMetadata.Builder streamBuilder = StreamMetadata.newBuilder()
                        .setStreamId(stream.getId())
                        .setStartOffset(stream.getStartOffset())
                        .setEpoch(stream.getEpoch())
                        .setState(stream.getState())
                        .setRangeId(stream.getRangeId());
                    RangeMapper rangeMapper = session.getMapper(RangeMapper.class);
                    List<Range> ranges = rangeMapper.list(null, stream.getId(), null);
                    OptionalLong endOffset = ranges.stream().mapToLong(Range::getEndOffset).max();
                    if (endOffset.isPresent()) {
                        streamBuilder.setEndOffset(endOffset.getAsLong());
                    }
                    builder.setStream(streamBuilder);

                    builder.addAllRanges(ranges.stream().map(r -> apache.rocketmq.controller.v1.Range.newBuilder()
                        .setStreamId(r.getStreamId())
                        .setStartOffset(r.getStartOffset())
                        .setEndOffset(r.getEndOffset())
                        .setBrokerId(r.getNodeId())
                        .setEpoch(r.getEpoch())
                        .build()).collect(Collectors.toList()));
                    return builder.build();
                } else {
                    return DescribeStreamReply.newBuilder()
                        .setStatus(Status.newBuilder().setCode(Code.NOT_FOUND)
                            .setMessage(String.format("Stream[stream-id=%d] is not found", request.getStreamId())).build())
                        .build();
                }
            }
        }, MoreExecutors.directExecutor());
    }

    public void deleteStream(long streamId) {
        try (SqlSession session = metadataStore.openSession()) {
            S3StreamObjectMapper streamObjectMapper = session.getMapper(S3StreamObjectMapper.class);
            streamObjectMapper.delete(null, streamId, null);
            S3ObjectMapper s3ObjectMapper = session.getMapper(S3ObjectMapper.class);
            List<S3Object> list = s3ObjectMapper.list(null, streamId);
            List<Long> objectIds = new ArrayList<>();
            for (S3Object s3Object : list) {
                objectIds.add(s3Object.getId());
            }

            while (!objectIds.isEmpty()) {
                List<Long> deleted = metadataStore.getDataStore().batchDeleteS3Objects(objectIds).join();
                objectIds.removeAll(deleted);
                if (!deleted.isEmpty()) {
                    LOGGER.info("DataStore batch deleted S3 objects having object-id-list={}", deleted);
                    s3ObjectMapper.deleteByCriteria(S3ObjectCriteria.newBuilder().addObjectIds(deleted).build());
                }
            }

            s3ObjectMapper.deleteByCriteria(S3ObjectCriteria.newBuilder().withStreamId(streamId).build());

            session.commit();
        }
    }
}
