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

import apache.rocketmq.controller.v1.AcceptTypes;
import apache.rocketmq.controller.v1.AssignmentStatus;
import apache.rocketmq.common.v1.Code;
import apache.rocketmq.controller.v1.CreateTopicRequest;
import apache.rocketmq.controller.v1.GroupStatus;
import apache.rocketmq.controller.v1.StreamRole;
import apache.rocketmq.controller.v1.StreamState;
import apache.rocketmq.controller.v1.TopicStatus;
import apache.rocketmq.controller.v1.UpdateTopicRequest;
import com.automq.rocketmq.common.exception.ControllerException;
import com.automq.rocketmq.controller.server.store.BrokerNode;
import com.automq.rocketmq.controller.MetadataStore;
import com.automq.rocketmq.controller.server.store.impl.cache.AssignmentCache;
import com.automq.rocketmq.controller.server.store.impl.cache.Inflight;
import com.automq.rocketmq.controller.server.store.impl.cache.StreamCache;
import com.automq.rocketmq.controller.server.store.impl.cache.TopicCache;
import com.automq.rocketmq.metadata.dao.Group;
import com.automq.rocketmq.metadata.dao.GroupCriteria;
import com.automq.rocketmq.metadata.dao.Node;
import com.automq.rocketmq.metadata.dao.QueueAssignment;
import com.automq.rocketmq.metadata.dao.Stream;
import com.automq.rocketmq.metadata.dao.StreamCriteria;
import com.automq.rocketmq.metadata.dao.Topic;
import com.automq.rocketmq.metadata.mapper.GroupMapper;
import com.automq.rocketmq.metadata.mapper.GroupProgressMapper;
import com.automq.rocketmq.metadata.mapper.QueueAssignmentMapper;
import com.automq.rocketmq.metadata.mapper.StreamMapper;
import com.automq.rocketmq.metadata.mapper.TopicMapper;
import com.google.common.base.Strings;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.IntStream;
import javax.annotation.Nonnull;
import org.apache.ibatis.session.SqlSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TopicManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(TopicManager.class);

    private final MetadataStore metadataStore;

    final TopicCache topicCache;

    final AssignmentCache assignmentCache;

    final StreamCache streamCache;

    final ConcurrentMap<Long, Inflight<apache.rocketmq.controller.v1.Topic>> topicIdRequests;
    final ConcurrentMap<String, Inflight<apache.rocketmq.controller.v1.Topic>> topicNameRequests;

    public TopicManager(MetadataStore metadataStore) {
        this.metadataStore = metadataStore;

        this.topicCache = new TopicCache();
        this.assignmentCache = new AssignmentCache();
        this.streamCache = new StreamCache();
        this.topicIdRequests = new ConcurrentHashMap<>();
        this.topicNameRequests = new ConcurrentHashMap<>();
    }

    public Optional<String> nameOf(long topicId) {
        apache.rocketmq.controller.v1.Topic topic = topicCache.byId(topicId);
        if (null == topic) {
            return Optional.empty();
        }
        return Optional.of(topic.getName());
    }

    public TopicCache getTopicCache() {
        return topicCache;
    }

    public AssignmentCache getAssignmentCache() {
        return assignmentCache;
    }

    public StreamCache getStreamCache() {
        return streamCache;
    }

    public int topicQuantity() {
        return topicCache.topicQuantity();
    }

    public int queueQuantity() {
        return assignmentCache.queueQuantity();
    }

    public int streamQuantity() {
        return streamCache.streamQuantity();
    }

    public int topicNumOfNode(int nodeId) {
        return assignmentCache.topicNumOfNode(nodeId);
    }

    public int queueNumOfNode(int nodeId) {
        return assignmentCache.queueNumOfNode(nodeId);
    }

    public int streamNumOfNode(int nodeId) {
        return streamCache.streamNumOfNode(nodeId);
    }

    public CompletableFuture<Long> createTopic(CreateTopicRequest request) {
        CompletableFuture<Long> future = new CompletableFuture<>();
        for (; ; ) {
            if (metadataStore.isLeader()) {
                try (SqlSession session = metadataStore.openSession()) {
                    if (!metadataStore.maintainLeadershipWithSharedLock(session)) {
                        continue;
                    }
                    TopicMapper topicMapper = session.getMapper(TopicMapper.class);
                    if (null != topicMapper.get(null, request.getTopic())) {
                        ControllerException e = new ControllerException(Code.DUPLICATED_VALUE,
                            String.format("Topic %s was taken", request.getTopic()));
                        future.completeExceptionally(e);
                        return future;
                    }

                    Topic topic = new Topic();
                    topic.setName(request.getTopic());
                    topic.setQueueNum(request.getCount());
                    topic.setStatus(TopicStatus.TOPIC_STATUS_ACTIVE);
                    topic.setAcceptMessageTypes(JsonFormat.printer().print(request.getAcceptTypes()));
                    topic.setRetentionHours(request.getRetentionHours());
                    topicMapper.create(topic);
                    long topicId = topic.getId();
                    List<QueueAssignment> assignments = createQueues(IntStream.range(0, request.getCount()),
                        topicId, session);
                    // Commit transaction
                    session.commit();

                    // Cache new topic and queue assignments immediately
                    topicCache.apply(List.of(topic));
                    assignmentCache.apply(assignments);
                    future.complete(topicId);
                } catch (ControllerException | InvalidProtocolBufferException e) {
                    future.completeExceptionally(e);
                }
                return future;
            } else {
                Optional<String> leaderAddress = metadataStore.electionService().leaderAddress();
                if (leaderAddress.isEmpty()) {
                    return CompletableFuture.failedFuture(new ControllerException(Code.NO_LEADER_VALUE, "No leader is elected yet"));
                }
                metadataStore.controllerClient().createTopic(leaderAddress.get(), request).whenComplete((res, e) -> {
                    if (null != e) {
                        future.completeExceptionally(e);
                    } else {
                        future.complete(res);
                    }
                });
            }
            break;
        }
        return future;
    }

    public CompletableFuture<apache.rocketmq.controller.v1.Topic> updateTopic(UpdateTopicRequest request) {
        CompletableFuture<apache.rocketmq.controller.v1.Topic> future = new CompletableFuture<>();
        for (; ; ) {
            if (metadataStore.isLeader()) {
                try (SqlSession session = metadataStore.openSession()) {
                    if (!metadataStore.maintainLeadershipWithSharedLock(session)) {
                        continue;
                    }

                    TopicMapper topicMapper = session.getMapper(TopicMapper.class);
                    Topic topic = topicMapper.get(request.getTopicId(), null);

                    if (null == topic) {
                        ControllerException e = new ControllerException(Code.NOT_FOUND_VALUE,
                            String.format("Topic not found for topic-id=%d, topic-name=%s", request.getTopicId(),
                                request.getName()));
                        future.completeExceptionally(e);
                        return future;
                    }

                    boolean changed = false;

                    // Update accepted message types
                    if (!request.getAcceptTypes().getTypesList().isEmpty()) {
                        String types = JsonFormat.printer().print(request.getAcceptTypes());
                        if (!types.equals(topic.getAcceptMessageTypes())) {
                            topic.setAcceptMessageTypes(types);
                            changed = true;
                        }
                    }

                    // Update topic name
                    if (!Strings.isNullOrEmpty(request.getName()) && !topic.getName().equals(request.getName())) {
                        topic.setName(request.getName());
                        changed = true;
                    }

                    // Update queue number
                    if (request.getCount() > 0 && request.getCount() != topic.getQueueNum()) {
                        if (request.getCount() > topic.getQueueNum()) {
                            IntStream range = IntStream.range(topic.getQueueNum(), request.getCount());
                            List<QueueAssignment> assignments = createQueues(range, topic.getId(), session);
                            assignmentCache.apply(assignments);
                            topic.setQueueNum(request.getCount());
                            changed = true;
                        } else {
                            // Ignore queue number field if not enlarged
                            topic.setQueueNum(null);
                        }
                    }

                    if (changed) {
                        topicMapper.update(topic);
                        // Commit transaction
                        session.commit();
                    }

                    apache.rocketmq.controller.v1.Topic uTopic = apache.rocketmq.controller.v1.Topic
                        .newBuilder()
                        .setTopicId(topic.getId())
                        .setCount(topic.getQueueNum())
                        .setName(topic.getName())
                        .setAcceptTypes(request.getAcceptTypes())
                        .build();
                    future.complete(uTopic);
                } catch (ControllerException | InvalidProtocolBufferException e) {
                    future.completeExceptionally(e);
                }
            } else {
                Optional<String> leaderAddress = metadataStore.electionService().leaderAddress();
                if (leaderAddress.isEmpty()) {
                    return CompletableFuture.failedFuture(new ControllerException(Code.NO_LEADER_VALUE, "No leader is elected yet"));
                }
                metadataStore.controllerClient().updateTopic(leaderAddress.get(), request).whenComplete((topic, e) -> {
                    if (null != e) {
                        future.completeExceptionally(e);
                    } else {
                        future.complete(topic);
                    }
                });
            }
            break;
        }
        return future;
    }

    public CompletableFuture<Void> deleteTopic(long topicId) {
        return CompletableFuture.supplyAsync(() -> {
            for (; ; ) {
                if (metadataStore.isLeader()) {
                    Set<Integer> toNotify = new HashSet<>();
                    try (SqlSession session = metadataStore.openSession()) {
                        if (!metadataStore.maintainLeadershipWithSharedLock(session)) {
                            continue;
                        }

                        TopicMapper topicMapper = session.getMapper(TopicMapper.class);
                        Topic topic = topicMapper.get(topicId, null);
                        if (null == topic) {
                            ControllerException e = new ControllerException(Code.NOT_FOUND_VALUE, String.format("This is no topic with topic-id %d", topicId));
                            throw new CompletionException(e);
                        }

                        // Ensure this topic is not as DEAD LETTER TOPIC.
                        GroupMapper groupMapper = session.getMapper(GroupMapper.class);
                        List<Group> groups = groupMapper.byCriteria(GroupCriteria.newBuilder()
                            .setStatus(GroupStatus.GROUP_STATUS_ACTIVE)
                            .setDeadLetterTopicId(topicId)
                            .build());
                        if (!groups.isEmpty()) {
                            String msg = String.format("Topic %s is still referenced by %s as dead letter topic",
                                topic.getName(), groups.get(0).getName());
                            ControllerException e = new ControllerException(Code.REFERENCED_AS_DEAD_LETTER_TOPIC_VALUE, msg);
                            throw new CompletionException(e);
                        }

                        // Logically delete topic
                        topicMapper.updateStatusById(topicId, TopicStatus.TOPIC_STATUS_DELETED);

                        // Delete queue-assignment
                        QueueAssignmentMapper assignmentMapper = session.getMapper(QueueAssignmentMapper.class);
                        List<QueueAssignment> assignments = assignmentMapper.list(topicId, null, null, null, null);
                        assignments.stream().filter(assignment -> assignment.getStatus() != AssignmentStatus.ASSIGNMENT_STATUS_DELETED)
                            .forEach(assignment -> {
                                switch (assignment.getStatus()) {
                                    case ASSIGNMENT_STATUS_ASSIGNED -> toNotify.add(assignment.getDstNodeId());
                                    case ASSIGNMENT_STATUS_YIELDING -> toNotify.add(assignment.getSrcNodeId());
                                    default -> {
                                    }
                                }
                                assignment.setStatus(AssignmentStatus.ASSIGNMENT_STATUS_DELETED);
                                assignmentMapper.update(assignment);
                            });
                        StreamMapper streamMapper = session.getMapper(StreamMapper.class);
                        StreamCriteria criteria = StreamCriteria.newBuilder()
                            .withTopicId(topicId)
                            .build();
                        streamMapper.updateStreamState(criteria, StreamState.DELETED);

                        // Delete group-progress
                        GroupProgressMapper groupProgressMapper = session.getMapper(GroupProgressMapper.class);
                        groupProgressMapper.delete(null, topicId);

                        session.commit();
                    }
                    notifyOnResourceChange(toNotify);
                    return null;
                } else {
                    Optional<String> leaderAddress = metadataStore.electionService().leaderAddress();
                    if (leaderAddress.isEmpty()) {
                        throw new CompletionException(new ControllerException(Code.NO_LEADER_VALUE, "No leader is elected yet"));
                    }
                    return metadataStore.controllerClient().deleteTopic(leaderAddress.get(), topicId).join();
                }
            }
        }, metadataStore.asyncExecutor());
    }

    private void completeTopicDescription(@Nonnull apache.rocketmq.controller.v1.Topic topic) {
        Inflight<apache.rocketmq.controller.v1.Topic> idInflight = topicIdRequests.remove(topic.getTopicId());
        if (null != idInflight) {
            idInflight.complete(topic);
        }

        Inflight<apache.rocketmq.controller.v1.Topic> nameInflight = topicNameRequests.remove(topic.getName());
        if (null != nameInflight) {
            nameInflight.complete(topic);
        }
    }

    private void completeTopicDescriptionExceptionally(Long topicId, String topicName, Throwable e) {
        if (null != topicId) {
            Inflight<apache.rocketmq.controller.v1.Topic> idInflight = topicIdRequests.remove(topicId);
            if (null != idInflight) {
                idInflight.completeExceptionally(e);
            }
        }

        if (!Strings.isNullOrEmpty(topicName)) {
            Inflight<apache.rocketmq.controller.v1.Topic> nameInflight = topicNameRequests.remove(topicName);
            if (null != nameInflight) {
                nameInflight.completeExceptionally(e);
            }
        }
    }

    public CompletableFuture<apache.rocketmq.controller.v1.Topic> describeTopic(Long topicId, String topicName) {
        // Look up cache first
        {
            apache.rocketmq.controller.v1.Topic topic = null;
            if (null != topicId) {
                topic = topicCache.byId(topicId);
            }

            if (null == topic && null != topicName) {
                topic = topicCache.byName(topicName);
            }

            if (null != topic) {
                Map<Integer, QueueAssignment> assignmentMap = assignmentCache.byTopicId(topic.getTopicId());
                if (null != assignmentMap && !assignmentMap.isEmpty()) {
                    apache.rocketmq.controller.v1.Topic.Builder topicBuilder = topic.toBuilder();
                    Helper.setAssignments(topicBuilder, assignmentMap.values());
                    return CompletableFuture.completedFuture(topicBuilder.build());
                }
            }
        }

        CompletableFuture<apache.rocketmq.controller.v1.Topic> future = new CompletableFuture<>();
        boolean queryNow = false;
        if (null != topicId) {
            switch (Helper.addFuture(topicId, future, topicIdRequests)) {
                case COMPLETED -> {
                    return describeTopic(topicId, topicName);
                }
                case LEADER -> queryNow = true;
            }
        }

        if (!Strings.isNullOrEmpty(topicName)) {
            switch (Helper.addFuture(topicName, future, topicNameRequests)) {
                case COMPLETED -> {
                    return describeTopic(topicId, topicName);
                }
                case LEADER -> queryNow = true;
            }
        }

        if (queryNow) {
            metadataStore.asyncExecutor().submit(() -> {
                try (SqlSession session = metadataStore.openSession()) {
                    TopicMapper topicMapper = session.getMapper(TopicMapper.class);
                    Topic topic = topicMapper.get(topicId, topicName);
                    if (null == topic) {
                        ControllerException e = new ControllerException(Code.NOT_FOUND_VALUE,
                            String.format("Topic not found for topic-id=%d, topic-name=%s", topicId, topicName));
                        completeTopicDescriptionExceptionally(topicId, topicName, e);
                        return;
                    }

                    QueueAssignmentMapper assignmentMapper = session.getMapper(QueueAssignmentMapper.class);
                    List<QueueAssignment> assignments = assignmentMapper.list(topic.getId(), null, null, null, null);
                    // Update cache
                    topicCache.apply(List.of(topic));
                    assignmentCache.apply(assignments);
                    apache.rocketmq.controller.v1.Topic result = Helper.buildTopic(topic, assignments);
                    completeTopicDescription(result);
                } catch (InvalidProtocolBufferException e) {
                    completeTopicDescriptionExceptionally(topicId, topicName, e);
                }
            });
        }

        return future;
    }

    public CompletableFuture<List<apache.rocketmq.controller.v1.Topic>> listTopics() {
        CompletableFuture<List<apache.rocketmq.controller.v1.Topic>> future = new CompletableFuture<>();
        List<apache.rocketmq.controller.v1.Topic> result = new ArrayList<>();
        try (SqlSession session = metadataStore.openSession()) {
            TopicMapper topicMapper = session.getMapper(TopicMapper.class);
            List<Topic> topics = topicMapper.list(TopicStatus.TOPIC_STATUS_ACTIVE, null);
            for (Topic topic : topics) {
                AcceptTypes.Builder builder = AcceptTypes.newBuilder();
                JsonFormat.parser().ignoringUnknownFields().merge(topic.getAcceptMessageTypes(), builder);
                apache.rocketmq.controller.v1.Topic t = apache.rocketmq.controller.v1.Topic.newBuilder()
                    .setTopicId(topic.getId())
                    .setName(topic.getName())
                    .setCount(topic.getQueueNum())
                    .setRetentionHours(topic.getRetentionHours())
                    .setAcceptTypes(builder.build())
                    .build();
                result.add(t);
            }
            future.complete(result);
        } catch (InvalidProtocolBufferException e) {
            future.completeExceptionally(e);
        }
        return future;
    }

    private List<QueueAssignment> createQueues(IntStream range, long topicId,
        SqlSession session) throws ControllerException {
        QueueAssignmentMapper assignmentMapper = session.getMapper(QueueAssignmentMapper.class);
        StreamMapper streamMapper = session.getMapper(StreamMapper.class);

        List<Node> aliveNodes = metadataStore
            .allNodes()
            .values()
            .stream()
            .filter(node -> node.isAlive(metadataStore.config()))
            .map(BrokerNode::getNode)
            .toList();
        if (aliveNodes.isEmpty()) {
            LOGGER.warn("0 of {} broker nodes is alive", metadataStore.allNodes().size());
            throw new ControllerException(Code.NODE_UNAVAILABLE_VALUE, "No broker node is available");
        }

        List<QueueAssignment> assignments = new ArrayList<>();

        range.forEach(n -> {
            QueueAssignment assignment = new QueueAssignment();
            assignment.setTopicId(topicId);
            assignment.setStatus(AssignmentStatus.ASSIGNMENT_STATUS_ASSIGNED);
            assignment.setQueueId(n);
            Node node = aliveNodes.get(n % aliveNodes.size());
            // On creation, both src and dst node_id are the same.
            assignment.setSrcNodeId(node.getId());
            assignment.setDstNodeId(node.getId());
            assignmentMapper.create(assignment);
            assignments.add(assignment);
            // Create data stream
            long streamId = createStream(streamMapper, topicId, n, null, StreamRole.STREAM_ROLE_DATA, node.getId());
            LOGGER.debug("Create assignable data stream[stream-id={}] for topic-id={}, queue-id={}",
                streamId, topicId, n);
            // Create ops stream
            streamId = createStream(streamMapper, topicId, n, null, StreamRole.STREAM_ROLE_OPS, node.getId());
            LOGGER.debug("Create assignable ops stream[stream-id={}] for topic-id={}, queue-id={}",
                streamId, topicId, n);

            // Create snapshot stream
            streamId = createStream(streamMapper, topicId, n, null, StreamRole.STREAM_ROLE_SNAPSHOT, node.getId());
            LOGGER.debug("Create assignable snapshot stream[stream-id={}] for topic-id={}, queue-id={}",
                streamId, topicId, n);
        });
        return assignments;
    }

    public long createStream(StreamMapper streamMapper, long topicId, int queueId, Long groupId,
        StreamRole role, int nodeId) {
        // epoch and rangeId default to -1
        Stream stream = new Stream();
        stream.setState(StreamState.UNINITIALIZED);
        stream.setTopicId(topicId);
        stream.setQueueId(queueId);
        stream.setGroupId(groupId);
        stream.setStreamRole(role);
        stream.setSrcNodeId(nodeId);
        stream.setDstNodeId(nodeId);
        streamMapper.create(stream);
        LOGGER.info("Created {} stream for topic-id={}, queue-id={}, group-id={}", role, topicId, queueId, groupId);
        return stream.getId();
    }

    /**
     * Notify nodes that some resources are changed by leader controller.
     *
     * @param nodes Nodes to notify
     */
    private void notifyOnResourceChange(Set<Integer> nodes) {

    }

    public Optional<Integer> ownerNode(long topicId, int queueId) {
        Map<Integer, QueueAssignment> map = assignmentCache.byTopicId(topicId);
        if (null != map) {
            QueueAssignment assignment = map.get(queueId);
            if (null != assignment) {
                switch (assignment.getStatus()) {
                    case ASSIGNMENT_STATUS_ASSIGNED -> {
                        return Optional.of(assignment.getDstNodeId());
                    }
                    case ASSIGNMENT_STATUS_YIELDING -> {
                        return Optional.of(assignment.getSrcNodeId());
                    }
                }
            }
        }
        return Optional.empty();
    }
}
