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

package com.automq.rocketmq.controller.metadata.database;

import apache.rocketmq.controller.v1.AssignmentStatus;
import apache.rocketmq.controller.v1.Code;
import apache.rocketmq.controller.v1.CreateTopicRequest;
import apache.rocketmq.controller.v1.MessageType;
import apache.rocketmq.controller.v1.StreamRole;
import apache.rocketmq.controller.v1.StreamState;
import apache.rocketmq.controller.v1.SubStream;
import apache.rocketmq.controller.v1.TopicStatus;
import apache.rocketmq.controller.v1.UpdateTopicRequest;
import com.automq.rocketmq.controller.exception.ControllerException;
import com.automq.rocketmq.controller.metadata.BrokerNode;
import com.automq.rocketmq.controller.metadata.MetadataStore;
import com.automq.rocketmq.controller.metadata.database.dao.Node;
import com.automq.rocketmq.controller.metadata.database.dao.QueueAssignment;
import com.automq.rocketmq.controller.metadata.database.dao.Stream;
import com.automq.rocketmq.controller.metadata.database.dao.Topic;
import com.automq.rocketmq.controller.metadata.database.mapper.QueueAssignmentMapper;
import com.automq.rocketmq.controller.metadata.database.mapper.StreamMapper;
import com.automq.rocketmq.controller.metadata.database.mapper.TopicMapper;
import com.automq.rocketmq.controller.metadata.database.serde.SubStreamDeserializer;
import com.automq.rocketmq.controller.metadata.database.serde.SubStreamSerializer;
import com.google.common.base.Strings;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.stream.IntStream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.ibatis.session.SqlSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TopicManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(TopicManager.class);

    private final MetadataStore metadataStore;

    private final ExecutorService asyncExecutorService;

    private final Gson gson;

    final TopicCache topicCache;

    final AssignmentCache assignmentCache;

    public TopicManager(MetadataStore metadataStore, ExecutorService asyncExecutorService) {
        this.metadataStore = metadataStore;
        this.asyncExecutorService = asyncExecutorService;

        this.gson = new GsonBuilder()
            .registerTypeAdapter(SubStream.class, new SubStreamSerializer())
            .registerTypeAdapter(SubStream.class, new SubStreamDeserializer())
            .create();

        this.topicCache = new TopicCache();
        this.assignmentCache = new AssignmentCache();
    }

    public CompletableFuture<Long> createTopic(String topicName, int queueNum,
        List<MessageType> acceptMessageTypesList) throws ControllerException {
        CompletableFuture<Long> future = new CompletableFuture<>();
        for (; ; ) {
            if (metadataStore.isLeader()) {
                try (SqlSession session = metadataStore.openSession()) {
                    if (!metadataStore.maintainLeadershipWithSharedLock(session)) {
                        continue;
                    }

                    TopicMapper topicMapper = session.getMapper(TopicMapper.class);
                    if (null != topicMapper.get(null, topicName)) {
                        ControllerException e = new ControllerException(Code.DUPLICATED_VALUE, String.format("Topic %s was taken", topicName));
                        future.completeExceptionally(e);
                        return future;
                    }

                    Topic topic = new Topic();
                    topic.setName(topicName);
                    topic.setQueueNum(queueNum);
                    topic.setStatus(TopicStatus.TOPIC_STATUS_ACTIVE);
                    topic.setAcceptMessageTypes(gson.toJson(acceptMessageTypesList));
                    topicMapper.create(topic);
                    long topicId = topic.getId();
                    List<QueueAssignment> assignments = createQueues(IntStream.range(0, queueNum), topicId, session);
                    // Commit transaction
                    session.commit();

                    // Cache new topic and queue assignments immediately
                    topicCache.apply(List.of(topic));
                    assignmentCache.apply(assignments);

                    future.complete(topicId);
                }
            } else {
                CreateTopicRequest request = CreateTopicRequest.newBuilder()
                    .setTopic(topicName)
                    .setCount(queueNum)
                    .addAllAcceptMessageTypes(acceptMessageTypesList)
                    .build();
                try {
                    metadataStore.controllerClient().createTopic(metadataStore.leaderAddress(), request).whenComplete((res, e) -> {
                        if (null != e) {
                            future.completeExceptionally(e);
                        } else {
                            future.complete(res);
                        }
                    });
                } catch (ControllerException e) {
                    future.completeExceptionally(e);
                }
            }
            break;
        }
        return future;
    }

    public CompletableFuture<apache.rocketmq.controller.v1.Topic> updateTopic(long topicId,
        @Nullable String topicName,
        @Nullable Integer queueNumber,
        @Nonnull List<MessageType> acceptMessageTypesList) throws ControllerException {
        CompletableFuture<apache.rocketmq.controller.v1.Topic> future = new CompletableFuture<>();
        for (; ; ) {
            if (metadataStore.isLeader()) {
                try (SqlSession session = metadataStore.openSession()) {
                    if (!metadataStore.maintainLeadershipWithSharedLock(session)) {
                        continue;
                    }

                    TopicMapper topicMapper = session.getMapper(TopicMapper.class);
                    Topic topic = topicMapper.get(topicId, null);

                    if (null == topic) {
                        ControllerException e = new ControllerException(Code.NOT_FOUND_VALUE,
                            String.format("Topic not found for topic-id=%d, topic-name=%s", topicId, topicName));
                        future.completeExceptionally(e);
                        return future;
                    }

                    boolean changed = false;

                    // Update accepted message types
                    if (!acceptMessageTypesList.isEmpty()) {
                        String types = gson.toJson(acceptMessageTypesList);
                        if (!types.equals(topic.getAcceptMessageTypes())) {
                            topic.setAcceptMessageTypes(types);
                            changed = true;
                        }
                    }

                    // Update topic name
                    if (!Strings.isNullOrEmpty(topicName) && !topic.getName().equals(topicName)) {
                        topic.setName(topicName);
                        changed = true;
                    }

                    // Update queue nums
                    if (null != queueNumber && !queueNumber.equals(topic.getQueueNum())) {
                        if (queueNumber > topic.getQueueNum()) {
                            createQueues(IntStream.range(topic.getQueueNum(), queueNumber), topic.getId(), session);
                        }
                        changed = true;
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
                        .addAllAcceptMessageTypes(acceptMessageTypesList)
                        .build();
                    future.complete(uTopic);
                }
            } else {
                UpdateTopicRequest request = UpdateTopicRequest.newBuilder()
                    .setTopicId(topicId)
                    .setName(topicName)
                    .addAllAcceptMessageTypes(acceptMessageTypesList)
                    .build();
                try {
                    metadataStore.controllerClient().updateTopic(metadataStore.leaderAddress(), request).whenComplete((topic, e) -> {
                        if (null != e) {
                            future.completeExceptionally(e);
                        } else {
                            future.complete(topic);
                        }
                    });
                } catch (ControllerException e) {
                    future.completeExceptionally(e);
                }
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
                        topicMapper.updateStatusById(topicId, TopicStatus.TOPIC_STATUS_DELETED);

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
                        streamMapper.updateStreamState(null, topicId, null, StreamState.DELETED);
                        session.commit();
                    }
                    notifyOnResourceChange(toNotify);
                    return null;
                } else {
                    try {
                        return metadataStore.controllerClient().deleteTopic(metadataStore.leaderAddress(), topicId).join();
                    } catch (ControllerException e) {
                        throw new CompletionException(e);
                    }
                }
            }
        }, asyncExecutorService);
    }

    public CompletableFuture<apache.rocketmq.controller.v1.Topic> describeTopic(Long topicId,
        String topicName) {

        // Look up cache first
        Topic topicMetadataCache = null;
        if (null != topicId) {
            topicMetadataCache = this.topicCache.byId(topicId);
        }
        if (null == topicMetadataCache && null != topicName) {
            topicMetadataCache = this.topicCache.byName(topicName);
        }
        if (null != topicMetadataCache) {
            Map<Integer, QueueAssignment> assignmentMap = assignmentCache.byTopicId(topicMetadataCache.getId());
            if (null != assignmentMap && !assignmentMap.isEmpty()) {
                apache.rocketmq.controller.v1.Topic topic =
                    GrpcHelper.buildTopic(gson, topicMetadataCache, assignmentMap.values());
                return CompletableFuture.completedFuture(topic);
            }
        }

        return CompletableFuture.supplyAsync(() -> {
            if (metadataStore.isLeader()) {
                try (SqlSession session = metadataStore.openSession()) {
                    TopicMapper topicMapper = session.getMapper(TopicMapper.class);
                    Topic topic = topicMapper.get(topicId, topicName);

                    if (null == topic) {
                        ControllerException e = new ControllerException(Code.NOT_FOUND_VALUE,
                            String.format("Topic not found for topic-id=%d, topic-name=%s", topicId, topicName));
                        throw new CompletionException(e);
                    }

                    QueueAssignmentMapper assignmentMapper = session.getMapper(QueueAssignmentMapper.class);
                    List<QueueAssignment> assignments = assignmentMapper.list(topic.getId(), null, null, null, null);
                    return GrpcHelper.buildTopic(gson, topic, assignments);
                }
            } else {
                try {
                    return metadataStore.controllerClient().describeTopic(metadataStore.leaderAddress(), topicId, topicName).join();
                } catch (ControllerException e) {
                    throw new CompletionException(e);
                }
            }
        }, asyncExecutorService);
    }

    public CompletableFuture<List<apache.rocketmq.controller.v1.Topic>> listTopics() {
        CompletableFuture<List<apache.rocketmq.controller.v1.Topic>> future = new CompletableFuture<>();
        List<apache.rocketmq.controller.v1.Topic> result = new ArrayList<>();
        try (SqlSession session = metadataStore.openSession()) {
            TopicMapper topicMapper = session.getMapper(TopicMapper.class);
            List<Topic> topics = topicMapper.list(null, null);
            for (Topic topic : topics) {
                if (TopicStatus.TOPIC_STATUS_DELETED == topic.getStatus()) {
                    continue;
                }
                apache.rocketmq.controller.v1.Topic t = apache.rocketmq.controller.v1.Topic.newBuilder()
                    .setTopicId(topic.getId())
                    .setName(topic.getName())
                    .setCount(topic.getQueueNum())
                    .addAllAcceptMessageTypes(gson.fromJson(topic.getAcceptMessageTypes(), new TypeToken<List<MessageType>>() {
                    }.getType()))
                    .build();
                result.add(t);
            }
            future.complete(result);
        }
        return future;
    }

    private List<QueueAssignment> createQueues(IntStream range, long topicId, SqlSession session) throws ControllerException {
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
            throw new ControllerException(Code.NODE_UNAVAILABLE_VALUE);
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

    long createStream(StreamMapper streamMapper, long topicId, int queueId, Long groupId,
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
}
