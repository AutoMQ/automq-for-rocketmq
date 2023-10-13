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
import apache.rocketmq.controller.v1.CloseStreamRequest;
import apache.rocketmq.controller.v1.Code;
import apache.rocketmq.controller.v1.ConsumerGroup;
import apache.rocketmq.controller.v1.CommitStreamObjectRequest;
import apache.rocketmq.controller.v1.CommitWALObjectRequest;
import apache.rocketmq.controller.v1.CreateGroupRequest;
import apache.rocketmq.controller.v1.CreateTopicRequest;
import apache.rocketmq.controller.v1.GroupStatus;
import apache.rocketmq.controller.v1.GroupType;
import apache.rocketmq.controller.v1.ListOpenStreamsRequest;
import apache.rocketmq.controller.v1.ListOpenStreamsReply;
import apache.rocketmq.controller.v1.MessageQueue;
import apache.rocketmq.controller.v1.MessageQueueAssignment;
import apache.rocketmq.controller.v1.MessageType;
import apache.rocketmq.controller.v1.OngoingMessageQueueReassignment;
import apache.rocketmq.controller.v1.OpenStreamReply;
import apache.rocketmq.controller.v1.OpenStreamRequest;
import apache.rocketmq.controller.v1.S3WALObject;
import apache.rocketmq.controller.v1.StreamMetadata;
import apache.rocketmq.controller.v1.StreamRole;
import apache.rocketmq.controller.v1.PrepareS3ObjectsReply;
import apache.rocketmq.controller.v1.PrepareS3ObjectsRequest;
import apache.rocketmq.controller.v1.S3ObjectState;
import apache.rocketmq.controller.v1.StreamState;
import apache.rocketmq.controller.v1.SubStream;
import apache.rocketmq.controller.v1.TopicStatus;
import apache.rocketmq.controller.v1.TrimStreamRequest;
import apache.rocketmq.controller.v1.UpdateTopicRequest;
import com.automq.rocketmq.common.PrefixThreadFactory;
import com.automq.rocketmq.common.api.DataStore;
import com.automq.rocketmq.common.system.S3Constants;
import com.automq.rocketmq.controller.exception.ControllerException;
import com.automq.rocketmq.controller.metadata.BrokerNode;
import com.automq.rocketmq.controller.metadata.ControllerClient;
import com.automq.rocketmq.common.config.ControllerConfig;
import com.automq.rocketmq.controller.metadata.MetadataStore;
import com.automq.rocketmq.controller.metadata.Role;
import com.automq.rocketmq.controller.metadata.database.dao.Group;
import com.automq.rocketmq.controller.metadata.database.dao.GroupProgress;
import com.automq.rocketmq.controller.metadata.database.dao.Lease;
import com.automq.rocketmq.controller.metadata.database.dao.Node;
import com.automq.rocketmq.controller.metadata.database.dao.QueueAssignment;
import com.automq.rocketmq.controller.metadata.database.dao.S3StreamObject;
import com.automq.rocketmq.controller.metadata.database.dao.S3WalObject;
import com.automq.rocketmq.controller.metadata.database.dao.Stream;
import com.automq.rocketmq.controller.metadata.database.dao.S3Object;
import com.automq.rocketmq.controller.metadata.database.dao.Range;
import com.automq.rocketmq.controller.metadata.database.dao.Topic;
import com.automq.rocketmq.controller.metadata.database.mapper.GroupMapper;
import com.automq.rocketmq.controller.metadata.database.mapper.GroupProgressMapper;
import com.automq.rocketmq.controller.metadata.database.mapper.NodeMapper;
import com.automq.rocketmq.controller.metadata.database.mapper.LeaseMapper;
import com.automq.rocketmq.controller.metadata.database.mapper.QueueAssignmentMapper;
import com.automq.rocketmq.controller.metadata.database.mapper.S3ObjectMapper;
import com.automq.rocketmq.controller.metadata.database.mapper.S3StreamObjectMapper;
import com.automq.rocketmq.controller.metadata.database.mapper.S3WalObjectMapper;
import com.automq.rocketmq.controller.metadata.database.mapper.RangeMapper;
import com.automq.rocketmq.controller.metadata.database.mapper.SequenceMapper;
import com.automq.rocketmq.controller.metadata.database.mapper.StreamMapper;
import com.automq.rocketmq.controller.metadata.database.mapper.TopicMapper;
import com.automq.rocketmq.controller.tasks.KeepAliveTask;
import com.automq.rocketmq.controller.tasks.LeaseTask;
import com.automq.rocketmq.controller.tasks.ReclaimS3ObjectTask;
import com.automq.rocketmq.controller.tasks.RecycleGroupTask;
import com.automq.rocketmq.controller.tasks.RecycleTopicTask;
import com.automq.rocketmq.controller.tasks.ScanNodeTask;
import com.automq.rocketmq.controller.tasks.ScanYieldingQueueTask;
import com.automq.rocketmq.controller.tasks.SchedulerTask;
import com.google.common.base.Strings;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultMetadataStore implements MetadataStore {

    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultMetadataStore.class);

    private final ControllerClient controllerClient;

    private final SqlSessionFactory sessionFactory;

    private final ControllerConfig config;

    private Role role;

    private final ConcurrentHashMap<Integer, BrokerNode> nodes;

    private final ScheduledExecutorService scheduledExecutorService;

    private final ExecutorService asyncExecutorService;

    /// The following fields are runtime specific
    private Lease lease;

    private final Gson gson;

    private DataStore dataStore;

    public DefaultMetadataStore(ControllerClient client, SqlSessionFactory sessionFactory, ControllerConfig config) {
        this.controllerClient = client;
        this.sessionFactory = sessionFactory;
        this.config = config;
        // TODO: set leader here to bypass leader election, just for testing purpose.
        this.role = Role.Leader;
        this.nodes = new ConcurrentHashMap<>();
        this.gson = new Gson();
        this.scheduledExecutorService = new ScheduledThreadPoolExecutor(Runtime.getRuntime().availableProcessors(),
            new PrefixThreadFactory("Controller"));
        this.asyncExecutorService = Executors.newFixedThreadPool(10, new PrefixThreadFactory("Controller-Async"));
    }

    @Override
    public ControllerConfig config() {
        return config;
    }

    @Override
    public SqlSession openSession() {
        return sessionFactory.openSession(false);
    }

    @Override
    public ControllerClient controllerClient() {
        return controllerClient;
    }

    public DataStore getDataStore() {
        return dataStore;
    }

    @Override
    public void setDataStore(DataStore dataStore) {
        this.dataStore = dataStore;
    }

    public void start() {
        LeaseTask leaseTask = new LeaseTask(this);
        leaseTask.run();
        this.scheduledExecutorService.scheduleAtFixedRate(leaseTask, 1,
            config.scanIntervalInSecs(), TimeUnit.SECONDS);
        this.scheduledExecutorService.scheduleWithFixedDelay(new ScanNodeTask(this), 1,
            config.scanIntervalInSecs(), TimeUnit.SECONDS);
        this.scheduledExecutorService.scheduleWithFixedDelay(new ScanYieldingQueueTask(this), 1,
            config.scanIntervalInSecs(), TimeUnit.SECONDS);
        this.scheduledExecutorService.scheduleWithFixedDelay(new SchedulerTask(this), 1,
            config.scanIntervalInSecs(), TimeUnit.SECONDS);
        this.scheduledExecutorService.scheduleAtFixedRate(new KeepAliveTask(this), 3,
            Math.max(config().nodeAliveIntervalInSecs() / 2, 10), TimeUnit.SECONDS);
        this.scheduledExecutorService.scheduleWithFixedDelay(new RecycleTopicTask(this), 1,
            config.deletedTopicLingersInSecs(), TimeUnit.SECONDS);
        this.scheduledExecutorService.scheduleWithFixedDelay(new RecycleGroupTask(this), 1,
            config.deletedGroupLingersInSecs(), TimeUnit.SECONDS);
        this.scheduledExecutorService.scheduleWithFixedDelay(new ReclaimS3ObjectTask(this), 1,
            config.scanIntervalInSecs(), TimeUnit.SECONDS);
        LOGGER.info("MetadataStore tasks scheduled");
    }

    @Override
    public CompletableFuture<Node> registerBrokerNode(String name, String address, String instanceId) {
        CompletableFuture<Node> future = new CompletableFuture<>();
        if (Strings.isNullOrEmpty(name)) {
            future.completeExceptionally(
                new ControllerException(Code.BAD_REQUEST_VALUE, "Broker name is null or empty"));
            return future;
        }

        if (Strings.isNullOrEmpty(address)) {
            future.completeExceptionally(
                new ControllerException(Code.BAD_REQUEST_VALUE, "Broker address is null or empty"));
            return future;
        }

        if (Strings.isNullOrEmpty(instanceId)) {
            future.completeExceptionally(
                new ControllerException(Code.BAD_REQUEST_VALUE, "Broker instance-id is null or empty"));
            return future;
        }

        return CompletableFuture.supplyAsync(() -> {
            for (; ; ) {
                if (this.isLeader()) {
                    try (SqlSession session = openSession()) {
                        NodeMapper nodeMapper = session.getMapper(NodeMapper.class);
                        Node node = nodeMapper.get(null, name, instanceId, null);
                        if (null != node) {
                            nodeMapper.increaseEpoch(node.getId());
                            node.setEpoch(node.getEpoch() + 1);
                        } else {
                            LeaseMapper leaseMapper = session.getMapper(LeaseMapper.class);
                            Lease lease = leaseMapper.currentWithShareLock();
                            if (lease.getEpoch() != this.lease.getEpoch()) {
                                // Refresh cached lease
                                this.lease = lease;
                                LOGGER.info("Node[{}] has yielded its leader role", this.config.nodeId());
                                // Redirect register node to the new leader in the next iteration.
                                continue;
                            }
                            // epoch of node default to 1 when created
                            node = new Node();
                            node.setName(name);
                            node.setAddress(address);
                            node.setInstanceId(instanceId);
                            nodeMapper.create(node);
                            session.commit();
                        }
                        return node;
                    }
                } else {
                    try {
                        return controllerClient.registerBroker(this.leaderAddress(), name, address, instanceId).join();
                    } catch (ControllerException e) {
                        throw new CompletionException(e);
                    }
                }
            }
        }, this.asyncExecutorService);
    }

    @Override
    public void registerCurrentNode(String name, String address, String instanceId) throws ControllerException {
        try {
            Node node = registerBrokerNode(name, address, instanceId).get();
            config.setNodeId(node.getId());
            config.setEpoch(node.getEpoch());
        } catch (InterruptedException e) {
            throw new ControllerException(Code.INTERRUPTED_VALUE, e);
        } catch (ExecutionException e) {
            if (e.getCause() instanceof ControllerException) {
                throw (ControllerException) e.getCause();
            }
            throw new ControllerException(Code.INTERNAL_VALUE, e);
        }
    }

    @Override
    public void keepAlive(int nodeId, long epoch, boolean goingAway) {
        BrokerNode brokerNode = nodes.get(nodeId);
        if (null != brokerNode) {
            brokerNode.keepAlive(epoch, goingAway);
        }
    }

    public boolean maintainLeadershipWithSharedLock(SqlSession session) {
        LeaseMapper leaseMapper = session.getMapper(LeaseMapper.class);
        Lease current = leaseMapper.currentWithShareLock();
        if (current.getEpoch() != this.lease.getEpoch()) {
            // Current node is not leader any longer, forward to the new leader in the next iteration.
            this.lease = current;
            return false;
        }
        return true;
    }

    private void createQueues(IntStream range, long topicId, SqlSession session) throws ControllerException {
        QueueAssignmentMapper assignmentMapper = session.getMapper(QueueAssignmentMapper.class);
        StreamMapper streamMapper = session.getMapper(StreamMapper.class);

        List<Node> aliveNodes = this.nodes.values().stream().filter(node -> node.isAlive(config)).map(BrokerNode::getNode)
            .toList();
        if (aliveNodes.isEmpty()) {
            LOGGER.warn("0 of {} broker nodes is alive", nodes.size());
            throw new ControllerException(Code.NODE_UNAVAILABLE_VALUE);
        }

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
    }

    @Override
    public CompletableFuture<Long> createTopic(String topicName, int queueNum,
        List<MessageType> acceptMessageTypesList) throws ControllerException {
        CompletableFuture<Long> future = new CompletableFuture<>();
        for (; ; ) {
            if (this.isLeader()) {
                try (SqlSession session = openSession()) {
                    if (!maintainLeadershipWithSharedLock(session)) {
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
                    createQueues(IntStream.range(0, queueNum), topicId, session);
                    // Commit transaction
                    session.commit();
                    future.complete(topicId);
                }
            } else {
                CreateTopicRequest request = CreateTopicRequest.newBuilder()
                    .setTopic(topicName)
                    .setCount(queueNum)
                    .addAllAcceptMessageTypes(acceptMessageTypesList)
                    .build();
                try {
                    controllerClient.createTopic(this.leaderAddress(), request).whenComplete((res, e) -> {
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

    private long createStream(StreamMapper streamMapper, long topicId, int queueId, Long groupId,
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

    @Override
    public CompletableFuture<apache.rocketmq.controller.v1.Topic> updateTopic(long topicId,
        @Nullable String topicName,
        @Nullable Integer queueNumber,
        @Nonnull List<MessageType> acceptMessageTypesList) throws ControllerException {
        CompletableFuture<apache.rocketmq.controller.v1.Topic> future = new CompletableFuture<>();
        for (; ; ) {
            if (this.isLeader()) {
                try (SqlSession session = openSession()) {
                    if (!maintainLeadershipWithSharedLock(session)) {
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
                    controllerClient.updateTopic(this.leaderAddress(), request).whenComplete((topic, e) -> {
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

    @Override
    public CompletableFuture<Void> deleteTopic(long topicId) {
        return CompletableFuture.supplyAsync(() -> {
            for (; ; ) {
                if (this.isLeader()) {
                    Set<Integer> toNotify = new HashSet<>();
                    try (SqlSession session = openSession()) {
                        if (!maintainLeadershipWithSharedLock(session)) {
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
                    this.notifyOnResourceChange(toNotify);
                    return null;
                } else {
                    try {
                        return controllerClient.deleteTopic(this.leaderAddress(), topicId).join();
                    } catch (ControllerException e) {
                        throw new CompletionException(e);
                    }
                }
            }
        }, asyncExecutorService);
    }

    @Override
    public CompletableFuture<apache.rocketmq.controller.v1.Topic> describeTopic(Long topicId,
        String topicName) {
        return CompletableFuture.supplyAsync(() -> {
            if (this.isLeader()) {
                try (SqlSession session = openSession()) {
                    TopicMapper topicMapper = session.getMapper(TopicMapper.class);
                    Topic topic = topicMapper.get(topicId, topicName);

                    if (null == topic) {
                        ControllerException e = new ControllerException(Code.NOT_FOUND_VALUE,
                            String.format("Topic not found for topic-id=%d, topic-name=%s", topicId, topicName));
                        throw new CompletionException(e);
                    }

                    QueueAssignmentMapper assignmentMapper = session.getMapper(QueueAssignmentMapper.class);
                    List<QueueAssignment> assignments = assignmentMapper.list(topic.getId(), null, null, null, null);

                    apache.rocketmq.controller.v1.Topic.Builder topicBuilder = apache.rocketmq.controller.v1.Topic
                        .newBuilder()
                        .setTopicId(topic.getId())
                        .setCount(topic.getQueueNum())
                        .setName(topic.getName())
                        .addAllAcceptMessageTypes(gson.fromJson(topic.getAcceptMessageTypes(), new TypeToken<List<MessageType>>() {
                        }.getType()));

                    if (null != assignments) {
                        for (QueueAssignment assignment : assignments) {
                            switch (assignment.getStatus()) {
                                case ASSIGNMENT_STATUS_DELETED -> {
                                }

                                case ASSIGNMENT_STATUS_ASSIGNED -> {
                                    MessageQueueAssignment queueAssignment = MessageQueueAssignment.newBuilder()
                                        .setQueue(MessageQueue.newBuilder()
                                            .setTopicId(assignment.getTopicId())
                                            .setQueueId(assignment.getQueueId()))
                                        .setNodeId(assignment.getDstNodeId())
                                        .build();
                                    topicBuilder.addAssignments(queueAssignment);
                                }

                                case ASSIGNMENT_STATUS_YIELDING -> {
                                    OngoingMessageQueueReassignment reassignment = OngoingMessageQueueReassignment.newBuilder()
                                        .setQueue(MessageQueue.newBuilder()
                                            .setTopicId(assignment.getTopicId())
                                            .setQueueId(assignment.getQueueId())
                                            .build())
                                        .setSrcNodeId(assignment.getSrcNodeId())
                                        .setDstNodeId(assignment.getDstNodeId())
                                        .build();
                                    topicBuilder.addReassignments(reassignment);
                                }
                            }
                        }
                    }
                    return topicBuilder.build();
                }
            } else {
                try {
                    return this.controllerClient.describeTopic(leaderAddress(), topicId, topicName).join();
                } catch (ControllerException e) {
                    throw new CompletionException(e);
                }
            }
        }, asyncExecutorService);
    }

    @Override
    public CompletableFuture<List<apache.rocketmq.controller.v1.Topic>> listTopics() {
        CompletableFuture<List<apache.rocketmq.controller.v1.Topic>> future = new CompletableFuture<>();
        List<apache.rocketmq.controller.v1.Topic> result = new ArrayList<>();
        try (SqlSession session = openSession()) {
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

    /**
     * Notify nodes that some resources are changed by leader controller.
     *
     * @param nodes Nodes to notify
     */
    private void notifyOnResourceChange(Set<Integer> nodes) {

    }

    @Override
    public boolean isLeader() {
        return this.role == Role.Leader;
    }

    @Override
    public boolean hasAliveBrokerNodes() {
        return this.nodes.values().stream().anyMatch(node -> node.isAlive(config));
    }

    @Override
    public String leaderAddress() throws ControllerException {
        if (null == lease || lease.expired()) {
            LOGGER.error("No lease is populated yet or lease was expired");
            throw new ControllerException(Code.NO_LEADER_VALUE);
        }

        BrokerNode brokerNode = nodes.get(this.lease.getNodeId());
        if (null == brokerNode) {
            try (SqlSession session = openSession()) {
                NodeMapper nodeMapper = session.getMapper(NodeMapper.class);
                Node node = nodeMapper.get(this.lease.getNodeId(), null, null, null);
                if (null != node) {
                    addBrokerNode(node);
                    return node.getAddress();
                }
            }
            LOGGER.error("Address of broker[broker-id={}] is missing", this.lease.getNodeId());
            throw new ControllerException(Code.NOT_FOUND_VALUE,
                String.format("Node[node-id=%d] is missing", this.lease.getNodeId()));
        }

        return brokerNode.getNode().getAddress();
    }

    @Override
    public CompletableFuture<List<QueueAssignment>> listAssignments(Long topicId, Integer srcNodeId, Integer dstNodeId,
        AssignmentStatus status) {
        CompletableFuture<List<QueueAssignment>> future = new CompletableFuture<>();
        try (SqlSession session = openSession()) {
            QueueAssignmentMapper mapper = session.getMapper(QueueAssignmentMapper.class);
            List<QueueAssignment> queueAssignments = mapper.list(topicId, srcNodeId, dstNodeId, status, null);
            future.complete(queueAssignments);
            return future;
        }
    }

    @Override
    public CompletableFuture<Void> reassignMessageQueue(long topicId, int queueId,
        int dstNodeId) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        for (; ; ) {
            if (isLeader()) {
                try (SqlSession session = sessionFactory.openSession()) {
                    if (!maintainLeadershipWithSharedLock(session)) {
                        continue;
                    }
                    QueueAssignmentMapper assignmentMapper = session.getMapper(QueueAssignmentMapper.class);
                    List<QueueAssignment> assignments = assignmentMapper.list(topicId, null, null, null, null);
                    for (QueueAssignment assignment : assignments) {
                        if (assignment.getQueueId() != queueId) {
                            continue;
                        }

                        switch (assignment.getStatus()) {
                            case ASSIGNMENT_STATUS_YIELDING -> {
                                assignment.setDstNodeId(dstNodeId);
                                assignmentMapper.update(assignment);
                            }
                            case ASSIGNMENT_STATUS_ASSIGNED -> {
                                assignment.setDstNodeId(dstNodeId);
                                assignment.setStatus(AssignmentStatus.ASSIGNMENT_STATUS_YIELDING);
                                assignmentMapper.update(assignment);
                            }
                            case ASSIGNMENT_STATUS_DELETED ->
                                future.completeExceptionally(new ControllerException(Code.NOT_FOUND_VALUE, "Already deleted"));
                        }
                        break;
                    }
                    session.commit();
                }
                future.complete(null);
                break;
            } else {
                try {
                    this.controllerClient.reassignMessageQueue(leaderAddress(), topicId, queueId, dstNodeId).whenComplete((res, e) -> {
                        if (e != null) {
                            future.completeExceptionally(e);
                        } else {
                            future.complete(null);
                        }
                    });
                } catch (ControllerException e) {
                    future.completeExceptionally(e);
                }
            }
        }
        return future;
    }

    @Override
    public CompletableFuture<Void> markMessageQueueAssignable(long topicId, int queueId) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        for (; ; ) {
            if (isLeader()) {
                try (SqlSession session = openSession()) {
                    QueueAssignmentMapper assignmentMapper = session.getMapper(QueueAssignmentMapper.class);
                    List<QueueAssignment> assignments = assignmentMapper.list(topicId, null, null, AssignmentStatus.ASSIGNMENT_STATUS_YIELDING, null);
                    for (QueueAssignment assignment : assignments) {
                        if (assignment.getQueueId() != queueId) {
                            continue;
                        }
                        assignment.setStatus(AssignmentStatus.ASSIGNMENT_STATUS_ASSIGNED);
                        assignmentMapper.update(assignment);
                        LOGGER.info("Mark queue[topic-id={}, queue-id={}] assignable", topicId, queueId);
                        break;
                    }
                    session.commit();
                }
                future.complete(null);
                break;
            } else {
                try {
                    this.controllerClient.notifyQueueClose(leaderAddress(), topicId, queueId).whenComplete((res, e) -> {
                        if (null != e) {
                            future.completeExceptionally(e);
                        } else {
                            future.complete(null);
                        }
                    });
                } catch (ControllerException e) {
                    future.completeExceptionally(e);
                }
            }
        }
        return future;
    }

    @Override
    public CompletableFuture<Void> commitOffset(long groupId, long topicId, int queueId, long offset) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        for (; ; ) {
            if (isLeader()) {
                try (SqlSession session = openSession()) {
                    if (!maintainLeadershipWithSharedLock(session)) {
                        continue;
                    }
                    GroupProgressMapper groupProgressMapper = session.getMapper(GroupProgressMapper.class);
                    GroupProgress progress = new GroupProgress();
                    progress.setGroupId(groupId);
                    progress.setTopicId(topicId);
                    progress.setQueueId(queueId);
                    progress.setQueueOffset(offset);
                    groupProgressMapper.createOrUpdate(progress);
                    session.commit();
                }
                future.complete(null);
            } else {
                try {
                    this.controllerClient.commitOffset(leaderAddress(), groupId, topicId, queueId, offset).whenComplete((res, e) -> {
                        if (null != e) {
                            future.completeExceptionally(e);
                        } else {
                            future.complete(null);
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

    @Override
    public CompletableFuture<Long> getConsumerOffset(long consumerGroupId, long topicId, int queueId) {
        try (SqlSession session = openSession()) {
            GroupProgressMapper groupProgressMapper = session.getMapper(GroupProgressMapper.class);
            GroupProgress progress = groupProgressMapper.get(consumerGroupId, topicId, queueId);
            if (Objects.isNull(progress)) {
                return CompletableFuture.completedFuture(0L);
            } else {
                return CompletableFuture.completedFuture(progress.getQueueOffset());
            }
        }
    }

    @Override
    public CompletableFuture<Long> createGroup(String groupName, int maxRetry, GroupType type, long deadLetterTopicId) {
        CompletableFuture<Long> future = new CompletableFuture<>();
        for (; ; ) {
            if (isLeader()) {
                try (SqlSession session = openSession()) {
                    if (!maintainLeadershipWithSharedLock(session)) {
                        continue;
                    }
                    GroupMapper groupMapper = session.getMapper(GroupMapper.class);
                    List<Group> groups = groupMapper.list(null, groupName, null, null);
                    if (!groups.isEmpty()) {
                        ControllerException e = new ControllerException(Code.DUPLICATED_VALUE, String.format("Group name '%s' is not available", groupName));
                        future.completeExceptionally(e);
                        return future;
                    }

                    Group group = new Group();
                    group.setName(groupName);
                    group.setMaxDeliveryAttempt(maxRetry);
                    group.setDeadLetterTopicId(deadLetterTopicId);
                    group.setStatus(GroupStatus.GROUP_STATUS_ACTIVE);
                    group.setGroupType(type);
                    groupMapper.create(group);
                    session.commit();
                    future.complete(group.getId());
                }
            } else {
                CreateGroupRequest request = CreateGroupRequest.newBuilder()
                    .setName(groupName)
                    .setMaxRetryAttempt(maxRetry)
                    .setGroupType(type)
                    .setDeadLetterTopicId(deadLetterTopicId)
                    .build();

                try {
                    this.controllerClient.createGroup(leaderAddress(), request).whenComplete((reply, e) -> {
                        if (null != e) {
                            future.completeExceptionally(e);
                        } else {
                            future.complete(reply.getGroupId());
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

    @Override
    public CompletableFuture<StreamMetadata> getStream(long topicId, int queueId, Long groupId, StreamRole streamRole) {
        return CompletableFuture.supplyAsync(() -> {
            try (SqlSession session = openSession()) {
                StreamMapper streamMapper = session.getMapper(StreamMapper.class);
                List<Stream> streams = streamMapper.list(topicId, queueId, groupId).stream()
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
                        List<Group> groups = groupMapper.list(groupId, null, GroupStatus.GROUP_STATUS_ACTIVE, null);
                        if (groups.size() != 1) {
                            String msg = String.format("Group[group-id=%d] is not found", groupId);
                            throw new CompletionException(new ControllerException(Code.NOT_FOUND_VALUE, msg));
                        }

                        int nodeId = assignment.getDstNodeId();
                        long streamId = createStream(streamMapper, topicId, queueId, groupId, streamRole, nodeId);
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
        }, asyncExecutorService);
    }

    @Override
    public CompletableFuture<Void> onQueueClosed(long topicId, int queueId) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        for (; ; ) {
            if (isLeader()) {
                try (SqlSession session = openSession()) {
                    if (!maintainLeadershipWithSharedLock(session)) {
                        continue;
                    }
                    QueueAssignmentMapper assignmentMapper = session.getMapper(QueueAssignmentMapper.class);
                    QueueAssignment assignment = assignmentMapper.get(topicId, queueId);
                    assignment.setStatus(AssignmentStatus.ASSIGNMENT_STATUS_ASSIGNED);

                    StreamMapper streamMapper = session.getMapper(StreamMapper.class);
                    streamMapper.updateStreamState(null, topicId, queueId, StreamState.CLOSED);
                    session.commit();
                    future.complete(null);
                    return future;
                }
            } else {
                try {
                    controllerClient.notifyQueueClose(leaderAddress(), topicId, queueId)
                        .whenComplete((res, e) -> {
                            if (null != e) {
                                future.completeExceptionally(e);
                            }
                            future.complete(null);
                        });
                } catch (ControllerException e) {
                    future.completeExceptionally(e);
                }
                return future;
            }
        }
    }

    @Override
    public CompletableFuture<ConsumerGroup> describeConsumerGroup(Long groupId, String groupName) {
        return CompletableFuture.supplyAsync(() -> {
            try (SqlSession session = openSession()) {
                GroupMapper groupMapper = session.getMapper(GroupMapper.class);
                List<Group> groups = groupMapper.list(groupId, groupName, null, null);
                if (groups.isEmpty()) {
                    ControllerException e = new ControllerException(Code.NOT_FOUND_VALUE,
                        String.format("Group with group-id=%d is not found", groupId));
                    throw new CompletionException(e);
                } else {
                    Group group = groups.get(0);
                    return ConsumerGroup.newBuilder()
                        .setGroupId(group.getId())
                        .setName(group.getName())
                        .setGroupType(group.getGroupType())
                        .setMaxDeliveryAttempt(group.getMaxDeliveryAttempt())
                        .setDeadLetterTopicId(group.getDeadLetterTopicId())
                        .build();
                }
            }
        }, asyncExecutorService);
    }

    @Override
    public CompletableFuture<Void> trimStream(long streamId, long streamEpoch,
        long newStartOffset) throws ControllerException {
        CompletableFuture<Void> future = new CompletableFuture<>();
        for (; ; ) {
            if (this.isLeader()) {
                try (SqlSession session = this.sessionFactory.openSession(false)) {
                    if (!maintainLeadershipWithSharedLock(session)) {
                        continue;
                    }
                    StreamMapper streamMapper = session.getMapper(StreamMapper.class);
                    RangeMapper rangeMapper = session.getMapper(RangeMapper.class);
                    S3StreamObjectMapper s3StreamObjectMapper = session.getMapper(S3StreamObjectMapper.class);
                    S3WalObjectMapper s3WALObjectMapper = session.getMapper(S3WalObjectMapper.class);
                    S3ObjectMapper s3ObjectMapper = session.getMapper(S3ObjectMapper.class);

                    Stream stream = streamMapper.getByStreamId(streamId);
                    if (null == stream) {
                        ControllerException e = new ControllerException(Code.NOT_FOUND_VALUE,
                            String.format("Stream[stream-id=%d] is not found", streamId)
                        );
                        future.completeExceptionally(e);
                        return future;
                    }
                    if (stream.getState() == StreamState.CLOSED) {
                        LOGGER.warn("Stream[{}]‘s state is CLOSED, can't trim", streamId);
                        return null;
                    }
                    if (stream.getStartOffset() > newStartOffset) {
                        LOGGER.warn("Stream[{}]‘s start offset {} is larger than request new start offset {}",
                            streamId, stream.getStartOffset(), newStartOffset);
                        return null;
                    }
                    if (stream.getStartOffset() == newStartOffset) {
                        // regard it as redundant trim operation, just return success
                        return null;
                    }

                    // now the request is valid
                    // update the stream metadata start offset
                    stream.setEpoch(streamEpoch);
                    stream.setStartOffset(newStartOffset);
                    streamMapper.update(stream);

                    // remove range or update range's start offset
                    rangeMapper.listByStreamId(streamId).forEach(range -> {
                        if (newStartOffset <= range.getStartOffset()) {
                            return;
                        }
                        if (stream.getRangeId() == range.getRangeId()) {
                            // current range, update start offset
                            // if current range is [50, 100)
                            // 1. try to trim to 40, then current range will be [50, 100)
                            // 2. try to trim to 60, then current range will be [60, 100)
                            // 3. try to trim to 100, then current range will be [100, 100)
                            // 4. try to trim to 110, then current range will be [100, 100)
                            long newRangeStartOffset = newStartOffset < range.getEndOffset() ? newStartOffset : range.getEndOffset();
                            range.setStartOffset(newRangeStartOffset);
                            rangeMapper.update(range);
                            return;
                        }
                        if (newStartOffset >= range.getEndOffset()) {
                            // remove range
                            rangeMapper.delete(range.getRangeId(), streamId);
                            return;
                        }
                        // update range's start offset
                        range.setStartOffset(newStartOffset);
                        rangeMapper.update(range);
                    });
                    // remove stream object
                    s3StreamObjectMapper.listByStreamId(streamId).forEach(streamObject -> {
                        long streamStartOffset = streamObject.getStartOffset();
                        long streamEndOffset = streamObject.getEndOffset();
                        if (newStartOffset <= streamStartOffset) {
                            return;
                        }
                        if (newStartOffset >= streamEndOffset) {
                            // stream object
                            s3StreamObjectMapper.delete(null, streamId, streamObject.getObjectId());
                            // markDestroyObjects
                            S3Object s3Object = s3ObjectMapper.getById(streamObject.getObjectId());
                            s3Object.setMarkedForDeletionTimestamp(new Date());
                            s3ObjectMapper.markToDelete(s3Object.getId(), new Date());
                        }
                    });

                    // remove wal object or remove sub-stream range in wal object
                    s3WALObjectMapper.list(stream.getDstNodeId(), null).stream()
                        .map(s3WALObject -> {
                            Map<Long, SubStream> subStreams = gson.fromJson(new String(s3WALObject.getSubStreams().getBytes(StandardCharsets.UTF_8)), new TypeToken<Map<Long, SubStream>>() {
                            }.getType());
                            return buildS3WALObject(s3WALObject, subStreams);
                        })
                        .filter(s3WALObject -> s3WALObject.getSubStreamsMap().containsKey(streamId))
                        .filter(s3WALObject -> s3WALObject.getSubStreamsMap().get(streamId).getEndOffset() <= newStartOffset)
                        .forEach(s3WALObject -> {
                            if (s3WALObject.getSubStreamsMap().size() == 1) {
                                // only this range, but we will remove this range, so now we can remove this wal object
                                S3Object s3Object = s3ObjectMapper.getById(s3WALObject.getObjectId());
                                s3Object.setMarkedForDeletionTimestamp(new Date());
                                s3ObjectMapper.markToDelete(s3Object.getId(), new Date());
                            }

                            // remove offset range about sub-stream ...
                        });
                    session.commit();
                    LOGGER.info("Node[node-id={}] trim stream [stream-id={}] with epoch={} and newStartOffset={}",
                        this.lease.getNodeId(), streamId, streamEpoch, newStartOffset);
                    future.complete(null);
                }
            } else {
                TrimStreamRequest request = TrimStreamRequest.newBuilder()
                    .setStreamId(streamId)
                    .setStreamEpoch(streamEpoch)
                    .setNewStartOffset(newStartOffset)
                    .build();
                try {
                    this.controllerClient.trimStream(this.leaderAddress(), request).whenComplete(((reply, e) -> {
                        if (null != e) {
                            future.completeExceptionally(e);
                        } else {
                            future.complete(null);
                        }
                    }));
                } catch (ControllerException e) {
                    future.completeExceptionally(e);
                }
            }
            break;
        }
        return future;
    }

    @Override
    public CompletableFuture<StreamMetadata> openStream(long streamId, long epoch, int nodeId) {
        CompletableFuture<StreamMetadata> future = new CompletableFuture<>();
        for (; ; ) {
            if (isLeader()) {
                try (SqlSession session = openSession()) {
                    if (!maintainLeadershipWithSharedLock(session)) {
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
                        this.lease.getNodeId(), streamId, epoch + 1);
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
                OpenStreamRequest request = OpenStreamRequest.newBuilder()
                    .setStreamId(streamId)
                    .setStreamEpoch(epoch)
                    .build();
                try {
                    return this.controllerClient.openStream(this.leaderAddress(), request)
                        .thenApply(OpenStreamReply::getStreamMetadata);
                } catch (ControllerException e) {
                    future.completeExceptionally(e);
                }
            }
        }
    }

    @Override
    public CompletableFuture<Void> closeStream(long streamId, long streamEpoch, int nodeId) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        for (; ; ) {
            if (isLeader()) {
                try (SqlSession session = openSession()) {
                    if (!maintainLeadershipWithSharedLock(session)) {
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
                CloseStreamRequest request = CloseStreamRequest.newBuilder()
                    .setStreamId(streamId)
                    .setStreamEpoch(streamEpoch)
                    .build();
                try {
                    this.controllerClient.closeStream(this.leaderAddress(), request).whenComplete(((reply, e) -> {
                        if (null != e) {
                            future.completeExceptionally(e);
                        } else {
                            future.complete(null);
                        }
                    }));
                } catch (ControllerException e) {
                    future.completeExceptionally(e);
                }
                break;
            }
        }
        return future;
    }

    @Override
    public CompletableFuture<List<StreamMetadata>> listOpenStreams(int nodeId) {
        CompletableFuture<List<StreamMetadata>> future = new CompletableFuture<>();
        for (; ; ) {
            if (isLeader()) {
                try (SqlSession session = openSession()) {
                    if (!maintainLeadershipWithSharedLock(session)) {
                        continue;
                    }
                    StreamMapper streamMapper = session.getMapper(StreamMapper.class);
                    RangeMapper rangeMapper = session.getMapper(RangeMapper.class);
                    List<StreamMetadata> streams = streamMapper.listByNode(nodeId, StreamState.OPEN)
                        .stream()
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
                    future.complete(streams);
                    break;
                }
            } else {
                ListOpenStreamsRequest request = ListOpenStreamsRequest.newBuilder()
                    .setBrokerId(nodeId)
                    .build();
                try {
                    return controllerClient.listOpenStreams(leaderAddress(), request)
                        .thenApply((ListOpenStreamsReply::getStreamMetadataList));
                } catch (ControllerException e) {
                    future.completeExceptionally(e);
                }
                break;
            }
        }
        return future;
    }

    @Override
    public CompletableFuture<Long> prepareS3Objects(int count, int ttlInMinutes) {
        CompletableFuture<Long> future = new CompletableFuture<>();
        for (; ; ) {
            if (isLeader()) {
                try (SqlSession session = openSession()) {
                    if (!maintainLeadershipWithSharedLock(session)) {
                        continue;
                    }

                    // Get and update sequence
                    SequenceMapper sequenceMapper = session.getMapper(SequenceMapper.class);
                    long next = sequenceMapper.next(S3ObjectMapper.SEQUENCE_NAME);
                    sequenceMapper.update(S3ObjectMapper.SEQUENCE_NAME, next + count);

                    S3ObjectMapper s3ObjectMapper = session.getMapper(S3ObjectMapper.class);
                    Calendar calendar = Calendar.getInstance();
                    calendar.add(Calendar.MINUTE, ttlInMinutes);
                    IntStream.range(0, count).forEach(i -> {
                        S3Object object = new S3Object();
                        object.setId(next + i);
                        object.setState(S3ObjectState.BOS_PREPARED);
                        object.setExpiredTimestamp(calendar.getTime());
                        s3ObjectMapper.prepare(object);
                    });
                    session.commit();
                    future.complete(next);
                }
            } else {
                PrepareS3ObjectsRequest request = PrepareS3ObjectsRequest.newBuilder()
                    .setPreparedCount(count)
                    .setTimeToLiveMinutes(ttlInMinutes)
                    .build();
                try {
                    this.controllerClient.prepareS3Objects(this.leaderAddress(), request)
                        .thenApply(PrepareS3ObjectsReply::getFirstObjectId);
                } catch (ControllerException e) {
                    future.completeExceptionally(e);
                }
            }
            break;
        }
        return future;
    }

    @Override
    public CompletableFuture<Void> commitWalObject(S3WALObject walObject,
        List<apache.rocketmq.controller.v1.S3StreamObject> streamObjects, List<Long> compactedObjects) {
        if (Objects.isNull(walObject)) {
            LOGGER.error("S3WALObject is unexpectedly null");
            ControllerException e = new ControllerException(Code.INTERNAL_VALUE, "S3WALObject is unexpectedly null");
            return CompletableFuture.failedFuture(e);
        }

        CompletableFuture<Void> future = new CompletableFuture<>();
        for (; ; ) {
            if (isLeader()) {
                try (SqlSession session = openSession()) {
                    if (!maintainLeadershipWithSharedLock(session)) {
                        continue;
                    }

                    S3WalObjectMapper s3WALObjectMapper = session.getMapper(S3WalObjectMapper.class);
                    S3ObjectMapper s3ObjectMapper = session.getMapper(S3ObjectMapper.class);
                    S3StreamObjectMapper s3StreamObjectMapper = session.getMapper(S3StreamObjectMapper.class);

                    int brokerId = walObject.getBrokerId();
                    long objectId = walObject.getObjectId();

                    if (Objects.isNull(compactedObjects) || compactedObjects.isEmpty()) {
                        // verify stream continuity
                        List<long[]> offsets = java.util.stream.Stream.concat(
                            streamObjects.stream()
                                .map(s3StreamObject -> new long[] {s3StreamObject.getStreamId(), s3StreamObject.getStartOffset(), s3StreamObject.getEndOffset()}),
                            walObject.getSubStreamsMap().entrySet()
                                .stream()
                                .map(obj -> new long[] {obj.getKey(), obj.getValue().getStartOffset(), obj.getValue().getEndOffset()})
                        ).toList();

                        if (!checkStreamAdvance(session, offsets)) {
                            LOGGER.error("S3WALObject[object-id={}]'s stream advance check failed", walObject.getObjectId());
                            ControllerException e = new ControllerException(Code.NOT_FOUND_VALUE, String.format("S3WALObject[object-id=%d]'s stream advance check failed", walObject.getObjectId()));
                            future.completeExceptionally(e);
                            return future;
                        }
                    }

                    // commit S3 object
                    if (objectId != S3Constants.NOOP_OBJECT_ID && !commitObject(objectId, session)) {
                        ControllerException e = new ControllerException(Code.ILLEGAL_STATE_VALUE,
                            String.format("S3WALObject[object-id=%d] is not prepare", walObject.getObjectId()));
                        future.completeExceptionally(e);
                        return future;
                    }

                    long dataTs = System.currentTimeMillis();
                    if (!Objects.isNull(compactedObjects) && !compactedObjects.isEmpty()) {
                        // update dataTs to the min compacted object's dataTs
                        dataTs = compactedObjects.stream()
                            .map(id -> {
                                // mark destroy compacted object
                                S3Object object = s3ObjectMapper.getById(id);
                                object.setState(S3ObjectState.BOS_WILL_DELETE);
                                object.setMarkedForDeletionTimestamp(new Date());
                                s3ObjectMapper.markToDelete(object.getId(), new Date());

                                S3WalObject s3WALObject = s3WALObjectMapper.getByObjectId(id);
                                return s3WALObject.getBaseDataTimestamp();
                            })
                            .min(Long::compareTo).get();
                    }

                    // update broker's wal object
                    if (objectId != S3Constants.NOOP_OBJECT_ID) {
                        // generate broker's wal object record
                        S3WalObject s3WALObject = new S3WalObject();
                        s3WALObject.setObjectId(objectId);
                        s3WALObject.setObjectSize(walObject.getObjectSize());
                        s3WALObject.setBaseDataTimestamp(dataTs);
                        s3WALObject.setCommittedTimestamp(System.currentTimeMillis());
                        s3WALObject.setNodeId(brokerId);
                        s3WALObject.setSequenceId(walObject.getSequenceId());
                        s3WALObject.setSubStreams(gson.toJson(walObject.getSubStreamsMap()));
                        s3WALObjectMapper.create(s3WALObject);
                    }

                    // commit stream objects;
                    if (!Objects.isNull(streamObjects) && !streamObjects.isEmpty()) {
                        for (apache.rocketmq.controller.v1.S3StreamObject s3StreamObject : streamObjects) {
                            long oId = s3StreamObject.getObjectId();
                            if (!commitObject(oId, session)) {
                                ControllerException e = new ControllerException(Code.ILLEGAL_STATE_VALUE, String.format("S3StreamObject[object-id=%d] is not prepare", oId));
                                future.completeExceptionally(e);
                                return future;
                            }
                        }
                        // create stream object records
                        streamObjects.forEach(s3StreamObject -> {
                            S3StreamObject object = new S3StreamObject();
                            object.setStreamId(s3StreamObject.getStreamId());
                            object.setObjectId(s3StreamObject.getObjectId());
                            object.setCommittedTimestamp(System.currentTimeMillis());
                            object.setStartOffset(s3StreamObject.getStartOffset());
                            object.setBaseDataTimestamp(s3StreamObject.getBaseDataTimestamp());
                            object.setEndOffset(s3StreamObject.getEndOffset());
                            object.setObjectSize(s3StreamObject.getObjectSize());
                            s3StreamObjectMapper.commit(object);
                        });
                    }

                    // generate compacted objects' remove record ...
                    if (!Objects.isNull(compactedObjects) && !compactedObjects.isEmpty()) {
                        compactedObjects.forEach(id -> s3WALObjectMapper.delete(id, null, null));
                    }
                    session.commit();
                    LOGGER.info("broker[broke-id={}] commit wal object[object-id={}] success, compacted objects[{}], stream objects[{}]",
                        brokerId, walObject.getObjectId(), compactedObjects, streamObjects);
                    future.complete(null);
                }
            } else {
                CommitWALObjectRequest request = CommitWALObjectRequest.newBuilder()
                    .setS3WalObject(walObject)
                    .addAllS3StreamObjects(streamObjects)
                    .addAllCompactedObjectIds(compactedObjects)
                    .build();
                try {
                    this.controllerClient.commitWALObject(this.leaderAddress(), request).whenComplete(((reply, e) -> {
                        if (null != e) {
                            future.completeExceptionally(e);
                        } else {
                            future.complete(null);
                        }
                    }));
                } catch (ControllerException e) {
                    future.completeExceptionally(e);
                }
            }
            break;
        }
        return future;
    }

    @Override
    public CompletableFuture<Void> commitStreamObject(apache.rocketmq.controller.v1.S3StreamObject streamObject,
        List<Long> compactedObjects) throws ControllerException {
        CompletableFuture<Void> future = new CompletableFuture<>();
        for (; ; ) {
            if (isLeader()) {
                try (SqlSession session = openSession()) {
                    if (!maintainLeadershipWithSharedLock(session)) {
                        continue;
                    }

                    if (Objects.isNull(streamObject) || streamObject.getObjectId() == S3Constants.NOOP_OBJECT_ID) {
                        LOGGER.error("S3StreamObject[object-id={}] is null or objectId is unavailable", streamObject.getObjectId());
                        ControllerException e = new ControllerException(Code.NOT_FOUND_VALUE, String.format("S3StreamObject[object-id=%d] is null or objectId is unavailable", streamObject.getObjectId()));
                        future.completeExceptionally(e);
                        return future;
                    }

                    long committedTs = System.currentTimeMillis();
                    S3ObjectMapper s3ObjectMapper = session.getMapper(S3ObjectMapper.class);
                    S3StreamObjectMapper s3StreamObjectMapper = session.getMapper(S3StreamObjectMapper.class);

                    // commit object
                    if (!commitObject(streamObject.getObjectId(), session)) {
                        ControllerException e = new ControllerException(Code.ILLEGAL_STATE_VALUE, String.format("S3StreamObject[object-id=%d] is not prepare", streamObject.getObjectId()));
                        future.completeExceptionally(e);
                        return future;
                    }
                    long dataTs = committedTs;
                    if (!Objects.isNull(compactedObjects) && !compactedObjects.isEmpty()) {
                        dataTs = compactedObjects.stream()
                            .map(id -> {
                                // mark destroy compacted object
                                S3Object object = s3ObjectMapper.getById(id);
                                object.setState(S3ObjectState.BOS_WILL_DELETE);
                                object.setMarkedForDeletionTimestamp(new Date());
                                s3ObjectMapper.markToDelete(object.getId(), new Date());

                                // update dataTs to the min compacted object's dataTs
                                S3StreamObject s3StreamObject = s3StreamObjectMapper.getByObjectId(id);
                                return s3StreamObject.getBaseDataTimestamp();
                            })
                            .min(Long::compareTo).get();
                    }
                    // create a new S3StreamObject to replace committed ones
                    S3StreamObject newS3StreamObj = new S3StreamObject();
                    newS3StreamObj.setStreamId(streamObject.getStreamId());
                    newS3StreamObj.setObjectId(streamObject.getObjectId());
                    newS3StreamObj.setObjectSize(streamObject.getObjectSize());
                    newS3StreamObj.setStartOffset(streamObject.getStartOffset());
                    newS3StreamObj.setEndOffset(streamObject.getEndOffset());
                    newS3StreamObj.setBaseDataTimestamp(dataTs);
                    newS3StreamObj.setCommittedTimestamp(committedTs);
                    s3StreamObjectMapper.create(newS3StreamObj);

                    // delete the compactedObjects of S3Stream
                    if (!Objects.isNull(compactedObjects) && !compactedObjects.isEmpty()) {
                        compactedObjects.forEach(id -> s3StreamObjectMapper.delete(null, null, id));
                    }
                    session.commit();
                    LOGGER.info("S3StreamObject[object-id={}] commit success, compacted objects: {}", streamObject.getObjectId(), compactedObjects);
                    future.complete(null);
                }
            } else {
                CommitStreamObjectRequest request = CommitStreamObjectRequest.newBuilder()
                    .setS3StreamObject(streamObject)
                    .addAllCompactedObjectIds(compactedObjects)
                    .build();
                try {
                    this.controllerClient.commitStreamObject(this.leaderAddress(), request).whenComplete(((reply, e) -> {
                        if (null != e) {
                            future.completeExceptionally(e);
                        } else {
                            future.complete(null);
                        }
                    }));
                } catch (ControllerException e) {
                    future.completeExceptionally(e);
                }
            }
            break;
        }
        return future;
    }

    @Override
    public CompletableFuture<List<S3WALObject>> listWALObjects() {
        CompletableFuture<List<S3WALObject>> future = new CompletableFuture<>();
        try (SqlSession session = this.sessionFactory.openSession()) {
            S3WalObjectMapper s3WalObjectMapper = session.getMapper(S3WalObjectMapper.class);
            List<S3WALObject> walObjects = s3WalObjectMapper.list(this.config.nodeId(), null).stream()
                .map(s3WALObject -> {
                    Map<Long, SubStream> subStreams = gson.fromJson(new String(s3WALObject.getSubStreams().getBytes(StandardCharsets.UTF_8)), new TypeToken<Map<Long, SubStream>>() {

                    }.getType());
                    return buildS3WALObject(s3WALObject, subStreams);
                })
                .toList();
            future.complete(walObjects);
        }
        return future;
    }

    @Override
    public CompletableFuture<List<S3WALObject>> listWALObjects(long streamId, long startOffset,
        long endOffset, int limit) {
        CompletableFuture<List<S3WALObject>> future = new CompletableFuture<>();
        try (SqlSession session = this.sessionFactory.openSession()) {
            RangeMapper rangeMapper = session.getMapper(RangeMapper.class);

            List<Integer> nodes = rangeMapper.listByStreamId(streamId)
                .stream()
                .filter(range -> range.getEndOffset() > startOffset && range.getStartOffset() < endOffset)
                .mapToInt(Range::getNodeId)
                .distinct()
                .boxed()
                .toList();

            S3WalObjectMapper s3WalObjectMapper = session.getMapper(S3WalObjectMapper.class);
            List<S3WALObject> s3WALObjects = new ArrayList<>();
            for (int nodeId : nodes) {
                List<S3WalObject> s3WalObjects = s3WalObjectMapper.list(nodeId, null);
                s3WalObjects.stream()
                    .map(s3WalObject -> {
                        TypeToken<Map<Long, SubStream>> typeToken = new TypeToken<>() {
                        };
                        Map<Long, SubStream> subStreams = gson.fromJson(new String(s3WalObject.getSubStreams().getBytes(StandardCharsets.UTF_8)), typeToken.getType());
                        Map<Long, SubStream> streamsRecords = new HashMap<>();
                        if (!Objects.isNull(subStreams) && subStreams.containsKey(streamId)) {
                            SubStream subStream = subStreams.get(streamId);
                            if (subStream.getStartOffset() <= endOffset && subStream.getEndOffset() > startOffset) {
                                streamsRecords.put(streamId, subStream);
                            }
                        }
                        if (!streamsRecords.isEmpty()) {
                            return buildS3WALObject(s3WalObject, streamsRecords);
                        }
                        return null;
                    })
                    .filter(Objects::nonNull)
                    .forEach(s3WALObjects::add);
            }

            // Sort by start-offset of the given stream
            s3WALObjects.sort((l, r) -> {
                long lhs = l.getSubStreamsMap().get(streamId).getStartOffset();
                long rhs = r.getSubStreamsMap().get(streamId).getStartOffset();
                return Long.compare(lhs, rhs);
            });

            future.complete(s3WALObjects.stream().limit(limit).toList());
        }
        return future;
    }

    @Override
    public CompletableFuture<List<apache.rocketmq.controller.v1.S3StreamObject>> listStreamObjects(long streamId,
        long startOffset,
        long endOffset, int limit) {
        CompletableFuture<List<apache.rocketmq.controller.v1.S3StreamObject>> future = new CompletableFuture<>();
        try (SqlSession session = this.sessionFactory.openSession()) {
            S3StreamObjectMapper s3StreamObjectMapper = session.getMapper(S3StreamObjectMapper.class);
            List<apache.rocketmq.controller.v1.S3StreamObject> streamObjects = s3StreamObjectMapper.list(null, streamId, startOffset, endOffset, limit).stream()
                .map(streamObject -> apache.rocketmq.controller.v1.S3StreamObject.newBuilder()
                    .setStreamId(streamObject.getStreamId())
                    .setObjectSize(streamObject.getObjectSize())
                    .setObjectId(streamObject.getObjectId())
                    .setStartOffset(streamObject.getStartOffset())
                    .setEndOffset(streamObject.getEndOffset())
                    .setBaseDataTimestamp(streamObject.getBaseDataTimestamp())
                    .setCommittedTimestamp(streamObject.getCommittedTimestamp())
                    .build())
                .toList();
            future.complete(streamObjects);
        }
        return future;
    }

    private S3WALObject buildS3WALObject(
        S3WalObject originalObject,
        Map<Long, SubStream> subStreams) {
        return S3WALObject.newBuilder()
            .setObjectId(originalObject.getObjectId())
            .setObjectSize(originalObject.getObjectSize())
            .setBrokerId(originalObject.getNodeId())
            .setSequenceId(originalObject.getSequenceId())
            .setBaseDataTimestamp(originalObject.getBaseDataTimestamp())
            .setCommittedTimestamp(originalObject.getCommittedTimestamp())
            .putAllSubStreams(subStreams)
            .build();
    }

    private boolean commitObject(Long objectId, SqlSession session) {
        S3ObjectMapper s3ObjectMapper = session.getMapper(S3ObjectMapper.class);
        S3Object s3Object = s3ObjectMapper.getById(objectId);
        if (Objects.isNull(s3Object)) {
            LOGGER.error("object[object-id={}] not exist", objectId);
            return false;
        }
        // verify the state
        if (s3Object.getState() == S3ObjectState.BOS_COMMITTED) {
            LOGGER.warn("object[object-id={}] already committed", objectId);
            return false;
        }
        if (s3Object.getState() != S3ObjectState.BOS_PREPARED) {
            LOGGER.error("object[object-id={}] is not prepared but try to commit", objectId);
            return false;
        }
        s3Object.setCommittedTimestamp(new Date());
        s3Object.setState(S3ObjectState.BOS_COMMITTED);
        s3ObjectMapper.commit(s3Object);
        return true;
    }

    private boolean checkStreamAdvance(SqlSession session, List<long[]> offsets) {
        if (offsets == null || offsets.isEmpty()) {
            return true;
        }
        StreamMapper streamMapper = session.getMapper(StreamMapper.class);
        RangeMapper rangeMapper = session.getMapper(RangeMapper.class);
        for (long[] offset : offsets) {
            long streamId = offset[0], startOffset = offset[1], endOffset = offset[2];
            // verify the stream exists and is open
            Stream stream = streamMapper.getByStreamId(streamId);
            if (stream.getState() != StreamState.OPEN) {
                LOGGER.warn("Stream[stream-id={}] not opened", streamId);
                return false;
            }

            Range range = rangeMapper.get(stream.getRangeId(), streamId, null);
            if (Objects.isNull(range)) {
                // should not happen
                LOGGER.error("Stream[stream-id={}]'s current range[range-id={}] not exist when stream has been created",
                    streamId, stream.getRangeId());
                return false;
            }

            if (range.getEndOffset() != startOffset) {
                LOGGER.warn("Stream[stream-id={}]'s current range[range-id={}]'s end offset[{}] is not equal to request start offset[{}]",
                    streamId, range.getRangeId(), range.getEndOffset(), startOffset);
                return false;
            }

            range.setEndOffset(endOffset);
            rangeMapper.update(range);
        }
        return true;
    }

    @Override
    public CompletableFuture<Pair<List<apache.rocketmq.controller.v1.S3StreamObject>, List<S3WALObject>>> listObjects(
        long streamId, long startOffset, long endOffset, int limit) {
        return CompletableFuture.supplyAsync(() -> {
            try (SqlSession session = this.sessionFactory.openSession()) {
                S3StreamObjectMapper s3StreamObjectMapper = session.getMapper(S3StreamObjectMapper.class);
                S3WalObjectMapper s3WalObjectMapper = session.getMapper(S3WalObjectMapper.class);
                List<apache.rocketmq.controller.v1.S3StreamObject> s3StreamObjects = s3StreamObjectMapper.list(null, streamId, startOffset, endOffset, limit)
                    .parallelStream()
                    .map(streamObject -> apache.rocketmq.controller.v1.S3StreamObject.newBuilder()
                        .setStreamId(streamObject.getStreamId())
                        .setObjectSize(streamObject.getObjectSize())
                        .setObjectId(streamObject.getObjectId())
                        .setStartOffset(streamObject.getStartOffset())
                        .setEndOffset(streamObject.getEndOffset())
                        .setBaseDataTimestamp(streamObject.getBaseDataTimestamp())
                        .setCommittedTimestamp(streamObject.getCommittedTimestamp())
                        .build())
                    .toList();

                List<S3WALObject> walObjects = new ArrayList<>();
                s3WalObjectMapper.list(config.nodeId(), null)
                    .parallelStream()
                    .map(s3WalObject -> {
                        TypeToken<Map<Long, SubStream>> typeToken = new TypeToken<>() {
                        };
                        Map<Long, SubStream> subStreams = gson.fromJson(new String(s3WalObject.getSubStreams().getBytes(StandardCharsets.UTF_8)), typeToken.getType());
                        Map<Long, SubStream> streamsRecords = new HashMap<>();
                        subStreams.entrySet().stream()
                            .filter(entry -> !Objects.isNull(entry) && entry.getKey().equals(streamId))
                            .filter(entry -> entry.getValue().getStartOffset() <= endOffset && entry.getValue().getEndOffset() > startOffset)
                            .forEach(entry -> streamsRecords.put(entry.getKey(), entry.getValue()));
                        return streamsRecords.isEmpty() ? null : buildS3WALObject(s3WalObject, streamsRecords);
                    })
                    .filter(Objects::nonNull)
                    .limit(limit)
                    .forEach(walObjects::add);

                if (!walObjects.isEmpty()) {
                    walObjects.sort((l, r) -> {
                        long lhs = l.getSubStreamsMap().get(streamId).getStartOffset();
                        long rhs = r.getSubStreamsMap().get(streamId).getStartOffset();
                        return Long.compare(lhs, rhs);
                    });
                }

                // TODO: apply limit in whole.
                return new ImmutablePair<>(s3StreamObjects, walObjects);
            }
        }, asyncExecutorService);
    }

    @Override
    public void close() throws IOException {
        this.scheduledExecutorService.shutdown();
        this.asyncExecutorService.shutdown();
    }

    public void setRole(Role role) {
        this.role = role;
    }

    public void setLease(Lease lease) {
        this.lease = lease;
    }

    public Lease getLease() {
        return lease;
    }

    public void addBrokerNode(Node node) {
        this.nodes.put(node.getId(), new BrokerNode(node));
    }

    public ConcurrentMap<Integer, BrokerNode> getNodes() {
        return nodes;
    }

    public SqlSessionFactory getSessionFactory() {
        return sessionFactory;
    }

    public ControllerConfig getConfig() {
        return config;
    }

    @Override
    public String addressOfNode(int nodeId) {
        BrokerNode node = this.nodes.get(nodeId);
        if (null != node) {
            return node.getNode().getAddress();
        }

        try (SqlSession session = openSession()) {
            NodeMapper mapper = session.getMapper(NodeMapper.class);
            Node n = mapper.get(nodeId, null, null, null);
            if (null != n) {
                return n.getAddress();
            }
        }
        return null;
    }
}