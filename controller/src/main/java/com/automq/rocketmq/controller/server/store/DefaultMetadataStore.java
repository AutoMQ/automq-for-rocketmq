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

package com.automq.rocketmq.controller.server.store;

import apache.rocketmq.common.v1.Code;
import apache.rocketmq.controller.v1.AssignmentStatus;
import apache.rocketmq.controller.v1.Cluster;
import apache.rocketmq.controller.v1.ClusterSummary;
import apache.rocketmq.controller.v1.ConsumerGroup;
import apache.rocketmq.controller.v1.CreateGroupRequest;
import apache.rocketmq.controller.v1.CreateTopicRequest;
import apache.rocketmq.controller.v1.DescribeClusterRequest;
import apache.rocketmq.controller.v1.DescribeStreamReply;
import apache.rocketmq.controller.v1.DescribeStreamRequest;
import apache.rocketmq.controller.v1.StreamMetadata;
import apache.rocketmq.controller.v1.StreamRole;
import apache.rocketmq.controller.v1.StreamState;
import apache.rocketmq.controller.v1.TerminationStage;
import apache.rocketmq.controller.v1.UpdateGroupRequest;
import apache.rocketmq.controller.v1.UpdateTopicRequest;
import com.automq.rocketmq.common.PrefixThreadFactory;
import com.automq.rocketmq.common.api.DataStore;
import com.automq.rocketmq.common.config.ControllerConfig;
import com.automq.rocketmq.common.exception.ControllerException;
import com.automq.rocketmq.controller.ControllerClient;
import com.automq.rocketmq.controller.MetadataStore;
import com.automq.rocketmq.controller.server.store.impl.ElectionServiceImpl;
import com.automq.rocketmq.controller.server.store.impl.GroupManager;
import com.automq.rocketmq.controller.server.store.impl.StreamManager;
import com.automq.rocketmq.controller.server.store.impl.TopicManager;
import com.automq.rocketmq.controller.server.tasks.DataRetentionTask;
import com.automq.rocketmq.controller.server.tasks.HeartbeatTask;
import com.automq.rocketmq.controller.server.tasks.ReclaimS3ObjectTask;
import com.automq.rocketmq.controller.server.tasks.RecycleGroupTask;
import com.automq.rocketmq.controller.server.tasks.RecycleTopicTask;
import com.automq.rocketmq.controller.server.tasks.ScanAssignmentTask;
import com.automq.rocketmq.controller.server.tasks.ScanGroupTask;
import com.automq.rocketmq.controller.server.tasks.ScanNodeTask;
import com.automq.rocketmq.controller.server.tasks.ScanStreamTask;
import com.automq.rocketmq.controller.server.tasks.ScanTopicTask;
import com.automq.rocketmq.controller.server.tasks.ScanYieldingQueueTask;
import com.automq.rocketmq.metadata.dao.Group;
import com.automq.rocketmq.metadata.dao.GroupProgress;
import com.automq.rocketmq.metadata.dao.Lease;
import com.automq.rocketmq.metadata.dao.Node;
import com.automq.rocketmq.metadata.dao.QueueAssignment;
import com.automq.rocketmq.metadata.dao.Stream;
import com.automq.rocketmq.metadata.dao.StreamCriteria;
import com.automq.rocketmq.metadata.dao.Topic;
import com.automq.rocketmq.metadata.mapper.GroupProgressMapper;
import com.automq.rocketmq.metadata.mapper.LeaseMapper;
import com.automq.rocketmq.metadata.mapper.NodeMapper;
import com.automq.rocketmq.metadata.mapper.QueueAssignmentMapper;
import com.automq.rocketmq.metadata.mapper.StreamMapper;
import com.google.common.base.Strings;
import com.google.protobuf.Timestamp;
import java.io.IOException;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
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
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultMetadataStore implements MetadataStore {

    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultMetadataStore.class);

    private final ControllerClient controllerClient;

    private final SqlSessionFactory sessionFactory;

    private final ControllerConfig config;

    private final ConcurrentMap<Integer, BrokerNode> nodes;

    private final ScheduledExecutorService scheduledExecutorService;

    private final ExecutorService asyncExecutorService;

    private final TopicManager topicManager;

    private final GroupManager groupManager;

    private final StreamManager streamManager;

    private DataStore dataStore;

    private ElectionService electionService;

    public DefaultMetadataStore(ControllerClient client, SqlSessionFactory sessionFactory, ControllerConfig config) {
        this.controllerClient = client;
        this.sessionFactory = sessionFactory;
        this.config = config;
        this.nodes = new ConcurrentHashMap<>();
        this.scheduledExecutorService = new ScheduledThreadPoolExecutor(Runtime.getRuntime().availableProcessors(),
            new PrefixThreadFactory("Controller"));
        this.asyncExecutorService = Executors.newFixedThreadPool(10, new PrefixThreadFactory("Controller-Async"));
        this.topicManager = new TopicManager(this);
        this.groupManager = new GroupManager(this);
        this.streamManager = new StreamManager(this);
        this.electionService = new ElectionServiceImpl(this, scheduledExecutorService);
    }

    @Override
    public ControllerConfig config() {
        return config;
    }

    @Override
    public ExecutorService asyncExecutor() {
        return asyncExecutorService;
    }

    @Override
    public SqlSession openSession() {
        return sessionFactory.openSession(false);
    }

    @Override
    public SqlSessionFactory sessionFactory() {
        return sessionFactory;
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

    @Override
    public ElectionService electionService() {
        return electionService;
    }

    @Override
    public List<QueueAssignment> assignmentsOf(int nodeId) {
        return topicManager.getAssignmentCache().byNode(nodeId);
    }

    @Override
    public List<Long> streamsOf(long topicId, int queueId) {
        return topicManager.getStreamCache().streamsOf(topicId, queueId);
    }

    /**
     * Expose for test purpose only.
     *
     * @param electionService Provided election service.
     */
    public void setElectionService(ElectionService electionService) {
        this.electionService = electionService;
    }

    public void start() {
        electionService.start();
        this.scheduledExecutorService.scheduleWithFixedDelay(new ScanNodeTask(this), 1,
            config.scanIntervalInSecs(), TimeUnit.SECONDS);
        this.scheduledExecutorService.scheduleWithFixedDelay(new ScanYieldingQueueTask(this), 1,
            config.scanIntervalInSecs(), TimeUnit.SECONDS);
        // Disable balance workload for now
//        this.scheduledExecutorService.scheduleWithFixedDelay(new SchedulerTask(this), 1,
//            config.balanceWorkloadIntervalInSecs(), TimeUnit.SECONDS);
        this.scheduledExecutorService.scheduleAtFixedRate(new HeartbeatTask(this), 3,
            Math.max(config().nodeAliveIntervalInSecs() / 2, 10), TimeUnit.SECONDS);
        this.scheduledExecutorService.scheduleWithFixedDelay(new RecycleTopicTask(this), 1,
            config.deletedTopicLingersInSecs(), TimeUnit.SECONDS);
        this.scheduledExecutorService.scheduleWithFixedDelay(new RecycleGroupTask(this), 1,
            config.deletedGroupLingersInSecs(), TimeUnit.SECONDS);
        this.scheduledExecutorService.scheduleWithFixedDelay(new ReclaimS3ObjectTask(this), 1,
            config.scanIntervalInSecs(), TimeUnit.SECONDS);
        this.scheduledExecutorService.scheduleWithFixedDelay(new ScanTopicTask(this), 1,
            config.scanIntervalInSecs(), TimeUnit.SECONDS);
        this.scheduledExecutorService.scheduleWithFixedDelay(new ScanGroupTask(this), 1,
            config.scanIntervalInSecs(), TimeUnit.SECONDS);
        this.scheduledExecutorService.scheduleWithFixedDelay(new ScanAssignmentTask(this), 1,
            config.scanIntervalInSecs(), TimeUnit.SECONDS);
        this.scheduledExecutorService.scheduleWithFixedDelay(new ScanStreamTask(this), 1,
            config.scanIntervalInSecs(), TimeUnit.SECONDS);
        this.scheduledExecutorService.scheduleWithFixedDelay(new DataRetentionTask(this),
            config.recycleS3IntervalInSecs(), config.recycleS3IntervalInSecs(), TimeUnit.SECONDS);
        LOGGER.info("MetadataStore tasks scheduled");
    }

    private static Timestamp toTimestamp(Date time) {
        return Timestamp.newBuilder()
            .setSeconds(TimeUnit.MILLISECONDS.toSeconds(time.getTime()))
            .setNanos((int) TimeUnit.MILLISECONDS.toNanos(time.getTime() % 1000))
            .build();
    }

    @Override
    public CompletableFuture<Cluster> describeCluster(DescribeClusterRequest request) {
        if (isLeader()) {
            assert electionService.leaderEpoch().isPresent();
            assert electionService.leaderNodeId().isPresent();
            assert electionService.leaseExpirationTime().isPresent();
            Cluster.Builder builder = Cluster.newBuilder()
                .setLease(apache.rocketmq.controller.v1.Lease.newBuilder()
                    .setEpoch(electionService.leaderEpoch().get())
                    .setNodeId(electionService.leaderNodeId().get())
                    .setExpirationTimestamp(toTimestamp(electionService.leaseExpirationTime().get())).build());

            builder.setSummary(ClusterSummary.newBuilder()
                .setNodeQuantity(nodes.size())
                .setTopicQuantity(topicManager.topicQuantity())
                .setQueueQuantity(topicManager.queueQuantity())
                .setStreamQuantity(topicManager.streamQuantity())
                .setGroupQuantity(groupManager.getGroupCache().groupQuantity())
                .build());

            for (Map.Entry<Integer, BrokerNode> entry : nodes.entrySet()) {
                BrokerNode brokerNode = entry.getValue();
                apache.rocketmq.controller.v1.Node node = apache.rocketmq.controller.v1.Node.newBuilder()
                    .setId(entry.getKey())
                    .setName(brokerNode.getNode().getName())
                    .setLastHeartbeat(toTimestamp(brokerNode.lastKeepAliveTime(config)))
                    .setTopicNum(topicManager.topicNumOfNode(entry.getKey()))
                    .setQueueNum(topicManager.queueNumOfNode(entry.getKey()))
                    .setStreamNum(topicManager.streamNumOfNode(entry.getKey()))
                    .setGoingAway(brokerNode.isGoingAway())
                    .setAddress(brokerNode.getNode().getAddress())
                    .build();
                builder.addNodes(node);
            }
            return CompletableFuture.completedFuture(builder.build());
        } else {
            Optional<String> leaderAddress = electionService.leaderAddress();
            if (leaderAddress.isEmpty()) {
                return CompletableFuture.failedFuture(new ControllerException(Code.NO_LEADER_VALUE, "No leader is elected yet"));
            }
            return controllerClient.describeCluster(leaderAddress.get(), request);
        }
    }

    @Override
    public CompletableFuture<Node> registerBrokerNode(String name, String address, String instanceId) {
        LOGGER.info("Register broker node with name={}, address={}, instance-id={}", name, address, instanceId);
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
                        Node node = nodeMapper.get(null, name, null, null);
                        if (null != node) {
                            if (!Strings.isNullOrEmpty(address)) {
                                node.setAddress(address);
                            }
                            if (!Strings.isNullOrEmpty(instanceId)) {
                                node.setInstanceId(instanceId);
                            }
                            node.setEpoch(node.getEpoch() + 1);
                            nodeMapper.update(node);
                        } else {
                            LeaseMapper leaseMapper = session.getMapper(LeaseMapper.class);
                            Lease lease = leaseMapper.currentWithShareLock();
                            assert electionService.leaderEpoch().isPresent();
                            assert electionService.leaderNodeId().isPresent();
                            assert electionService.leaseExpirationTime().isPresent();
                            if (lease.getEpoch() != electionService.leaderEpoch().get()) {
                                // Refresh cached lease
                                electionService.updateLease(lease);
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
                    Optional<String> leaderAddress = electionService.leaderAddress();
                    if (leaderAddress.isEmpty()) {
                        throw new CompletionException(new ControllerException(Code.NO_LEADER_VALUE, "No leader is elected yet"));
                    }
                    return controllerClient.registerBroker(leaderAddress.get(), name, address, instanceId).join();
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
        if (!isLeader()) {
            LOGGER.warn("Non-leader node cannot keep the node[node-id={}, epoch={}] alive", nodeId, epoch);
            return;
        }
        BrokerNode brokerNode = nodes.get(nodeId);
        if (null != brokerNode) {
            brokerNode.keepAlive(epoch, goingAway);
        }
    }

    @Override
    public void heartbeat() {
        if (isLeader()) {
            LOGGER.debug("Node of leader does not need to send heartbeat request");
            return;
        }

        Optional<String> leaderAddress = electionService.leaderAddress();
        if (leaderAddress.isEmpty()) {
            LOGGER.warn("No leader is elected yet");
            return;
        }

        String target = leaderAddress.get();
        controllerClient.heartbeat(target, config.nodeId(), config.epoch(), config.goingAway())
            .whenComplete((r, e) -> {
                if (null != e) {
                    LOGGER.error("Failed to maintain heartbeat to {}", target);
                    return;
                }
                LOGGER.debug("Heartbeat to {} OK", target);
            });
    }

    public boolean maintainLeadershipWithSharedLock(SqlSession session) {
        LeaseMapper leaseMapper = session.getMapper(LeaseMapper.class);
        Lease current = leaseMapper.currentWithShareLock();
        assert electionService.leaderEpoch().isPresent();
        if (current.getEpoch() != electionService.leaderEpoch().get()) {
            // Current node is not leader any longer, forward to the new leader in the next iteration.
            electionService.updateLease(current);
            return false;
        }
        return true;
    }

    @Override
    public boolean isLeader() {
        if (electionService.leaderNodeId().isEmpty()) {
            return false;
        }
        return electionService.leaderNodeId().get() == config.nodeId();
    }

    @Override
    public boolean hasAliveBrokerNodes() {
        return this.nodes.values().stream().anyMatch(node -> node.isAlive(config));
    }

    @Override
    public Optional<String> leaderAddress() {
        return electionService.leaderAddress();
    }

    @Override
    public CompletableFuture<Long> createTopic(CreateTopicRequest request) {
        return topicManager.createTopic(request);
    }

    @Override
    public CompletableFuture<Void> deleteTopic(long topicId) {
        return topicManager.deleteTopic(topicId);
    }

    @Override
    public CompletableFuture<apache.rocketmq.controller.v1.Topic> describeTopic(Long topicId, String topicName) {
        return topicManager.describeTopic(topicId, topicName);
    }

    @Override
    public CompletableFuture<List<apache.rocketmq.controller.v1.Topic>> listTopics() {
        return topicManager.listTopics();
    }

    @Override
    public CompletableFuture<apache.rocketmq.controller.v1.Topic> updateTopic(UpdateTopicRequest request) {
        return topicManager.updateTopic(request);
    }

    @Override
    public CompletableFuture<Long> createGroup(CreateGroupRequest request) {
        return groupManager.createGroup(request);
    }

    @Override
    public CompletableFuture<ConsumerGroup> describeGroup(Long groupId, String groupName) {
        return groupManager.describeGroup(groupId, groupName);
    }

    @Override
    public CompletableFuture<Void> updateGroup(UpdateGroupRequest request) {
        return groupManager.updateGroup(request);
    }

    @Override
    public CompletableFuture<ConsumerGroup> deleteGroup(long groupId) {
        return groupManager.deleteGroup(groupId);
    }

    @Override
    public CompletableFuture<Collection<ConsumerGroup>> listGroups() {
        return groupManager.listGroups();
    }

    @Override
    public ConcurrentMap<Integer, BrokerNode> allNodes() {
        return nodes;
    }

    @Override
    public Optional<Integer> ownerNode(long topicId, int queueId) {
        return topicManager.ownerNode(topicId, queueId);
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
    public CompletableFuture<Void> reassignMessageQueue(long topicId, int queueId, int dstNodeId) {
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
                Optional<String> leaderAddress = electionService.leaderAddress();
                if (leaderAddress.isEmpty()) {
                    return CompletableFuture.failedFuture(new ControllerException(Code.NO_LEADER_VALUE, "No leader is elected yet"));
                }

                this.controllerClient.reassignMessageQueue(leaderAddress.get(), topicId, queueId, dstNodeId).whenComplete((res, e) -> {
                    if (e != null) {
                        future.completeExceptionally(e);
                    } else {
                        future.complete(null);
                    }
                });
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
                    if (!maintainLeadershipWithSharedLock(session)) {
                        continue;
                    }

                    QueueAssignmentMapper assignmentMapper = session.getMapper(QueueAssignmentMapper.class);
                    QueueAssignment assignment = assignmentMapper.get(topicId, queueId);
                    assignment.setSrcNodeId(assignment.getDstNodeId());
                    assignment.setStatus(AssignmentStatus.ASSIGNMENT_STATUS_ASSIGNED);
                    assignmentMapper.update(assignment);

                    StreamMapper streamMapper = session.getMapper(StreamMapper.class);
                    StreamCriteria criteria = StreamCriteria.newBuilder()
                        .withTopicId(topicId)
                        .withQueueId(queueId)
                        .build();
                    streamMapper.updateStreamAssignment(criteria, assignment.getDstNodeId(), assignment.getDstNodeId());
                    session.commit();
                    LOGGER.info("Update status of queue assignment and stream since all its belonging streams are closed," +
                        " having topic-id={}, queue-id={}", topicId, queueId);

                    if (assignment.getDstNodeId() == config.nodeId()) {
                        applyAssignmentChange(List.of(assignment));
                        dataStore.openQueue(topicId, queueId)
                            .whenComplete((res, e) -> {
                                if (null != e) {
                                    future.completeExceptionally(e);
                                }
                                future.complete(null);
                            });
                        return future;
                    }

                    // Notify the destination node that this queue is assignable
                    BrokerNode node = nodes.get(assignment.getDstNodeId());
                    if (node != null) {
                        controllerClient.notifyQueueClose(node.getNode().getAddress(), topicId, queueId)
                            .whenComplete((res, e) -> {
                                if (null != e) {
                                    future.completeExceptionally(e);
                                }
                                future.complete(null);
                            });
                    } else {
                        future.complete(null);
                        LOGGER.warn("Node[{}] is not found, can not notify it that topic-id={}, queue-id={} is assigned", assignment.getDstNodeId(), topicId, queueId);
                    }
                }
                return future;
            } else {
                // Check if this node is the leader of the queue
                try (SqlSession session = openSession()) {
                    QueueAssignmentMapper assignmentMapper = session.getMapper(QueueAssignmentMapper.class);
                    QueueAssignment assignment = assignmentMapper.get(topicId, queueId);
                    if (assignment.getSrcNodeId() == config.nodeId() && assignment.getStatus() == AssignmentStatus.ASSIGNMENT_STATUS_ASSIGNED) {
                        applyAssignmentChange(List.of(assignment));
                        return dataStore.openQueue(topicId, queueId);
                    }
                }

                // Forward to leader
                Optional<String> leaderAddress = electionService.leaderAddress();
                if (leaderAddress.isEmpty()) {
                    return CompletableFuture.failedFuture(new ControllerException(Code.NO_LEADER_VALUE, "No leader is elected yet"));
                }
                this.controllerClient.notifyQueueClose(leaderAddress.get(), topicId, queueId).whenComplete((res, e) -> {
                    if (null != e) {
                        future.completeExceptionally(e);
                    } else {
                        future.complete(null);
                    }
                });
                return future;
            }
        }
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
                Optional<String> leaderAddress = electionService.leaderAddress();
                if (leaderAddress.isEmpty()) {
                    return CompletableFuture.failedFuture(new ControllerException(Code.NO_LEADER_VALUE, "No leader is elected yet"));
                }
                this.controllerClient.commitOffset(leaderAddress.get(), groupId, topicId, queueId, offset).whenComplete((res, e) -> {
                    if (null != e) {
                        future.completeExceptionally(e);
                    } else {
                        future.complete(null);
                    }
                });
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
    public CompletableFuture<StreamMetadata> getStream(long topicId, int queueId, Long groupId, StreamRole streamRole) {
        return streamManager.getStream(topicId, queueId, groupId, streamRole);
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
                    assignment.setSrcNodeId(assignment.getDstNodeId());
                    assignment.setStatus(AssignmentStatus.ASSIGNMENT_STATUS_ASSIGNED);
                    assignmentMapper.update(assignment);

                    StreamMapper streamMapper = session.getMapper(StreamMapper.class);

                    StreamCriteria criteria = StreamCriteria.newBuilder()
                        .withTopicId(topicId)
                        .withQueueId(queueId)
                        .build();
                    streamMapper.updateStreamAssignment(criteria, assignment.getDstNodeId(), assignment.getDstNodeId());
                    session.commit();
                    LOGGER.info("Update status of queue assignment and stream since all its belonging streams are closed," +
                        " having topic-id={}, queue-id={}", topicId, queueId);

                    // Notify the destination node that this queue is assignable
                    BrokerNode node = nodes.get(assignment.getDstNodeId());
                    if (node != null) {
                        controllerClient.notifyQueueClose(node.getNode().getAddress(), topicId, queueId)
                            .whenComplete((res, e) -> {
                                if (null != e) {
                                    future.completeExceptionally(e);
                                }
                                future.complete(null);
                            });
                    } else {
                        future.complete(null);
                        LOGGER.warn("Node[{}] is not found, can not notify it that topic-id={}, queue-id={} is assigned", assignment.getDstNodeId(), topicId, queueId);
                    }
                    return future;
                }
            } else {
                Optional<String> leaderAddress = electionService.leaderAddress();
                if (leaderAddress.isEmpty()) {
                    return CompletableFuture.failedFuture(new ControllerException(Code.NO_LEADER_VALUE, "No leader is elected yet"));
                }
                controllerClient.notifyQueueClose(leaderAddress.get(), topicId, queueId)
                    .whenComplete((res, e) -> {
                        if (null != e) {
                            future.completeExceptionally(e);
                        }
                        future.complete(null);
                    });
                return future;
            }
        }
    }

    @Override
    public CompletableFuture<StreamMetadata> openStream(long streamId, long epoch, int nodeId) {
        return streamManager.openStream(streamId, epoch, nodeId);
    }

    @Override
    public CompletableFuture<Void> closeStream(long streamId, long streamEpoch, int nodeId) {
        return streamManager.closeStream(streamId, streamEpoch, nodeId);
    }

    @Override
    public CompletableFuture<List<StreamMetadata>> listOpenStreams(int nodeId) {
        return streamManager.listOpenStreams(nodeId);
    }

    @Override
    public CompletableFuture<List<StreamMetadata>> getStreams(List<Long> streamIds) {
        return streamManager.getStreams(streamIds);
    }

    @Override
    public CompletableFuture<DescribeStreamReply> describeStream(DescribeStreamRequest request) {
        return streamManager.describeStream(request);
    }

    @Override
    public TerminationStage fireClose() {
        config.flagGoingAway();
        if (isLeader()) {
            // TODO: yield leadership
            return TerminationStage.TS_TRANSFERRING_LEADERSHIP;
        } else {
            // Notify leader that this node is going away shortly
            heartbeat();
            return TerminationStage.TS_TRANSFERRING_STREAM;
        }
    }

    @Override
    public TopicManager topicManager() {
        return topicManager;
    }

    @Override
    public Optional<BrokerNode> getNode(int nodeId) {
        BrokerNode node = nodes.get(nodeId);
        if (null == node) {
            try (SqlSession session = openSession()) {
                NodeMapper mapper = session.getMapper(NodeMapper.class);
                Node rawNode = mapper.get(nodeId, null, null, null);
                if (null != rawNode) {
                    addBrokerNode(rawNode);
                }
            }

            node = nodes.get(nodeId);
            if (null == node) {
                return Optional.empty();
            }
        }
        return Optional.of(node);
    }

    @Override
    public void close() throws IOException {
        this.scheduledExecutorService.shutdown();
        this.asyncExecutorService.shutdown();
    }

    public void addBrokerNode(Node node) {
        this.nodes.put(node.getId(), new BrokerNode(node));
    }

    public ConcurrentMap<Integer, BrokerNode> getNodes() {
        return nodes;
    }

    public ControllerConfig getConfig() {
        return config;
    }

    @Override
    public CompletableFuture<String> addressOfNode(int nodeId) {
        BrokerNode node = this.nodes.get(nodeId);
        if (null != node) {
            return CompletableFuture.completedFuture(node.getNode().getAddress());
        }

        return CompletableFuture.supplyAsync(() -> {
            try (SqlSession session = openSession()) {
                NodeMapper mapper = session.getMapper(NodeMapper.class);
                Node n = mapper.get(nodeId, null, null, null);
                if (null != n) {
                    return n.getAddress();
                }
            }
            return null;
        }, asyncExecutorService);
    }

    @Override
    public void applyTopicChange(List<Topic> topics) {
        topicManager.getTopicCache().apply(topics);
    }

    @Override
    public void applyAssignmentChange(List<QueueAssignment> assignments) {
        topicManager.getAssignmentCache().apply(assignments);
    }

    @Override
    public void applyGroupChange(List<Group> groups) {
        this.groupManager.getGroupCache().apply(groups);
    }

    @Override
    public void applyStreamChange(List<Stream> streams) {
        this.topicManager.getStreamCache().apply(streams);

        // delete associated S3 assets
        if (isLeader()) {
            for (Stream stream : streams) {
                if (stream.getState() == StreamState.DELETED) {
                    try {
                        LOGGER.info("Delete associated S3 resources for stream[stream-id={}]", stream.getId());
                        streamManager.deleteStream(stream.getId());
                        LOGGER.info("Associated S3 resources deleted for stream[stream-id={}]", stream.getId());
                    } catch (Throwable e) {
                        LOGGER.error("Unexpected exception raised while cleaning S3 resources on stream deletion", e);
                    }
                }
            }
        }

    }
}