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

import apache.rocketmq.controller.v1.Code;
import apache.rocketmq.controller.v1.MessageQueue;
import apache.rocketmq.controller.v1.MessageQueueAssignment;
import apache.rocketmq.controller.v1.OngoingMessageQueueReassignment;
import com.automq.rocketmq.common.PrefixThreadFactory;
import com.automq.rocketmq.controller.exception.ControllerException;
import com.automq.rocketmq.controller.metadata.BrokerNode;
import com.automq.rocketmq.controller.metadata.ControllerClient;
import com.automq.rocketmq.controller.metadata.ControllerConfig;
import com.automq.rocketmq.controller.metadata.MetadataStore;
import com.automq.rocketmq.controller.metadata.Role;
import com.automq.rocketmq.controller.metadata.database.dao.Node;
import com.automq.rocketmq.controller.metadata.database.dao.QueueAssignment;
import com.automq.rocketmq.controller.metadata.database.dao.QueueAssignmentStatus;
import com.automq.rocketmq.controller.metadata.database.dao.Topic;
import com.automq.rocketmq.controller.metadata.database.dao.TopicStatus;
import com.automq.rocketmq.controller.metadata.database.mapper.NodeMapper;
import com.automq.rocketmq.controller.metadata.database.mapper.LeaseMapper;
import com.automq.rocketmq.controller.metadata.database.dao.Lease;
import com.automq.rocketmq.controller.metadata.database.mapper.QueueAssignmentMapper;
import com.automq.rocketmq.controller.metadata.database.mapper.TopicMapper;
import com.automq.rocketmq.controller.metadata.database.tasks.LeaseTask;
import com.automq.rocketmq.controller.metadata.database.tasks.ScanNodeTask;
import com.google.common.base.Strings;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
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

    private final ScheduledExecutorService executorService;

    /// The following fields are runtime specific
    private Lease lease;

    public DefaultMetadataStore(ControllerClient client, SqlSessionFactory sessionFactory, ControllerConfig config) {
        controllerClient = client;
        this.sessionFactory = sessionFactory;
        this.config = config;
        this.role = Role.Follower;
        this.nodes = new ConcurrentHashMap<>();
        this.executorService = new ScheduledThreadPoolExecutor(Runtime.getRuntime().availableProcessors(),
            new PrefixThreadFactory("Controller"));

    }

    public void start() {
        this.executorService.scheduleAtFixedRate(new LeaseTask(this), 1, config.scanIntervalInSecs(), TimeUnit.SECONDS);
        this.executorService.scheduleWithFixedDelay(new ScanNodeTask(this), 1, config.scanIntervalInSecs(), TimeUnit.SECONDS);
        LOGGER.info("MetadataStore tasks scheduled");
    }

    @Override
    public Node registerBrokerNode(String name, String address, String instanceId) throws ControllerException {
        if (Strings.isNullOrEmpty(name)) {
            throw new ControllerException(Code.BAD_REQUEST_VALUE, "Broker name is null or empty");
        }

        if (Strings.isNullOrEmpty(address)) {
            throw new ControllerException(Code.BAD_REQUEST_VALUE, "Broker address is null or empty");
        }

        if (Strings.isNullOrEmpty(instanceId)) {
            throw new ControllerException(Code.BAD_REQUEST_VALUE, "Broker instance-id is null or empty");
        }

        for (; ; ) {
            if (this.isLeader()) {
                try (SqlSession session = this.sessionFactory.openSession(false)) {
                    NodeMapper nodeMapper = session.getMapper(NodeMapper.class);
                    Node node = nodeMapper.get(null, instanceId, null);
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
                    return controllerClient.registerBroker(this.leaderAddress(), name, address, instanceId).get();
                } catch (InterruptedException | ExecutionException e) {
                    throw new ControllerException(Code.INTERRUPTED_VALUE, e);
                }
            }
        }
    }

    @Override
    public void keepAlive(int nodeId, long epoch, boolean goingAway) {
        BrokerNode brokerNode = nodes.get(nodeId);
        if (null != brokerNode) {
            brokerNode.keepAlive(epoch, goingAway);
        }
    }

    private boolean maintainLeadershipWithSharedLock(SqlSession session) {
        LeaseMapper leaseMapper = session.getMapper(LeaseMapper.class);
        Lease current = leaseMapper.currentWithShareLock();
        if (current.getEpoch() != this.lease.getEpoch()) {
            // Current node is not leader any longer, forward to the new leader in the next iteration.
            this.lease = current;
            return false;
        }
        return true;
    }

    @Override
    public long createTopic(String topicName, int queueNum) throws ControllerException {
        for (; ; ) {
            if (this.isLeader()) {
                Set<Integer> toNotify = new HashSet<>();
                try (SqlSession session = getSessionFactory().openSession()) {
                    if (!maintainLeadershipWithSharedLock(session)) {
                        continue;
                    }

                    TopicMapper topicMapper = session.getMapper(TopicMapper.class);
                    if (null != topicMapper.get(null, topicName)) {
                        throw new ControllerException(Code.DUPLICATED_VALUE, String.format("Topic %s was taken", topicName));
                    }

                    Topic topic = new Topic();
                    topic.setName(topicName);
                    topic.setQueueNum(queueNum);
                    topic.setStatus(TopicStatus.ACTIVE);
                    topicMapper.create(topic);

                    QueueAssignmentMapper assignmentMapper = session.getMapper(QueueAssignmentMapper.class);

                    long topicId = topic.getId();
                    List<Integer> aliveNodeIds =
                        this.nodes.values()
                            .stream()
                            .filter(brokerNode ->
                                brokerNode.isAlive(TimeUnit.SECONDS.toMillis(config.nodeAliveIntervalInSecs())) ||
                                    brokerNode.getNode().getId() == config.nodeId())
                            .map(node -> node.getNode().getId())
                            .toList();

                    IntStream.range(0, queueNum).forEach(n -> {
                        int idx = n % aliveNodeIds.size();
                        int nodeId = aliveNodeIds.get(idx);
                        toNotify.add(nodeId);
                        QueueAssignment assignment = new QueueAssignment();
                        assignment.setTopicId(topicId);
                        assignment.setStatus(QueueAssignmentStatus.ASSIGNED);
                        assignment.setQueueId(n);
                        // On creation, both src and dst node_id are the same.
                        assignment.setSrcNodeId(nodeId);
                        assignment.setDstNodeId(nodeId);
                        assignmentMapper.create(assignment);
                    });

                    // Commit transaction
                    session.commit();
                    this.notifyOnResourceChange(toNotify);
                    return topicId;
                }
            } else {
                try {
                    return controllerClient.createTopic(this.leaderAddress(), topicName, queueNum).get();
                } catch (InterruptedException e) {
                    throw new ControllerException(Code.INTERNAL_VALUE, e);
                } catch (ExecutionException e) {
                    if (e.getCause() instanceof ControllerException) {
                        throw (ControllerException) e.getCause();
                    } else {
                        throw new ControllerException(Code.INTERNAL_VALUE, e);
                    }
                }
            }
        }
    }

    @Override
    public void deleteTopic(long topicId) throws ControllerException {
        for (; ; ) {
            if (this.isLeader()) {
                Set<Integer> toNotify = new HashSet<>();
                try (SqlSession session = getSessionFactory().openSession()) {
                    if (!maintainLeadershipWithSharedLock(session)) {
                        continue;
                    }

                    TopicMapper topicMapper = session.getMapper(TopicMapper.class);
                    Topic topic = topicMapper.get(topicId, null);
                    if (null == topic) {
                        throw new ControllerException(Code.NOT_FOUND_VALUE, String.format("This is no topic with topic-id %d", topicId));
                    }
                    topicMapper.updateStatusById(topicId, TopicStatus.DELETED);

                    QueueAssignmentMapper assignmentMapper = session.getMapper(QueueAssignmentMapper.class);
                    List<QueueAssignment> assignments = assignmentMapper.list(topicId, null, null, null, null);
                    assignments.stream().filter(assignment -> assignment.getStatus() != QueueAssignmentStatus.DELETED)
                        .forEach(assignment -> {
                            switch (assignment.getStatus()) {
                                case ASSIGNED -> {
                                    toNotify.add(assignment.getDstNodeId());
                                }
                                case YIELDING -> {
                                    toNotify.add(assignment.getSrcNodeId());
                                }
                                default -> {
                                }
                            }
                            assignment.setStatus(QueueAssignmentStatus.DELETED);
                            assignmentMapper.update(assignment);
                        });
                    session.commit();
                }
                this.notifyOnResourceChange(toNotify);
            } else {
                controllerClient.deleteTopic(this.leaderAddress(), topicId);
            }
            break;
        }
    }

    @Override
    public apache.rocketmq.controller.v1.Topic describeTopic(Long topicId,
        String topicName) throws ControllerException {
        if (isLeader()) {
            try (SqlSession session = getSessionFactory().openSession()) {
                TopicMapper topicMapper = session.getMapper(TopicMapper.class);
                Topic topic = topicMapper.get(topicId, topicName);

                if (null == topic) {
                    throw new ControllerException(Code.NOT_FOUND_VALUE,
                        String.format("Topic not found for topic-id=%d, topic-name=%s", topicId, topicName));
                }

                QueueAssignmentMapper assignmentMapper = session.getMapper(QueueAssignmentMapper.class);
                List<QueueAssignment> assignments = assignmentMapper.list(topic.getId(), null, null, null, null);

                apache.rocketmq.controller.v1.Topic.Builder topicBuilder = apache.rocketmq.controller.v1.Topic
                    .newBuilder()
                    .setTopicId(topic.getId())
                    .setCount(topic.getQueueNum())
                    .setName(topic.getName());

                if (null != assignments) {
                    for (QueueAssignment assignment : assignments) {
                        switch (assignment.getStatus()) {
                            case DELETED -> {
                            }

                            case ASSIGNED -> {
                                MessageQueueAssignment queueAssignment = MessageQueueAssignment.newBuilder()
                                    .setQueue(MessageQueue.newBuilder()
                                        .setTopicId(assignment.getTopicId())
                                        .setQueueId(assignment.getQueueId()))
                                    .setBrokerId(assignment.getDstNodeId())
                                    .build();
                                topicBuilder.addAssignments(queueAssignment);
                            }

                            case YIELDING, ASSIGNABLE -> {
                                OngoingMessageQueueReassignment reassignment = OngoingMessageQueueReassignment.newBuilder()
                                    .setQueue(MessageQueue.newBuilder()
                                        .setTopicId(assignment.getTopicId())
                                        .setQueueId(assignment.getQueueId())
                                        .build())
                                    .setSrcBrokerId(assignment.getSrcNodeId())
                                    .setDstBrokerId(assignment.getDstNodeId())
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
                return this.controllerClient.describeTopic(leaderAddress(), topicId, topicName).get();
            } catch (InterruptedException e) {
                throw new ControllerException(Code.INTERRUPTED_VALUE, e);
            } catch (ExecutionException e) {
                if (e.getCause() instanceof ControllerException) {
                    throw (ControllerException) e.getCause();
                } else {
                    throw new ControllerException(Code.INTERNAL_VALUE, e);
                }
            }
        }
    }

    /**
     * Notify nodes that some resources are changed by leader controller.
     *
     * @param nodes Nodes to notify
     */
    private void notifyOnResourceChange(Set<Integer> nodes) {

    }

    @Override
    public boolean isLeader() throws ControllerException {
        return this.role == Role.Leader;
    }

    @Override
    public String leaderAddress() throws ControllerException {
        if (null == lease || lease.expired()) {
            LOGGER.error("No lease is populated yet or lease was expired");
            throw new ControllerException(Code.NO_LEADER_VALUE);
        }

        BrokerNode brokerNode = nodes.get(this.lease.getNodeId());
        if (null == brokerNode) {
            LOGGER.error("Address for Broker with brokerId={} is missing", this.lease.getNodeId());
            throw new ControllerException(Code.NOT_FOUND_VALUE,
                String.format("Broker is unexpected missing with brokerId=%d", this.lease.getNodeId()));
        }

        return brokerNode.getNode().getAddress();
    }

    @Override
    public List<QueueAssignment> listAssignments(Long topicId, Integer srcNodeId, Integer dstNodeId,
        QueueAssignmentStatus status) {
        try (SqlSession session = getSessionFactory().openSession()) {
            QueueAssignmentMapper mapper = session.getMapper(QueueAssignmentMapper.class);
            return mapper.list(topicId, srcNodeId, dstNodeId, status, null);
        }
    }

    @Override
    public void close() throws IOException {
        this.executorService.shutdown();
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

    public void addBroker(Node node) {
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
}
