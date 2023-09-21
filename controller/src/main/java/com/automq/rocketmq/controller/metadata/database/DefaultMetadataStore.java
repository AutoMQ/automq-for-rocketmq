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
import java.io.Closeable;
import java.io.IOException;
import java.util.List;
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

public class DefaultMetadataStore implements MetadataStore, Closeable {

    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultMetadataStore.class);

    private final ControllerClient controllerClient;

    private final SqlSessionFactory sessionFactory;

    private final ControllerConfig config;

    private Role role;

    private final ConcurrentHashMap<Integer, BrokerNode> brokers;

    private final ScheduledExecutorService executorService;

    /// The following fields are runtime specific
    private Lease lease;

    public DefaultMetadataStore(ControllerClient client, SqlSessionFactory sessionFactory, ControllerConfig config) {
        controllerClient = client;
        this.sessionFactory = sessionFactory;
        this.config = config;
        this.role = Role.Follower;
        this.brokers = new ConcurrentHashMap<>();
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
    public void createTopic(String topicName, int queueNum) throws ControllerException {
        for (; ; ) {
            if (this.isLeader()) {
                try (SqlSession session = getSessionFactory().openSession()) {
                    LeaseMapper leaseMapper = session.getMapper(LeaseMapper.class);
                    Lease current = leaseMapper.currentWithShareLock();
                    if (current.getEpoch() != this.lease.getEpoch()) {
                        // Current node is not leader any longer, forward to the new leader in the next iteration.
                        this.lease = current;
                        continue;
                    }

                    TopicMapper topicMapper = session.getMapper(TopicMapper.class);
                    if (null != topicMapper.getByName(topicName)) {
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
                        this.brokers.values()
                            .stream()
                            .filter(brokerNode ->
                                brokerNode.isAlive(TimeUnit.SECONDS.toMillis(config.nodeAliveIntervalInSecs())) ||
                                    brokerNode.getNode().getId() == config.nodeId())
                            .map(node -> node.getNode().getId())
                            .toList();

                    IntStream.range(0, queueNum).forEach(n -> {
                        int idx = n % aliveNodeIds.size();
                        int nodeId = aliveNodeIds.get(idx);
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
                }
            } else {
                return;
            }
        }

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

        BrokerNode brokerNode = brokers.get(this.lease.getNodeId());
        if (null == brokerNode) {
            LOGGER.error("Address for Broker with brokerId={} is missing", this.lease.getNodeId());
            throw new ControllerException(Code.NOT_FOUND_VALUE,
                String.format("Broker is unexpected missing with brokerId=%d", this.lease.getNodeId()));
        }

        return brokerNode.getNode().getAddress();
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
        this.brokers.put(node.getId(), new BrokerNode(node));
    }

    public ConcurrentMap<Integer, BrokerNode> getBrokers() {
        return brokers;
    }

    public SqlSessionFactory getSessionFactory() {
        return sessionFactory;
    }

    public ControllerConfig getConfig() {
        return config;
    }
}
