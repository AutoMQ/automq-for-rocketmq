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

package com.automq.rocketmq.controller;

import apache.rocketmq.controller.v1.AssignmentStatus;
import apache.rocketmq.controller.v1.Cluster;
import apache.rocketmq.controller.v1.ConsumerGroup;
import apache.rocketmq.controller.v1.CreateGroupRequest;
import apache.rocketmq.controller.v1.CreateTopicRequest;
import apache.rocketmq.controller.v1.DescribeClusterRequest;
import apache.rocketmq.controller.v1.DescribeStreamReply;
import apache.rocketmq.controller.v1.DescribeStreamRequest;
import apache.rocketmq.controller.v1.StreamMetadata;
import apache.rocketmq.controller.v1.StreamRole;
import apache.rocketmq.controller.v1.TerminationStage;
import apache.rocketmq.controller.v1.Topic;
import apache.rocketmq.controller.v1.UpdateGroupRequest;
import apache.rocketmq.controller.v1.UpdateTopicRequest;
import com.automq.rocketmq.common.api.DataStore;
import com.automq.rocketmq.common.config.ControllerConfig;
import com.automq.rocketmq.common.exception.ControllerException;
import com.automq.rocketmq.controller.server.store.BrokerNode;
import com.automq.rocketmq.controller.server.store.ElectionService;
import com.automq.rocketmq.controller.server.store.impl.TopicManager;
import com.automq.rocketmq.metadata.dao.Group;
import com.automq.rocketmq.metadata.dao.Node;
import com.automq.rocketmq.metadata.dao.QueueAssignment;
import com.automq.rocketmq.metadata.dao.Stream;
import java.io.Closeable;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;

public interface MetadataStore extends Closeable {

    ControllerConfig config();

    ExecutorService asyncExecutor();

    /**
     * Open a JDBC connection with auto-commit disabled.
     * </p>
     * <p>
     * Remember to commit session manually if updates are involved.
     *
     * @return SqlSession instance
     */
    SqlSession openSession();

    SqlSessionFactory sessionFactory();

    ControllerClient controllerClient();

    void addBrokerNode(Node node);


    DataStore getDataStore();

    void setDataStore(DataStore dataStore);

    ElectionService electionService();

    List<QueueAssignment> assignmentsOf(int nodeId);

    List<Long> streamsOf(long topicId, int queueId);

    void start();

    CompletableFuture<Cluster> describeCluster(DescribeClusterRequest request);

    /**
     * Register broker into metadata store and return broker epoch
     *
     * @return broker epoch
     */
    CompletableFuture<Node> registerBrokerNode(String name, String address, String instanceId);

    /**
     * Register broker into metadata store and return broker epoch
     *
     * @throws ControllerException If there is an I/O error.
     */
    void registerCurrentNode(String name, String address, String instanceId) throws ControllerException;

    /**
     * If the node is a leader, it should keep the sending node alive once it receives heartbeat requests
     * from it.
     *
     * @param nodeId    Heartbeat sender node-id
     * @param epoch     Epoch of the node
     * @param goingAway Flag if the node is going away shortly
     */
    void keepAlive(int nodeId, long epoch, boolean goingAway);

    /**
     * Send heartbeat request to leader to keep current node alive.
     */
    void heartbeat();

    CompletableFuture<Long> createTopic(CreateTopicRequest request);

    CompletableFuture<Void> deleteTopic(long topicId);

    CompletableFuture<Topic> describeTopic(Long topicId, String topicName);

    CompletableFuture<List<Topic>> listTopics();

    CompletableFuture<Topic> updateTopic(UpdateTopicRequest request);

    /**
     * Check if current controller is playing leader role
     *
     * @return true if leader; false otherwise
     */
    boolean isLeader();

    boolean hasAliveBrokerNodes();

    Optional<String> leaderAddress();

    /**
     * List queue assignments according to criteria.
     *
     * @param topicId   Optional topic-id
     * @param srcNodeId Optional source node-id
     * @param dstNodeId Optional destination node-id
     * @param status    Optional queue assignment status
     * @return List of the queue assignments meeting the specified criteria
     */
    CompletableFuture<List<QueueAssignment>> listAssignments(Long topicId, Integer srcNodeId, Integer dstNodeId,
        AssignmentStatus status);

    CompletableFuture<Void> reassignMessageQueue(long topicId, int queueId, int dstNodeId);

    CompletableFuture<Void> markMessageQueueAssignable(long topicId, int queueId);

    CompletableFuture<Void> commitOffset(long groupId, long topicId, int queueId, long offset);

    CompletableFuture<Long> createGroup(CreateGroupRequest request);

    CompletableFuture<ConsumerGroup> describeGroup(Long groupId, String groupName);

    CompletableFuture<Void> updateGroup(UpdateGroupRequest request);

    /**
     * Delete group with the given group id logically.
     *
     * @param groupId Given group ID
     * @return Deleted consumer group
     */
    CompletableFuture<ConsumerGroup> deleteGroup(long groupId);

    CompletableFuture<Collection<ConsumerGroup>> listGroups();

    CompletableFuture<StreamMetadata> getStream(long topicId, int queueId, Long groupId, StreamRole streamRole);

    /**
     * Invoked when store has closed the queue.
     *
     * @param topicId Topic ID
     * @param queueId Queue ID
     */
    CompletableFuture<Void> onQueueClosed(long topicId, int queueId);

    CompletableFuture<StreamMetadata> openStream(long streamId, long streamEpoch, int nodeId);

    CompletableFuture<Void> closeStream(long streamId, long streamEpoch, int nodeId);

    CompletableFuture<List<StreamMetadata>> listOpenStreams(int nodeId);


    CompletableFuture<Long> getConsumerOffset(long consumerGroupId, long topicId, int queueId);

    CompletableFuture<String> addressOfNode(int nodeId);


    boolean maintainLeadershipWithSharedLock(SqlSession session);

    void applyTopicChange(List<com.automq.rocketmq.metadata.dao.Topic> topics);

    void applyAssignmentChange(List<QueueAssignment> assignments);

    void applyGroupChange(List<Group> groups);

    void applyStreamChange(List<Stream> streams);

    ConcurrentMap<Integer, BrokerNode> allNodes();

    /**
     * Query owner node id of the given topic/queue.
     *
     * @param topicId Topic ID
     * @param queueId Queue ID
     * @return Owner node id if found in cache; 0 otherwise.
     */
    Optional<Integer> ownerNode(long topicId, int queueId);

    CompletableFuture<List<StreamMetadata>> getStreams(List<Long> streamIds);

    CompletableFuture<DescribeStreamReply> describeStream(DescribeStreamRequest request);

    TerminationStage fireClose();

    TopicManager topicManager();

    Optional<BrokerNode> getNode(int nodeId);
}
