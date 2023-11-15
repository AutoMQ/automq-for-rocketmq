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

package com.automq.rocketmq.controller;

import apache.rocketmq.controller.v1.CloseStreamReply;
import apache.rocketmq.controller.v1.CloseStreamRequest;
import apache.rocketmq.controller.v1.Cluster;
import apache.rocketmq.controller.v1.ConsumerGroup;
import apache.rocketmq.controller.v1.CreateGroupReply;
import apache.rocketmq.controller.v1.CreateGroupRequest;
import apache.rocketmq.controller.v1.DescribeClusterRequest;
import apache.rocketmq.controller.v1.DescribeStreamReply;
import apache.rocketmq.controller.v1.DescribeStreamRequest;
import apache.rocketmq.controller.v1.ListGroupReply;
import apache.rocketmq.controller.v1.ListGroupRequest;
import apache.rocketmq.controller.v1.ListOpenStreamsReply;
import apache.rocketmq.controller.v1.ListOpenStreamsRequest;
import apache.rocketmq.controller.v1.ListTopicsReply;
import apache.rocketmq.controller.v1.ListTopicsRequest;
import apache.rocketmq.controller.v1.OpenStreamReply;
import apache.rocketmq.controller.v1.OpenStreamRequest;
import apache.rocketmq.controller.v1.TerminateNodeReply;
import apache.rocketmq.controller.v1.TerminateNodeRequest;
import apache.rocketmq.controller.v1.Topic;
import apache.rocketmq.controller.v1.CreateTopicRequest;
import apache.rocketmq.controller.v1.UpdateGroupRequest;
import apache.rocketmq.controller.v1.UpdateTopicRequest;
import com.automq.rocketmq.metadata.dao.Node;

import io.grpc.stub.StreamObserver;
import java.io.Closeable;
import java.util.concurrent.CompletableFuture;

public interface ControllerClient extends Closeable {

    CompletableFuture<Cluster> describeCluster(String target, DescribeClusterRequest request);

    CompletableFuture<Node> registerBroker(String target, String name, String address, String instanceId);

    CompletableFuture<Long> createTopic(String target, CreateTopicRequest request);

    CompletableFuture<Void> deleteTopic(String target, long topicId);

    CompletableFuture<Topic> describeTopic(String target, Long topicId, String topicName);

    void listTopics(String target, ListTopicsRequest request, StreamObserver<ListTopicsReply> observer);

    CompletableFuture<Void> heartbeat(String target, int nodeId, long epoch, boolean goingAway);

    CompletableFuture<Void> reassignMessageQueue(String target, long topicId, int queueId, int dstNodeId);

    CompletableFuture<Void> notifyQueueClose(String target, long topicId, int queueId);

    CompletableFuture<CreateGroupReply> createGroup(String target, CreateGroupRequest request);

    CompletableFuture<ConsumerGroup> describeGroup(String target, String groupName);

    CompletableFuture<Void> updateGroup(String target, UpdateGroupRequest request);

    CompletableFuture<Void> deleteGroup(String target, long groupId);

    void listGroups(String target, ListGroupRequest request, StreamObserver<ListGroupReply> observer);

    CompletableFuture<Void> commitOffset(String target, long groupId, long topicId, int queueId, long offset);

    CompletableFuture<OpenStreamReply> openStream(String target, OpenStreamRequest request);

    CompletableFuture<CloseStreamReply> closeStream(String target, CloseStreamRequest request);

    CompletableFuture<ListOpenStreamsReply> listOpenStreams(String target, ListOpenStreamsRequest request);

    CompletableFuture<DescribeStreamReply> describeStream(String target, DescribeStreamRequest request);

    CompletableFuture<Topic> updateTopic(String target, UpdateTopicRequest request);

    void terminateNode(String target, TerminateNodeRequest request, StreamObserver<TerminateNodeReply> observer);
}
