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

package com.automq.rocketmq.controller.metadata;

import apache.rocketmq.controller.v1.CloseStreamReply;
import apache.rocketmq.controller.v1.CloseStreamRequest;
import apache.rocketmq.controller.v1.CreateGroupReply;
import apache.rocketmq.controller.v1.CreateGroupRequest;
import apache.rocketmq.controller.v1.OpenStreamReply;
import apache.rocketmq.controller.v1.OpenStreamRequest;
import apache.rocketmq.controller.v1.Topic;
import apache.rocketmq.controller.v1.TrimStreamReply;
import apache.rocketmq.controller.v1.TrimStreamRequest;
import com.automq.rocketmq.controller.exception.ControllerException;
import com.automq.rocketmq.controller.metadata.database.dao.Node;

import java.util.concurrent.CompletableFuture;

public interface ControllerClient {

    CompletableFuture<Node> registerBroker(String target, String name, String address, String instanceId)
        throws ControllerException;

    CompletableFuture<Long> createTopic(String target, String topicName, int queueNum) throws ControllerException;

    CompletableFuture<Void> deleteTopic(String target, long topicId) throws ControllerException;

    CompletableFuture<Topic> describeTopic(String target, Long topicId, String topicName) throws ControllerException;

    CompletableFuture<Void> heartbeat(String target, int nodeId, long epoch,
        boolean goingAway) throws ControllerException;

    CompletableFuture<Void> reassignMessageQueue(String target, long topicId, int queueId, int dstNodeId)
        throws ControllerException;

    CompletableFuture<Void> notifyMessageQueueAssignable(String target, long topicId,
        int queueId) throws ControllerException;

    CompletableFuture<CreateGroupReply> createGroup(String target, CreateGroupRequest request)
        throws ControllerException;

    CompletableFuture<Long> createRetryStream(String target, String groupName, long topicId, int queueId)
        throws ControllerException;

    CompletableFuture<Void> commitOffset(String target, long groupId, long topicId, int queueId,
        long offset) throws ControllerException;

    CompletableFuture<OpenStreamReply> openStream(String target, OpenStreamRequest request) throws ControllerException;

    CompletableFuture<CloseStreamReply> closeStream(String target, CloseStreamRequest request) throws ControllerException;

    CompletableFuture<TrimStreamReply> trimStream(String target, TrimStreamRequest request) throws ControllerException;
}
