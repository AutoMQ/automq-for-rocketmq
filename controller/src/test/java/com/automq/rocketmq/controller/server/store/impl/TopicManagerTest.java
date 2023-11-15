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

import apache.rocketmq.controller.v1.AssignmentStatus;
import apache.rocketmq.controller.v1.CreateTopicRequest;
import apache.rocketmq.controller.v1.UpdateTopicRequest;
import com.automq.rocketmq.controller.ControllerClient;
import com.automq.rocketmq.controller.MetadataStore;
import com.automq.rocketmq.controller.server.store.ElectionService;
import com.automq.rocketmq.metadata.dao.QueueAssignment;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

class TopicManagerTest {

    @Test
    public void testCreateTopic() {
        MetadataStore metadataStore = Mockito.mock(MetadataStore.class);
        ControllerClient controllerClient = Mockito.mock(ControllerClient.class);
        Mockito.when(controllerClient.createTopic(ArgumentMatchers.anyString(), ArgumentMatchers.any())).thenReturn(
            CompletableFuture.failedFuture(new CompletionException(new RuntimeException()))
        );
        Mockito.when(metadataStore.controllerClient()).thenReturn(controllerClient);
        Mockito.when(metadataStore.isLeader()).thenReturn(false);

        ElectionService electionService = Mockito.mock(ElectionService.class);
        Mockito.when(metadataStore.electionService()).thenReturn(electionService);

        Mockito.when(electionService.leaderAddress()).thenReturn(Optional.of("localhost:1234"));

        TopicManager topicManager = new TopicManager(metadataStore);
        topicManager.createTopic(CreateTopicRequest.newBuilder().build());
    }

    @Test
    public void testUpdateTopic_RemoteFailure() {
        MetadataStore metadataStore = Mockito.mock(MetadataStore.class);
        ElectionService electionService = Mockito.mock(ElectionService.class);
        Mockito.when(metadataStore.electionService()).thenReturn(electionService);

        Mockito.when(electionService.leaderAddress()).thenReturn(Optional.of("localhost:1234"));
        ControllerClient controllerClient = Mockito.mock(ControllerClient.class);
        Mockito.when(controllerClient.updateTopic(ArgumentMatchers.anyString(), ArgumentMatchers.any())).thenReturn(
            CompletableFuture.failedFuture(new CompletionException(new RuntimeException()))
        );
        Mockito.when(metadataStore.controllerClient()).thenReturn(controllerClient);
        Mockito.when(metadataStore.isLeader()).thenReturn(false);
        TopicManager topicManager = new TopicManager(metadataStore);
        topicManager.updateTopic(UpdateTopicRequest.newBuilder().build());
    }

    @Test
    public void testOwnerNode() {
        MetadataStore metadataStore = Mockito.mock(MetadataStore.class);
        TopicManager topicManager = new TopicManager(metadataStore);
        Optional<Integer> opt = topicManager.ownerNode(1L, 2);
        Assertions.assertTrue(opt.isEmpty());
        QueueAssignment assignment = new QueueAssignment();
        assignment.setDstNodeId(1);
        assignment.setTopicId(2);
        assignment.setQueueId(3);
        assignment.setStatus(AssignmentStatus.ASSIGNMENT_STATUS_ASSIGNED);
        topicManager.assignmentCache.apply(List.of(assignment));
        opt = topicManager.ownerNode(2, 3);
        Assertions.assertTrue(opt.isPresent());
        Assertions.assertEquals(1, opt.get());
    }

}