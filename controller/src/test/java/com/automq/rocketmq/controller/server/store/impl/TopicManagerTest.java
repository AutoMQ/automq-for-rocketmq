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