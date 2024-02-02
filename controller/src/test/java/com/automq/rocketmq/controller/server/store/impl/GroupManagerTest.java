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

import apache.rocketmq.controller.v1.ConsumerGroup;
import apache.rocketmq.controller.v1.UpdateGroupRequest;
import com.automq.rocketmq.controller.ControllerClient;
import com.automq.rocketmq.controller.MetadataStore;
import com.automq.rocketmq.controller.server.store.DefaultMetadataStore;
import com.automq.rocketmq.controller.server.store.ElectionService;
import com.automq.rocketmq.controller.store.DatabaseTestBase;
import java.io.IOException;
import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import static org.junit.jupiter.api.Assertions.assertFalse;

class GroupManagerTest extends DatabaseTestBase {

    @Test
    public void testListGroups() throws IOException {
        createGroup("G1");
        try (MetadataStore store = new DefaultMetadataStore(getControllerClient(), getSessionFactory(), config)) {
            store.start();
            awaitElectedAsLeader(store);

            GroupManager streamManager = new GroupManager(store);
            Collection<ConsumerGroup> groups = streamManager.listGroups().join();
            assertFalse(groups.isEmpty());
        }
    }

    @Test
    public void testUpdateGroup_RemoteNoLeader() throws IOException {
        ControllerClient controllerClient = getControllerClient();

        ElectionService electionService = Mockito.mock(ElectionService.class);
        Mockito.when(electionService.leaderAddress()).thenReturn(Optional.empty());

        try (MetadataStore store = new DefaultMetadataStore(controllerClient, getSessionFactory(), config)) {

            GroupManager streamManager = new GroupManager(store);
            Assertions.assertThrows(CompletionException.class, () -> {
                streamManager.updateGroup(UpdateGroupRequest.newBuilder().build()).join();
            });
        }
    }

    @Test
    public void testUpdateGroup_Remote() throws IOException {
        ControllerClient controllerClient = getControllerClient();

        ElectionService electionService = Mockito.mock(ElectionService.class);
        Mockito.when(electionService.leaderAddress()).thenReturn(Optional.of("localhost:2345"));

        Mockito.when(controllerClient.updateGroup(ArgumentMatchers.anyString(), ArgumentMatchers.any()))
            .thenReturn(CompletableFuture.completedFuture(null));

        try (DefaultMetadataStore store = new DefaultMetadataStore(controllerClient, getSessionFactory(), config)) {
            store.setElectionService(electionService);
            GroupManager streamManager = new GroupManager(store);
            Assertions.assertDoesNotThrow(() -> {
                streamManager.updateGroup(UpdateGroupRequest.newBuilder().build()).join();
            });
        }
    }
}