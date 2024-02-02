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

package com.automq.rocketmq.proxy.service;

import apache.rocketmq.controller.v1.AcceptTypes;
import apache.rocketmq.controller.v1.MessageQueue;
import apache.rocketmq.controller.v1.MessageQueueAssignment;
import apache.rocketmq.controller.v1.MessageType;
import apache.rocketmq.controller.v1.OngoingMessageQueueReassignment;
import apache.rocketmq.controller.v1.Topic;
import com.automq.rocketmq.common.config.BrokerConfig;
import com.automq.rocketmq.common.config.ProxyConfig;
import com.automq.rocketmq.metadata.api.ProxyMetadataService;
import com.automq.rocketmq.proxy.config.ProxyConfiguration;
import com.automq.rocketmq.proxy.model.VirtualQueue;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.apache.rocketmq.common.constant.PermName;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.service.route.ProxyTopicRouteData;
import org.apache.rocketmq.remoting.protocol.route.QueueData;
import org.apache.rocketmq.remoting.protocol.route.TopicRouteData;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class TopicRouteServiceImplTest {
    @Mock
    BrokerConfig brokerConfig;
    @Mock
    ProxyMetadataService proxyMetadataService;
    @Mock
    ProxyContext ctx;

    TopicRouteServiceImpl topicRouteService;

    @BeforeEach
    void setUp() {
        when(brokerConfig.proxy()).thenReturn(new ProxyConfig());
        ProxyConfiguration.intConfig(brokerConfig.proxy());
        topicRouteService = new TopicRouteServiceImpl(brokerConfig, proxyMetadataService);
    }

    @Test
    void testGetAllMessageQueueView() {
        String topicA = "topicA";
        // Build a topic with 3 queues.
        Topic.Builder tb = topicBuilder(topicA, 1, 3);
        final int queue0 = 0, queue1 = 1, queue2 = 2;
        final int node0 = 0, node1 = 1;
        final String node0Addr = "127.0.0.1:8080", node1Addr = "127.0.0.1:8081";

        tb.addAssignments(buildAssignmentWith(1, queue0, node0));
        tb.addAssignments(buildAssignmentWith(1, queue1, node0));
        tb.addReassignments(buildReassignmentWith(1, queue2, node0, node1));

        when(proxyMetadataService.topicOf(topicA)).thenReturn(CompletableFuture.completedFuture(tb.build()));
        when(proxyMetadataService.addressOf(node0)).thenReturn(CompletableFuture.completedFuture(node0Addr));
        when(proxyMetadataService.addressOf(node1)).thenReturn(CompletableFuture.completedFuture(node1Addr));

        TopicRouteData routeData = topicRouteService.getAllMessageQueueView(ctx, topicA).getTopicRouteData();
        assertEquals(routeData.getQueueDatas().size(), 3);
        for (int i = 0; i < routeData.getQueueDatas().size(); i++) {
            QueueData qd = routeData.getQueueDatas().get(i);
            assertEquals(qd.getPerm(), PermName.PERM_READ | PermName.PERM_WRITE);
            assertEquals(qd.getReadQueueNums(), 1);
            assertEquals(qd.getWriteQueueNums(), 1);
            assertEquals(qd.getBrokerName(), new VirtualQueue(1, i).brokerName());
        }

        Map<Integer, String> addressMap = new HashMap<>();
        addressMap.put(queue0, node0Addr);
        addressMap.put(queue1, node0Addr);
        addressMap.put(queue2, node1Addr);

        assertEquals(routeData.getBrokerDatas().size(), 3);
        routeData.getBrokerDatas().forEach(bd -> {
            VirtualQueue vq = new VirtualQueue(bd.getBrokerName());
            assertEquals(bd.getBrokerAddrs().size(), 1);
            assertEquals(bd.getBrokerAddrs().get(0L), addressMap.get(vq.physicalQueueId()));
        });
    }

    @Disabled
    @Test
    void testGetCurrentMessageQueueView() {
        String topicA = "topicA";
        // Build a topic with 3 queues.
        Topic.Builder tb = topicBuilder(topicA, 1, 3);
        final int queue0 = 0, queue1 = 1, queue2 = 2, queue3 = 3;
        final int node0 = 0, node1 = 1;
        String node0Addr = "127.0.0.1:8080";

        tb.addAssignments(buildAssignmentWith(1, queue0, node0));
        tb.addAssignments(buildAssignmentWith(1, queue1, node1));
        tb.addReassignments(buildReassignmentWith(1, queue2, node0, node1));
        tb.addReassignments(buildReassignmentWith(1, queue3, node1, node0));


        when(proxyMetadataService.topicOf(topicA)).thenReturn(CompletableFuture.completedFuture(tb.build()));
        when(proxyMetadataService.addressOf(node0)).thenReturn(CompletableFuture.completedFuture(node0Addr));
        when(brokerConfig.nodeId()).thenReturn(node0);

        TopicRouteData routeData = topicRouteService.getCurrentMessageQueueView(ctx, topicA).getTopicRouteData();
        assertEquals(routeData.getQueueDatas().size(), 2);
        Set<Integer> queueIds = Set.of(queue0, queue3);
        routeData.getBrokerDatas().forEach(bd -> {
            VirtualQueue vq = new VirtualQueue(bd.getBrokerName());
            assertTrue(queueIds.contains(vq.physicalQueueId()));
        });
    }

    @Test
    void testGetTopicRouteForProxy() {
        String topicA = "topicA", node0Addr = "127.0.0.1:8080";

        // Build a topic with 3 queues.
        Topic.Builder tb = topicBuilder(topicA, 1, 1);
        final int queue0 = 0, queue1 = 1, node0 = 0;
        tb.addAssignments(buildAssignmentWith(1, queue0, node0));
        tb.addAssignments(buildAssignmentWith(1, queue1, node0));

        when(proxyMetadataService.topicOf(topicA)).thenReturn(CompletableFuture.completedFuture(tb.build()));
        when(proxyMetadataService.addressOf(node0)).thenReturn(CompletableFuture.completedFuture(node0Addr));

        ProxyTopicRouteData forProxy = topicRouteService.getTopicRouteForProxy(ctx, Collections.emptyList(), topicA);
        assertEquals(forProxy.getQueueDatas().size(), 2);
        assertEquals(forProxy.getBrokerDatas().size(), 2);
    }

    private  Topic.Builder topicBuilder(String topicName, long topicId, int queueNums) {
        Topic.Builder topicBuilder = Topic.newBuilder();
        topicBuilder.setName(topicName);
        topicBuilder.setTopicId(topicId);
        topicBuilder.setCount(queueNums);
        topicBuilder.setAcceptTypes(AcceptTypes.newBuilder().addTypes(MessageType.NORMAL).build());
        return topicBuilder;
    }

    private MessageQueueAssignment buildAssignmentWith(long topicId, int queueId, int nodeId) {
        MessageQueueAssignment.Builder assignmentBuilder = MessageQueueAssignment.newBuilder();
        MessageQueue.Builder queueBuilder = MessageQueue.newBuilder();
        queueBuilder.setTopicId(topicId);
        queueBuilder.setQueueId(queueId);

        assignmentBuilder.setQueue(queueBuilder);
        assignmentBuilder.setNodeId(nodeId);
        return assignmentBuilder.build();
    }

    private OngoingMessageQueueReassignment buildReassignmentWith(long topicId, int queueId, int srcNodeId, int dstNodeId) {
        OngoingMessageQueueReassignment.Builder reassignmentBuilder = OngoingMessageQueueReassignment.newBuilder();
        MessageQueue.Builder queueBuilder = MessageQueue.newBuilder();
        queueBuilder.setTopicId(topicId);
        queueBuilder.setQueueId(queueId);

        reassignmentBuilder.setQueue(queueBuilder);
        reassignmentBuilder.setSrcNodeId(srcNodeId);
        reassignmentBuilder.setDstNodeId(dstNodeId);
        return reassignmentBuilder.build();
    }
}