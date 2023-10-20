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

package com.automq.rocketmq.proxy.service;

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
        topicBuilder.addAcceptMessageTypes(MessageType.NORMAL);
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