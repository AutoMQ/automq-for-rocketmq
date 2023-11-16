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

package com.automq.rocketmq.proxy.grpc.activity;

import apache.rocketmq.v2.Broker;
import apache.rocketmq.v2.Code;
import apache.rocketmq.v2.MessageQueue;
import apache.rocketmq.v2.QueryAssignmentRequest;
import apache.rocketmq.v2.QueryAssignmentResponse;
import apache.rocketmq.v2.QueryRouteRequest;
import apache.rocketmq.v2.QueryRouteResponse;
import apache.rocketmq.v2.Resource;
import com.automq.rocketmq.proxy.model.ProxyContextExt;
import com.google.common.net.HostAndPort;
import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.apache.rocketmq.common.attribute.TopicMessageType;
import org.apache.rocketmq.proxy.common.Address;
import org.apache.rocketmq.proxy.config.Configuration;
import org.apache.rocketmq.proxy.config.ConfigurationManager;
import org.apache.rocketmq.proxy.grpc.v2.channel.GrpcChannelManager;
import org.apache.rocketmq.proxy.grpc.v2.common.GrpcClientSettingsManager;
import org.apache.rocketmq.proxy.processor.MessagingProcessor;
import org.apache.rocketmq.proxy.service.metadata.MetadataService;
import org.apache.rocketmq.proxy.service.route.ProxyTopicRouteData;
import org.apache.rocketmq.remoting.protocol.route.QueueData;
import org.apache.rocketmq.remoting.protocol.subscription.SubscriptionGroupConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;

class ExtendRouteActivityTest {
    private ExtendRouteActivity routeActivity;

    @BeforeEach
    void setUp() throws Exception {
        Field field = ConfigurationManager.class.getDeclaredField("configuration");
        field.setAccessible(true);
        Configuration configuration = new Configuration();
        configuration.setProxyConfig(new org.apache.rocketmq.proxy.config.ProxyConfig());
        field.set(null, configuration);

        MessagingProcessor messagingProcessor = Mockito.mock(MessagingProcessor.class);

        ProxyTopicRouteData topicRouteData = new ProxyTopicRouteData();
        ProxyTopicRouteData.ProxyBrokerData brokerData = new ProxyTopicRouteData.ProxyBrokerData();
        brokerData.setCluster("cluster");
        brokerData.setBrokerName("1_2");
        brokerData.setBrokerAddrs(Map.of(0L, List.of(new Address(Address.AddressScheme.IPv4, HostAndPort.fromString("broker:10911")))));
        topicRouteData.setBrokerDatas(List.of(brokerData));

        QueueData queueData = new QueueData();
        queueData.setBrokerName("1_2");
        queueData.setPerm(6);
        queueData.setReadQueueNums(1);
        queueData.setWriteQueueNums(1);
        topicRouteData.setQueueDatas(List.of(queueData));
        Mockito.doReturn(topicRouteData).when(messagingProcessor).getTopicRouteDataForProxy(any(), any(), any());

        SubscriptionGroupConfig groupConfig = new SubscriptionGroupConfig();
        groupConfig.setGroupName("group");

        Mockito.doReturn(groupConfig).when(messagingProcessor).getSubscriptionGroupConfig(any(), any());

        MetadataService metadataService = Mockito.mock(MetadataService.class);
        Mockito.doReturn(metadataService).when(messagingProcessor).getMetadataService();
        Mockito.doReturn(TopicMessageType.NORMAL).when(metadataService).getTopicMessageType(any(), any());

        routeActivity = new ExtendRouteActivity(
            messagingProcessor,
            Mockito.mock(GrpcClientSettingsManager.class),
            Mockito.mock(GrpcChannelManager.class)
        );
    }

    @Test
    void queryRoute() {
        QueryRouteRequest request = QueryRouteRequest.newBuilder()
            .setTopic(Resource.newBuilder().setName("topic").build())
            .build();

        CompletableFuture<QueryRouteResponse> future = routeActivity.queryRoute(ProxyContextExt.create(), request);
        assertNotNull(future);
        assertTrue(future.isDone());
        assertFalse(future.isCompletedExceptionally());

        QueryRouteResponse response = future.join();
        assertEquals(Code.OK, response.getStatus().getCode());
        assertEquals(1, response.getMessageQueuesList().size());

        MessageQueue queue = response.getMessageQueuesList().get(0);
        assertEquals(2, queue.getId());

        Broker broker = queue.getBroker();
        assertEquals("1_2", broker.getName());
        assertEquals(1, broker.getEndpoints().getAddressesCount());

        apache.rocketmq.v2.Address addresses = broker.getEndpoints().getAddresses(0);
        assertEquals("broker", addresses.getHost());
        assertEquals(10911, addresses.getPort());
    }

    @Test
    void queryAssignment() {
        QueryAssignmentRequest request = QueryAssignmentRequest.newBuilder()
            .setTopic(Resource.newBuilder().setName("topic").build())
            .setGroup(Resource.newBuilder().setName("group").build())
            .build();

        CompletableFuture<QueryAssignmentResponse> future = routeActivity.queryAssignment(ProxyContextExt.create(), request);
        assertNotNull(future);
        assertTrue(future.isDone());
        assertFalse(future.isCompletedExceptionally());

        QueryAssignmentResponse response = future.join();
        assertEquals(Code.OK, response.getStatus().getCode());
        assertEquals(1, response.getAssignmentsList().size());

        MessageQueue queue = response.getAssignmentsList().get(0).getMessageQueue();
        assertEquals(2, queue.getId());

        Broker broker = queue.getBroker();
        assertEquals("1_2", broker.getName());
        assertEquals(1, broker.getEndpoints().getAddressesCount());

        apache.rocketmq.v2.Address addresses = broker.getEndpoints().getAddresses(0);
        assertEquals("broker", addresses.getHost());
        assertEquals(10911, addresses.getPort());

    }
}