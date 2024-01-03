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

package com.automq.rocketmq.proxy.grpc;

import apache.rocketmq.common.v1.Code;
import apache.rocketmq.proxy.v1.ConsumerClientConnection;
import apache.rocketmq.proxy.v1.ConsumerClientConnectionRequest;
import apache.rocketmq.proxy.v1.ProxyServiceGrpc;
import apache.rocketmq.proxy.v1.Status;
import com.automq.rocketmq.common.config.BrokerConfig;
import com.automq.rocketmq.common.model.FlatMessageExt;
import com.automq.rocketmq.proxy.grpc.client.GrpcProxyClient;
import com.automq.rocketmq.proxy.mock.MockMessageUtil;
import com.automq.rocketmq.proxy.service.ExtendMessageService;
import com.automq.rocketmq.store.api.MessageStore;
import com.automq.rocketmq.store.model.message.PutResult;
import java.lang.reflect.Field;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentMap;
import org.apache.rocketmq.broker.client.ConsumerManager;
import org.apache.rocketmq.broker.client.ProducerManager;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class ProxyServiceImplTest {
    private static final String TARGET = "target";

    @RegisterExtension
    public final GrpcServerRule grpcServerRule = new GrpcServerRule().directExecutor();
    GrpcProxyClient proxyClient;

    @BeforeEach
    @SuppressWarnings({"unchecked", "rawtypes"})
    public void setup() throws NoSuchFieldException, IllegalAccessException {
        MessageStore messageStore = mock(MessageStore.class);
        when(messageStore.put(any(), any())).thenReturn(CompletableFuture.completedFuture(new PutResult(PutResult.Status.PUT_OK, 0)));

        ExtendMessageService messageService = mock(ExtendMessageService.class);
        ProducerManager producerManager = mock(ProducerManager.class);
        ConsumerManager consumerManager = mock(ConsumerManager.class);
        ProxyServiceImpl server = new ProxyServiceImpl(messageStore, messageService, producerManager, consumerManager);
        grpcServerRule.getServiceRegistry().addService(server);

        ProxyServiceGrpc.ProxyServiceFutureStub stub = ProxyServiceGrpc.newFutureStub(grpcServerRule.getChannel());
        proxyClient = new GrpcProxyClient(new BrokerConfig());

        Field field = proxyClient.getClass().getDeclaredField("stubMap");
        field.setAccessible(true);
        ConcurrentMap<String, ProxyServiceGrpc.ProxyServiceFutureStub> stubMap = (ConcurrentMap) field.get(proxyClient);
        stubMap.put(TARGET, stub);
    }

    @Test
    void relay() {
        FlatMessageExt messageExt = MockMessageUtil.buildMessage(0, 1, "");
        Status status = proxyClient.relayMessage(TARGET, messageExt.message()).join();
        assertEquals(Code.OK, status.getCode());
    }


    @Test
    void ConsumerClientConnection() {
        final String groupName = "";

        ConsumerClientConnectionRequest request = ConsumerClientConnectionRequest.newBuilder().setGroup(groupName).build();
        List<ConsumerClientConnection> list = proxyClient.consumerClientConnection(TARGET, request).join();

    }
}