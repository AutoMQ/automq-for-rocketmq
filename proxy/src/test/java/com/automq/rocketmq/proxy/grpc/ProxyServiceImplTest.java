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

package com.automq.rocketmq.proxy.grpc;

import apache.rocketmq.common.v1.Code;
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
}