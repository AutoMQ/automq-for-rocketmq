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

import apache.rocketmq.v2.Code;
import apache.rocketmq.v2.ReceiveMessageRequest;
import com.automq.rocketmq.proxy.model.ProxyContextExt;
import java.lang.reflect.Field;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.apache.rocketmq.client.consumer.PopResult;
import org.apache.rocketmq.client.consumer.PopStatus;
import org.apache.rocketmq.common.filter.ExpressionType;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.config.Configuration;
import org.apache.rocketmq.proxy.config.ConfigurationManager;
import org.apache.rocketmq.proxy.config.ProxyConfig;
import org.apache.rocketmq.proxy.grpc.v2.consumer.ReceiveMessageResponseStreamWriter;
import org.apache.rocketmq.remoting.protocol.heartbeat.SubscriptionData;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SuspendPopRequestServiceTest {
    private SuspendPopRequestService suspendPopRequestService;

    @BeforeAll
    public static void setUpAll() throws Exception {
        Field field = ConfigurationManager.class.getDeclaredField("configuration");
        field.setAccessible(true);
        Configuration configuration = new Configuration();
        ProxyConfig config = new ProxyConfig();
        config.setGrpcClientConsumerMinLongPollingTimeoutMillis(0);
        configuration.setProxyConfig(config);
        field.set(null, configuration);
    }

    @BeforeEach
    public void setUp() {
        suspendPopRequestService = SuspendPopRequestService.getInstance();
    }

    static class MockWriter extends ReceiveMessageResponseStreamWriter {
        private final CompletableFuture<PopResult> future = new CompletableFuture<>();

        public MockWriter() {
            super(null, null);
        }

        public CompletableFuture<PopResult> future() {
            return future;
        }

        @Override
        public void writeAndComplete(ProxyContext ctx, ReceiveMessageRequest request, PopResult popResult) {
            future.complete(popResult);
        }

        @Override
        public void writeAndComplete(ProxyContext ctx, Code code, String message) {
            future.completeExceptionally(new IllegalStateException(message));
        }

        @Override
        public void writeAndComplete(ProxyContext ctx, ReceiveMessageRequest request, Throwable throwable) {
            future.completeExceptionally(throwable);
        }
    }

    @Test
    void suspendAndNotify() {
        SubscriptionData subscriptionData = new SubscriptionData();
        subscriptionData.setTopic("topic");
        subscriptionData.setExpressionType(ExpressionType.TAG);
        subscriptionData.setSubString("tagA");

        Function<Long, CompletableFuture<PopResult>> supplier = ignore -> CompletableFuture.completedFuture(new PopResult(PopStatus.FOUND, Collections.emptyList()));
        MockWriter writer = new MockWriter();
        CompletableFuture<PopResult> future = writer.future();

        // Try to suspend request with zero polling time.
        suspendPopRequestService.suspendPopRequest(ProxyContextExt.create(),
            null, "topic", 0, subscriptionData, 0, supplier, writer);
        assertEquals(0, suspendPopRequestService.suspendRequestCount());
        assertTrue(future.isDone());
        PopResult popResult = future.getNow(null);
        assertNotNull(popResult);
        assertEquals(PopStatus.POLLING_NOT_FOUND, popResult.getPopStatus());

        // Try to suspend request with non-zero polling time.
        writer = new MockWriter();
        future = writer.future();
        suspendPopRequestService.suspendPopRequest(ProxyContextExt.create(),
            null, "topic", 0, subscriptionData, 100_000, supplier, writer);
        assertEquals(1, suspendPopRequestService.suspendRequestCount());
        assertFalse(future.isDone());

        // Notify message that do not need arrival.
        suspendPopRequestService.notifyMessageArrival("topic", 0, "tagB");
        assertEquals(1, suspendPopRequestService.suspendRequestCount());
        assertFalse(future.isDone());

        // Notify message arrival.
        suspendPopRequestService.notifyMessageArrival("topic", 0, "tagA");

        // Wait for pop request to be processed.
        CompletableFuture<PopResult> finalFuture = future;
        await().atMost(1, TimeUnit.SECONDS).until(finalFuture::isDone);
        assertEquals(0, suspendPopRequestService.suspendRequestCount());

        popResult = future.getNow(null);
        assertNotNull(popResult);
        assertEquals(PopStatus.FOUND, popResult.getPopStatus());
    }

    @Test
    void cleanExpired() {
        SubscriptionData subscriptionData = new SubscriptionData();
        subscriptionData.setTopic("topic");
        subscriptionData.setExpressionType(ExpressionType.TAG);
        subscriptionData.setSubString("tagA");

        Function<Long, CompletableFuture<PopResult>> supplier = ignore -> CompletableFuture.completedFuture(new PopResult(PopStatus.NO_NEW_MSG, Collections.emptyList()));
        MockWriter writer = new MockWriter();

        suspendPopRequestService.suspendPopRequest(ProxyContextExt.create(),
            null, "topic", 0, subscriptionData, 100, supplier, writer);
        assertEquals(1, suspendPopRequestService.suspendRequestCount());
        CompletableFuture<PopResult> future = writer.future();
        assertFalse(future.isDone());

        // Notify message arrival but message supplier do not produce any messages.
        suspendPopRequestService.notifyMessageArrival("topic", 0, "tagA");
        assertEquals(1, suspendPopRequestService.suspendRequestCount());
        assertFalse(future.isDone());

        await()
            .atMost(200, TimeUnit.MILLISECONDS)
            .until(() -> {
                suspendPopRequestService.cleanExpiredRequest();
                return suspendPopRequestService.suspendRequestCount() == 0;
            });
    }
}