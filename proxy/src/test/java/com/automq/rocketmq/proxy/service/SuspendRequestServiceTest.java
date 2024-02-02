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

import com.automq.rocketmq.proxy.model.ProxyContextExt;
import com.automq.rocketmq.store.model.message.Filter;
import com.automq.rocketmq.store.model.message.TagFilter;
import java.lang.reflect.Field;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.apache.rocketmq.common.filter.ExpressionType;
import org.apache.rocketmq.proxy.config.Configuration;
import org.apache.rocketmq.proxy.config.ConfigurationManager;
import org.apache.rocketmq.proxy.config.ProxyConfig;
import org.apache.rocketmq.remoting.protocol.heartbeat.SubscriptionData;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SuspendRequestServiceTest {
    private SuspendRequestService suspendRequestService;

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
        suspendRequestService = SuspendRequestService.getInstance();
    }

    static class MockSuccessResult implements SuspendRequestService.GetMessageResult {
        @Override
        public boolean needWriteResponse() {
            return true;
        }
    }

    @Test
    void suspendAndNotify() {
        SubscriptionData subscriptionData = new SubscriptionData();
        subscriptionData.setTopic("topic");
        subscriptionData.setExpressionType(ExpressionType.TAG);
        subscriptionData.setSubString("tagA");

        Function<Long, CompletableFuture<MockSuccessResult>> supplier = ignore -> CompletableFuture.completedFuture(new MockSuccessResult());
        CompletableFuture<Optional<MockSuccessResult>> future;
        Optional<MockSuccessResult> result;

        // Try to suspend request with zero polling time.
        future = suspendRequestService.suspendRequest(ProxyContextExt.create(), "topic", 0, Filter.DEFAULT_FILTER, 0, supplier);
        assertEquals(0, suspendRequestService.suspendRequestCount());
        assertTrue(future.isDone());
        result = future.getNow(null);
        assertNotNull(result);
        assertTrue(result.isEmpty());

        // Try to suspend request with non-zero polling time.
        future = suspendRequestService.suspendRequest(ProxyContextExt.create(), "topic", 0, Filter.DEFAULT_FILTER, 100_000, supplier);
        assertEquals(1, suspendRequestService.suspendRequestCount());
        assertFalse(future.isDone());

        // Notify message that do not need arrival.
        suspendRequestService.notifyMessageArrival("topic", 0, "tagB");
        assertEquals(1, suspendRequestService.suspendRequestCount());
        assertFalse(future.isDone());

        // Notify message arrival.
        suspendRequestService.notifyMessageArrival("topic", 0, "tagA");

        // Wait for pop request to be processed.
        CompletableFuture<Optional<MockSuccessResult>> finalFuture = future;
        await().atMost(1, TimeUnit.SECONDS).until(finalFuture::isDone);
        assertEquals(0, suspendRequestService.suspendRequestCount());

        result = future.getNow(null);
        assertNotNull(result);
        assertTrue(result.isPresent());
    }

    @Test
    void cleanExpired() {
        Function<Long, CompletableFuture<MockSuccessResult>> supplier = ignore -> CompletableFuture.completedFuture(new MockSuccessResult());

        CompletableFuture<Optional<MockSuccessResult>> future =
            suspendRequestService.suspendRequest(ProxyContextExt.create(), "topic", 0, new TagFilter("tagA"), 100, supplier);
        assertEquals(1, suspendRequestService.suspendRequestCount());
        assertFalse(future.isDone());

        // Notify message arrival but message supplier do not produce any messages.
        suspendRequestService.notifyMessageArrival("topic", 0, "tagA");
        assertEquals(1, suspendRequestService.suspendRequestCount());
        assertFalse(future.isDone());

        await()
            .atMost(200, TimeUnit.MILLISECONDS)
            .until(() -> {
                suspendRequestService.cleanExpiredRequest();
                return suspendRequestService.suspendRequestCount() == 0;
            });
    }
}