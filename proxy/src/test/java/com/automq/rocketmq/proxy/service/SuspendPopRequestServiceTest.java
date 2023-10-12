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

import com.automq.rocketmq.common.model.FlatMessageExt;
import com.automq.rocketmq.proxy.mock.MockMessageUtil;
import com.automq.rocketmq.store.model.message.TagFilter;
import com.google.common.base.Supplier;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.client.consumer.PopResult;
import org.apache.rocketmq.client.consumer.PopStatus;
import org.apache.rocketmq.remoting.protocol.header.PopMessageRequestHeader;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SuspendPopRequestServiceTest {
    private SuspendPopRequestService suspendPopRequestService;

    @BeforeEach
    public void setUp() {
        suspendPopRequestService = new SuspendPopRequestService();
    }

    @Test
    void suspendAndNotify() {
        PopMessageRequestHeader header = new PopMessageRequestHeader();
        header.setBornTime(System.currentTimeMillis());
        header.setTopic("topic");
        Supplier<CompletableFuture<List<FlatMessageExt>>> supplier = () -> CompletableFuture.completedFuture(List.of(MockMessageUtil.buildMessage(0, 0, "tagA")));

        // Try to suspend request with zero polling time.
        CompletableFuture<PopResult> future = suspendPopRequestService.suspendPopRequest(header, 0, 0,
            new TagFilter("tagA"), supplier);
        assertEquals(0, suspendPopRequestService.suspendRequestCount());
        assertTrue(future.isDone());
        PopResult popResult = future.getNow(null);
        assertNotNull(popResult);
        assertEquals(PopStatus.NO_NEW_MSG, popResult.getPopStatus());

        // Try to suspend request with non-zero polling time.
        header.setBornTime(System.currentTimeMillis());
        header.setPollTime(100_000);
        future = suspendPopRequestService.suspendPopRequest(header, 0, 0,
            new TagFilter("tagA"), supplier);
        assertEquals(1, suspendPopRequestService.suspendRequestCount());
        assertFalse(future.isDone());

        // Notify message that do not need arrival.
        suspendPopRequestService.notifyMessageArrival(0, 0, "tagB");
        assertEquals(1, suspendPopRequestService.suspendRequestCount());
        assertFalse(future.isDone());

        // Notify message arrival.
        suspendPopRequestService.notifyMessageArrival(0, 0, "tagA");

        // Wait for pop request to be processed.
        CompletableFuture<PopResult> finalFuture = future;
        await().atMost(1, TimeUnit.SECONDS).until(finalFuture::isDone);
        assertEquals(0, suspendPopRequestService.suspendRequestCount());

        popResult = future.getNow(null);
        assertNotNull(popResult);
        assertEquals(PopStatus.FOUND, popResult.getPopStatus());
        assertEquals(1, popResult.getMsgFoundList().size());
    }

    @Test
    void cleanExpired() {
        PopMessageRequestHeader header = new PopMessageRequestHeader();
        header.setBornTime(System.currentTimeMillis());
        header.setPollTime(100);
        header.setTopic("topic");

        CompletableFuture<PopResult> future = suspendPopRequestService.suspendPopRequest(header, 0, 0,
            new TagFilter("tagA"), () -> CompletableFuture.completedFuture(Collections.emptyList()));
        assertEquals(1, suspendPopRequestService.suspendRequestCount());
        assertFalse(future.isDone());

        // Notify message arrival but message supplier do not produce any messages.
        suspendPopRequestService.notifyMessageArrival(0, 0, "tagA");
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