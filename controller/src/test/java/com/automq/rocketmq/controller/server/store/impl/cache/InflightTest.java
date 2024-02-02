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

package com.automq.rocketmq.controller.server.store.impl.cache;

import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class InflightTest {

    @Test
    public void testCore() {
        Inflight<Integer> inflight = new Inflight<>();
        CompletableFuture<Integer> future = new CompletableFuture<>();
        inflight.addFuture(future);
        inflight.completeExceptionally(new RuntimeException());
        Assertions.assertTrue(future.isCompletedExceptionally());
    }

}