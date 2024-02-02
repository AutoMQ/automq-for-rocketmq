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

package com.automq.rocketmq.store.service;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class InflightServiceTest {

    @Test
    void inflight() {
        InflightService inflightService = new InflightService();
        inflightService.increaseInflightCount(0, 0, 0, 10);
        assertEquals(10, inflightService.getInflightCount(0, 0, 0));

        inflightService.decreaseInflightCount(0, 0, 0, 5);
        assertEquals(5, inflightService.getInflightCount(0, 0, 0));
    }
}