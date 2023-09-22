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

package com.automq.rocketmq.store.service.impl;

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