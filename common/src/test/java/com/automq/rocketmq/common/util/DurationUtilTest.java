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

package com.automq.rocketmq.common.util;

import java.time.Duration;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class DurationUtilTest {

    @Test
    void parse() {
        Duration d;
        Duration expected;

        d = DurationUtil.parse("1d");
        expected = Duration.ofDays(1);
        assertEquals(d, expected);

        d = DurationUtil.parse("1d1h1m");
        expected = Duration.ZERO
            .plus(Duration.ofDays(1)
                .plus(Duration.ofHours(1))
                .plus(Duration.ofMinutes(1)));
        assertEquals(d, expected);

        d = DurationUtil.parse("1h1m");
        expected = Duration.ZERO
            .plus(Duration.ofHours(1))
            .plus(Duration.ofMinutes(1));
        assertEquals(d, expected);

        d = DurationUtil.parse("1h");
        expected = Duration.ofHours(1);
        assertEquals(d, expected);

        d = DurationUtil.parse("1m");
        expected = Duration.ofMinutes(1);
        assertEquals(d, expected);

        d = DurationUtil.parse("1s");
        expected = Duration.ofSeconds(1);
        assertEquals(d, expected);

        d = DurationUtil.parse("3d0h0m0s");
        expected = Duration.ofDays(3);
        assertEquals(d, expected);
    }
}