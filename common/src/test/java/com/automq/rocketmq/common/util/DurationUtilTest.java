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