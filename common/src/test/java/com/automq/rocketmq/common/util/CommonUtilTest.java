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

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CommonUtilTest {

    @Test
    void applyPercentage() {
        assertFalse(CommonUtil.applyPercentage(-1));
        assertFalse(CommonUtil.applyPercentage(0));

        int trueCount = 0;
        int falseCount = 0;

        for (int i = 0; i < 100; i++) {
            if (CommonUtil.applyPercentage(50)) {
                trueCount++;
            } else {
                falseCount++;
            }
        }
        assertTrue(trueCount > 0);
        assertTrue(falseCount > 0);

        assertTrue(CommonUtil.applyPercentage(100));
        assertTrue(CommonUtil.applyPercentage(200));
    }
}