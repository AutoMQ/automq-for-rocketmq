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

import java.util.Random;

public class CommonUtil {
    private static final Random RANDOM = new Random();

    public static boolean applyPercentage(int percentage) {
        if (percentage <= 0) {
            return false;
        }

        if (percentage >= 100) {
            return true;
        }

        return RANDOM.nextInt(100) <= percentage;
    }
}
