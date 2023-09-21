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