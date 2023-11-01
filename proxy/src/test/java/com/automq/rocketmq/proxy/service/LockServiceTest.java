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

import com.automq.rocketmq.common.config.ProxyConfig;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LockServiceTest {

    @Test
    void lock() {
        LockService lockService = new LockService(new ProxyConfig());

        boolean result = lockService.tryLock(0, 0, "client1", false, false);
        assertTrue(result);

        // Check if the lock is reentrant.
        result = lockService.tryLock(0, 0, "client1", false, false);
        assertFalse(result);

        // Check if client can lock another queue.
        result = lockService.tryLock(1, 1, "client1", false, false);
        assertTrue(result);

        // Try to release and lock again.
        lockService.release(0, 0);
        result = lockService.tryLock(0, 0, "client1", false, true);
        assertTrue(result);

        // Check if the lock is reentrant.
        result = lockService.tryLock(0, 0, "client1", false, true);
        assertTrue(result);
    }

    @Test
    void lock_preempt() {
        LockService lockService = new LockService(new ProxyConfig());

        // Set both preempt and reentrant to true.
        assertThrowsExactly(IllegalArgumentException.class, () -> lockService.tryLock(0, 0, "client1", true, true));

        boolean result = lockService.tryLock(0, 0, "client1", true, false);
        assertTrue(result);

        // Other clients can not acquire the lock before the lock is expired.
        lockService.release(0, 0);
        result = lockService.tryLock(0, 0, "client2", true, false);
        assertFalse(result);

        result = lockService.tryLock(0, 0, "client1", true, false);
        assertTrue(result);

        // Can not expire the lock when there is a ongoing pop request.
        lockService.tryExpire(0, 0, 0);
        result = lockService.tryLock(0, 0, "client2", true, false);
        assertFalse(result);

        // Expire the lock immediately, so other clients can acquire the lock.
        lockService.release(0, 0);
        lockService.tryExpire(0, 0, 0);
        result = lockService.tryLock(0, 0, "client2", true, false);
        assertTrue(result);
    }
}