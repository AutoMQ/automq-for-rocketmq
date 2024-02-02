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