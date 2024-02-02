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

package com.automq.rocketmq.metadata.mapper;

import com.automq.rocketmq.metadata.dao.Lease;

public interface LeaseMapper {

    /**
     * Retrieve current lease
     *
     * @return Current lease
     */
    Lease current();


    /**
     * Retrieve current lease with SHARE lock
     *
     * @return Current lease
     */
    Lease currentWithShareLock();

    /**
     * Retrieve current lease with WRITE lock
     *
     * @return Current lease
     */
    Lease currentWithWriteLock();

    /**
     * Once {@link #currentWithWriteLock()} succeeds, inspect whether the lease is expired and update it to mark self
     * as leader.
     *
     * @param lease Lease of a new leader, whose epoch should be incremented by one and expiration_time renewed.
     * @return Number of rows affected, 1 if successful
     */
    int update(Lease lease);
}
