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

package com.automq.rocketmq.controller.metadata.database.mapper;

import com.automq.rocketmq.controller.metadata.database.dao.Lease;

public interface LeaseMapper {

    /**
     * Retrieve current lease
     *
     * @return Current lease
     */
    Lease current();


    /**
     * Retrieve current lease with WRITE locking
     *
     * @return Current lease
     */
    Lease currentWithShareLock();

    /**
     * Retrieve current lease with WRITE locking
     *
     * @return Current lease
     */
    Lease currentWithWriteLock();

    /**
     * Once {@link #currentWithWriteLock()} succeeds, inspect whether the lease is expired and update it to mark self
     * as leader.
     *
     * @param lease Lease of a new leader, whose term should be incremented by one and expiration_time renewed.
     * @return Number of rows affected, 1 if successful
     */
    int update(Lease lease);
}
