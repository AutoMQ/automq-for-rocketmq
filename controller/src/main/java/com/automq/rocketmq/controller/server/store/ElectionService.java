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

package com.automq.rocketmq.controller.server.store;

import com.automq.rocketmq.metadata.dao.Lease;
import java.util.Date;
import java.util.Optional;

public interface ElectionService {

    void start();

    Optional<Integer> leaderNodeId();

    Optional<Integer> leaderEpoch();

    Optional<String> leaderAddress();

    Optional<Date> leaseExpirationTime();

    void updateLease(Lease lease);
}
