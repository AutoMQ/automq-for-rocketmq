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

package com.automq.rocketmq.controller.server.store.impl;

import com.automq.rocketmq.controller.MetadataStore;
import com.automq.rocketmq.controller.server.store.BrokerNode;
import com.automq.rocketmq.controller.server.store.ElectionService;
import com.automq.rocketmq.controller.server.tasks.LeaseTask;
import com.automq.rocketmq.metadata.dao.Lease;
import java.util.Date;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class ElectionServiceImpl implements ElectionService {

    private final MetadataStore metadataStore;

    private final ScheduledExecutorService executorService;

    /// The following fields are runtime specific
    private Lease lease;

    public ElectionServiceImpl(MetadataStore metadataStore, ScheduledExecutorService executorService) {
        this.metadataStore = metadataStore;
        this.executorService = executorService;
    }

    @Override
    public void updateLease(Lease lease) {
        this.lease = lease;
    }

    @Override
    public void start() {
        LeaseTask leaseTask = new LeaseTask(metadataStore);
        leaseTask.run();

        executorService.scheduleAtFixedRate(leaseTask, 1,
            metadataStore.config().leaseLifeSpanInSecs() / 2, TimeUnit.SECONDS);
    }

    @Override
    public Optional<Integer> leaderNodeId() {
        if (null != lease && !lease.expired()) {
            return Optional.of(lease.getNodeId());
        }
        return Optional.empty();
    }

    @Override
    public Optional<Integer> leaderEpoch() {
        if (null != lease && !lease.expired()) {
            return Optional.of(lease.getEpoch());
        }
        return Optional.empty();
    }

    @Override
    public Optional<String> leaderAddress() {
        if (null == lease || lease.expired()) {
            return Optional.empty();
        }
        Optional<BrokerNode> brokerNode = metadataStore.getNode(lease.getNodeId());
        return brokerNode.map(node -> node.getNode().getAddress());
    }

    @Override
    public Optional<Date> leaseExpirationTime() {
        if (null != lease && !lease.expired()) {
            return Optional.of(lease.getExpirationTime());
        }
        return Optional.empty();
    }
}
