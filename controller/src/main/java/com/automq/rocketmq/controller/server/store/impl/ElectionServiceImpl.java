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
