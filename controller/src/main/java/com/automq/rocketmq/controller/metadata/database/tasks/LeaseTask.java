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

package com.automq.rocketmq.controller.metadata.database.tasks;

import com.automq.rocketmq.controller.metadata.Role;
import com.automq.rocketmq.controller.metadata.database.DefaultMetadataStore;
import com.automq.rocketmq.controller.metadata.database.mapper.LeaseMapper;
import com.automq.rocketmq.controller.metadata.database.dao.Lease;
import java.util.Calendar;
import org.apache.ibatis.session.SqlSession;

public class LeaseTask extends ControllerTask {

    public LeaseTask(DefaultMetadataStore metadataStore) {
        super(metadataStore);
    }

    @Override
    public void run() {
        LOGGER.info("LeaseTask starts");
        try {
            try (SqlSession session = metadataStore.getSessionFactory().openSession(false)) {
                LeaseMapper leaseMapper = session.getMapper(LeaseMapper.class);
                Lease lease = leaseMapper.current();
                this.metadataStore.setLease(lease);
                if (!lease.expired()) {
                    if (lease.getNodeId() == metadataStore.getConfig().nodeId()) {
                        // Current node is lease leader.
                        Lease update = leaseMapper.currentWithWriteLock();
                        this.metadataStore.setLease(update);

                        if (update.getNodeId() != lease.getNodeId() || update.getEpoch() != lease.getEpoch()) {
                            session.rollback();
                            this.metadataStore.setLease(update);
                            // Someone must have won the leader election campaign.
                            return;
                        }
                        Calendar calendar = Calendar.getInstance();
                        calendar.add(Calendar.SECOND, metadataStore.getConfig().leaseLifeSpanInSecs());
                        update.setExpirationTime(calendar.getTime());
                        int affectedRows = leaseMapper.update(update);
                        if (1 == affectedRows) {
                            metadataStore.setRole(Role.Leader);
                        } else {
                            LOGGER.warn("Unexpected state, update lease affected {} rows", affectedRows);
                        }
                        session.commit();
                        metadataStore.setLease(update);
                    } else {
                        metadataStore.setRole(Role.Follower);
                        LOGGER.info("Node[id={}, epoch={}] is controller leader currently, expiring at {}",
                            lease.getNodeId(), lease.getEpoch(), lease.getExpirationTime());
                    }
                } else {
                    // Perform leader election campaign
                    Lease update = leaseMapper.currentWithWriteLock();
                    if (lease.getEpoch() == update.getEpoch() && lease.getNodeId() == update.getNodeId()) {
                        update.setEpoch(update.getEpoch() + 1);
                        update.setNodeId(metadataStore.getConfig().nodeId());
                        Calendar calendar = Calendar.getInstance();
                        calendar.add(Calendar.SECOND, metadataStore.getConfig().leaseLifeSpanInSecs());
                        update.setExpirationTime(calendar.getTime());
                        leaseMapper.update(update);
                        this.metadataStore.setLease(update);
                        session.commit();
                        metadataStore.setRole(Role.Leader);
                        LOGGER.info("Completes campaign and become controller leader");
                    } else {
                        LOGGER.info("Another controller has taken the lease");
                    }
                }
            }
        } catch (Throwable e) {
            LOGGER.error("Exception raised while running scheduled job", e);
        }
        LOGGER.info("LeaseTask completed");
    }
}
