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

package com.automq.rocketmq.controller.tasks;

import com.automq.rocketmq.controller.exception.ControllerException;
import com.automq.rocketmq.controller.metadata.MetadataStore;
import com.automq.rocketmq.controller.metadata.Role;
import com.automq.rocketmq.controller.metadata.database.dao.Node;
import com.automq.rocketmq.controller.metadata.database.mapper.LeaseMapper;
import com.automq.rocketmq.controller.metadata.database.dao.Lease;
import com.automq.rocketmq.controller.metadata.database.mapper.NodeMapper;
import java.util.Calendar;
import org.apache.ibatis.session.SqlSession;

public class LeaseTask extends ControllerTask {

    public LeaseTask(MetadataStore metadataStore) {
        super(metadataStore);
    }

    @Override
    public void process() throws ControllerException {
        try (SqlSession session = metadataStore.openSession()) {
            tryCreateNode(session);

            LeaseMapper leaseMapper = session.getMapper(LeaseMapper.class);
            Lease lease = leaseMapper.current();
            this.metadataStore.setLease(lease);
            if (!lease.expired()) {
                if (lease.getNodeId() == metadataStore.config().nodeId()) {
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
                    calendar.add(Calendar.SECOND, metadataStore.config().leaseLifeSpanInSecs());
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
                    update.setNodeId(metadataStore.config().nodeId());
                    Calendar calendar = Calendar.getInstance();
                    calendar.add(Calendar.SECOND, metadataStore.config().leaseLifeSpanInSecs());
                    update.setExpirationTime(calendar.getTime());
                    leaseMapper.update(update);
                    this.metadataStore.setLease(update);
                    session.commit();
                    metadataStore.setRole(Role.Leader);
                    LOGGER.info("Node[node-id={}] completes campaign and becomes controller leader",
                        metadataStore.config().nodeId());
                } else {
                    LOGGER.info("An alternative controller has taken the lease");
                }
            }
        }
    }

    private void tryCreateNode(SqlSession session) {
        if (metadataStore.config().nodeId() > 0) {
            return;
        }

        NodeMapper mapper = session.getMapper(NodeMapper.class);

        // Check if current node has already been created before.
        // Note this would make the mybatis cache friendly.
        Node node = mapper.get(null, metadataStore.config().name(), null, null);
        if (null != node) {
            // Update advertise address if changed.
            if (!metadataStore.config().advertiseAddress().equals(node.getAddress())) {
                node.setAddress(metadataStore.config().advertiseAddress());
                mapper.update(node);
                session.commit();
            }
            metadataStore.config().setNodeId(node.getId());
            return;
        }

        node = new Node();
        node.setName(metadataStore.config().name());
        node.setInstanceId(metadataStore.config().instanceId());
        node.setAddress(metadataStore.config().advertiseAddress());
        try {
            mapper.create(node);
            session.commit();
            LOGGER.info("Registered current node");
            metadataStore.config().setNodeId(node.getId());
        } catch (Exception ignore) {
        }
    }
}
