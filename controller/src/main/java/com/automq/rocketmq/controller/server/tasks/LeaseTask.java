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

package com.automq.rocketmq.controller.server.tasks;

import apache.rocketmq.common.v1.Code;
import com.automq.rocketmq.common.exception.ControllerException;
import com.automq.rocketmq.controller.MetadataStore;
import com.automq.rocketmq.controller.server.store.BrokerNode;
import com.automq.rocketmq.metadata.dao.Lease;
import com.automq.rocketmq.metadata.dao.Node;
import com.automq.rocketmq.metadata.mapper.LeaseMapper;
import com.automq.rocketmq.metadata.mapper.NodeMapper;
import java.util.Calendar;
import org.apache.ibatis.session.SqlSession;

public class LeaseTask extends ControllerTask {

    /**
     * Current node representation in database
     */
    private Node node;

    public LeaseTask(MetadataStore metadataStore) {
        super(metadataStore);
    }

    @Override
    public void process() throws ControllerException {
        try (SqlSession session = metadataStore.openSession()) {
            tryCreateNode(session);

            LeaseMapper leaseMapper = session.getMapper(LeaseMapper.class);
            Lease lease = leaseMapper.current();
            this.metadataStore.electionService().updateLease(lease);
            if (!lease.expired()) {
                if (lease.getNodeId() == metadataStore.config().nodeId()) {
                    // Current node is lease leader.
                    Lease update = leaseMapper.currentWithWriteLock();
                    this.metadataStore.electionService().updateLease(update);

                    if (update.getNodeId() != lease.getNodeId() || update.getEpoch() != lease.getEpoch()) {
                        session.rollback();
                        this.metadataStore.electionService().updateLease(update);
                        // Someone must have won the leader election campaign.
                        return;
                    }

                    if (metadataStore.config().goingAway()) {
                        LOGGER.info("Current node is terminating thus should stop renewing its leadership lease");
                        return;
                    }

                    Calendar calendar = Calendar.getInstance();
                    calendar.add(Calendar.SECOND, metadataStore.config().leaseLifeSpanInSecs());
                    update.setExpirationTime(calendar.getTime());
                    int affectedRows = leaseMapper.update(update);
                    if (1 != affectedRows) {
                        LOGGER.warn("Unexpected state, update lease affected {} rows", affectedRows);
                    }
                    session.commit();
                    metadataStore.electionService().updateLease(update);
                } else {
                    LOGGER.debug("Node[id={}, epoch={}] is controller leader currently, expiring at {}",
                        lease.getNodeId(), lease.getEpoch(), lease.getExpirationTime());
                }
            } else {
                if (metadataStore.config().goingAway()) {
                    LOGGER.debug("Current node is terminating and will not campaign for leadership");
                    return;
                }

                // Perform leader election campaign
                Lease update = leaseMapper.currentWithWriteLock();
                if (lease.getEpoch() == update.getEpoch() && lease.getNodeId() == update.getNodeId()) {
                    update.setEpoch(update.getEpoch() + 1);
                    update.setNodeId(metadataStore.config().nodeId());
                    Calendar calendar = Calendar.getInstance();
                    calendar.add(Calendar.SECOND, metadataStore.config().leaseLifeSpanInSecs());
                    update.setExpirationTime(calendar.getTime());
                    leaseMapper.update(update);
                    session.commit();

                    this.metadataStore.electionService().updateLease(update);
                    // Add current node
                    assert null != this.node;
                    this.metadataStore.allNodes().put(node.getId(), new BrokerNode(node));
                    LOGGER.info("Node[node-id={}] completes campaign and becomes controller leader",
                        metadataStore.config().nodeId());
                } else {
                    LOGGER.info("An alternative controller has taken the lease");
                }
            }
        }
    }

    private void tryCreateNode(SqlSession session) throws ControllerException {
        if (null != node && metadataStore.config().nodeId() > 0) {
            return;
        }

        NodeMapper mapper = session.getMapper(NodeMapper.class);

        // Check if current node has already been created before.
        // Note this would make the mybatis cache friendly.
        Node node = mapper.get(null, metadataStore.config().name(), null, null);
        if (null != node) {
            this.node = node;
            // Update advertise address if changed.
            if (!metadataStore.config().advertiseAddress().equals(node.getAddress())) {
                node.setAddress(metadataStore.config().advertiseAddress());
                mapper.update(node);
                session.commit();
            }
            metadataStore.config().setNodeId(node.getId());
            return ;
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
            this.node = node;
        } catch (Exception e) {
            throw new ControllerException(Code.INTERNAL_VALUE, "Failed to register current node in database", e);
        }
    }
}
