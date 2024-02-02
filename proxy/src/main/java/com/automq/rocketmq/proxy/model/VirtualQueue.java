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

package com.automq.rocketmq.proxy.model;

import org.apache.rocketmq.proxy.service.route.AddressableMessageQueue;

/**
 * In RocketMQ, each MessageQueue should be bind to a fixed broker.
 * In current implementation, we assign each MessageQueue to a virtual broker for more flexible routing.
 */
public class VirtualQueue {
    public static final String VIRTUAL_QUEUE_SEPARATOR = "_";
    private final long topicId;
    private final int physicalQueueId;
    private final String brokerName;

    public VirtualQueue(long topicId, int physicalQueueId) {
        this.topicId = topicId;
        this.physicalQueueId = physicalQueueId;
        this.brokerName = buildBrokerName();
    }

    public VirtualQueue(AddressableMessageQueue messageQueue) {
        this(messageQueue.getBrokerName());
    }

    /**
     * The physical queue id is encoded in the broker name.
     *
     * @param brokerName broker name
     */
    public VirtualQueue(String brokerName) {
        this.brokerName = brokerName;
        String[] brokerNameParts = brokerName.split(VIRTUAL_QUEUE_SEPARATOR);
        this.topicId = Long.parseLong(brokerNameParts[0]);
        this.physicalQueueId = Integer.parseInt(brokerNameParts[1]);
    }

    private String buildBrokerName() {
        return topicId + VIRTUAL_QUEUE_SEPARATOR + physicalQueueId;
    }

    public long topicId() {
        return topicId;
    }

    public int physicalQueueId() {
        return physicalQueueId;
    }

    public String brokerName() {
        return brokerName;
    }
}
