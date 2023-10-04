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
