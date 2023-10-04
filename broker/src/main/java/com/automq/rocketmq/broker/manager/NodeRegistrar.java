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

package com.automq.rocketmq.broker.manager;

import com.automq.rocketmq.common.PrefixThreadFactory;
import com.automq.rocketmq.common.config.BrokerConfig;
import com.automq.rocketmq.common.util.Lifecycle;
import com.automq.rocketmq.controller.metadata.MetadataStore;
import com.automq.rocketmq.controller.metadata.database.dao.Node;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NodeRegistrar implements Lifecycle {
    private static final Logger LOGGER = LoggerFactory.getLogger(NodeRegistrar.class);
    private final MetadataStore metadataStore;
    private final BrokerConfig brokerConfig;
    private final ScheduledExecutorService nodeKeepAliveScheduler;
    private final String nodeAddress;
    private Node node;

    public NodeRegistrar(BrokerConfig brokerConfig, MetadataStore metadataStore) {
        this.brokerConfig = brokerConfig;
        this.metadataStore = metadataStore;
        this.nodeKeepAliveScheduler = new ScheduledThreadPoolExecutor(1, new PrefixThreadFactory("NodeKeepAliveScheduler"));

        // TODO: Support remoting address
        this.nodeAddress = brokerConfig.address() + ":" + brokerConfig.proxy().grpcServerPort();
    }

    @Override
    public void start() throws Exception {
        registerNode();

        this.nodeKeepAliveScheduler.scheduleAtFixedRate(() -> {
            try {
                metadataStore.keepAlive(node.getId(), node.getEpoch(), false);
            } catch (Exception e) {
                LOGGER.error("Failed to keep alive node", e);
            }
        }, 0, 5, java.util.concurrent.TimeUnit.SECONDS);
    }

    @Override
    public void shutdown() throws Exception {
        this.nodeKeepAliveScheduler.shutdown();
    }

    public long nodeEpoch() {
        return node.getEpoch();
    }

    public Node node() {
        return node;
    }

    public void registerNode() throws Exception {
        node = metadataStore.registerBrokerNode(brokerConfig.name(), nodeAddress, brokerConfig.instanceId()).get();
    }
}
