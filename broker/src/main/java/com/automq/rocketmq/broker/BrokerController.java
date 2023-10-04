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

package com.automq.rocketmq.broker;

import com.automq.rocketmq.broker.manager.NodeRegistrar;
import com.automq.rocketmq.broker.protocol.GrpcProtocolServer;
import com.automq.rocketmq.common.config.BrokerConfig;
import com.automq.rocketmq.common.util.Lifecycle;
import com.automq.rocketmq.controller.MetadataStoreBuilder;
import com.automq.rocketmq.controller.metadata.MetadataStore;
import com.automq.rocketmq.controller.metadata.database.dao.Node;
import com.automq.rocketmq.metadata.DefaultProxyMetadataService;
import com.automq.rocketmq.metadata.DefaultStoreMetadataService;
import com.automq.rocketmq.metadata.api.ProxyMetadataService;
import com.automq.rocketmq.metadata.api.StoreMetadataService;
import com.automq.rocketmq.proxy.config.ProxyConfiguration;
import com.automq.rocketmq.proxy.processor.ExtendMessagingProcessor;
import com.automq.rocketmq.proxy.service.DefaultServiceManager;
import com.automq.rocketmq.store.MessageStoreBuilder;
import com.automq.rocketmq.store.api.MessageStore;
import com.google.gson.Gson;
import org.apache.rocketmq.proxy.processor.MessagingProcessor;
import org.apache.rocketmq.proxy.service.ServiceManager;

public class BrokerController implements Lifecycle {
    private final BrokerConfig brokerConfig;
    private final ServiceManager serviceManager;
    private final GrpcProtocolServer grpcServer;
    private final MetadataStore metadataStore;
    private final MessageStore messageStore;
    private final StoreMetadataService storeMetadataService;
    private final ProxyMetadataService proxyMetadataService;
    private final MessagingProcessor messagingProcessor;
    private final NodeRegistrar nodeRegistrar;

    public BrokerController(BrokerConfig brokerConfig) throws Exception {
        this.brokerConfig = brokerConfig;

        // Init the proxy configuration.
        ProxyConfiguration.intConfig(brokerConfig.proxy());

        Node fakeNode = new Node();
        fakeNode.setEpoch(0);
        fakeNode.setId(1);
        metadataStore = MetadataStoreBuilder.build(brokerConfig.controller(), fakeNode);
        nodeRegistrar = new NodeRegistrar(brokerConfig, metadataStore);
        // Start the node registrar first, so that the node is registered before the proxy starts.
        metadataStore.start();
        nodeRegistrar.registerNode();

        proxyMetadataService = new DefaultProxyMetadataService(metadataStore, nodeRegistrar.node());
        storeMetadataService = new DefaultStoreMetadataService(metadataStore, nodeRegistrar.node());

        messageStore = MessageStoreBuilder.build(brokerConfig.store(), brokerConfig.s3Stream(), storeMetadataService);

        serviceManager = new DefaultServiceManager(brokerConfig.proxy(), proxyMetadataService, messageStore);
        messagingProcessor = ExtendMessagingProcessor.createForS3RocketMQ(serviceManager);

        grpcServer = new GrpcProtocolServer(brokerConfig.proxy(), messagingProcessor);
    }

    @Override
    public void start() throws Exception {
        nodeRegistrar.start();
        messageStore.start();
        messagingProcessor.start();
        grpcServer.start();
    }

    @Override
    public void shutdown() throws Exception {
        grpcServer.shutdown();
        messagingProcessor.shutdown();
        messageStore.shutdown();
        metadataStore.close();
        nodeRegistrar.shutdown();
    }
}
