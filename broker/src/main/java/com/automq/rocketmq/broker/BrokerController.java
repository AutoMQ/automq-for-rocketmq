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

import com.automq.rocketmq.common.api.DataStore;
import com.automq.rocketmq.common.config.BrokerConfig;
import com.automq.rocketmq.common.util.Lifecycle;
import com.automq.rocketmq.controller.server.ControllerServiceImpl;
import com.automq.rocketmq.controller.server.MetadataStoreBuilder;
import com.automq.rocketmq.controller.MetadataStore;
import com.automq.rocketmq.metadata.DefaultProxyMetadataService;
import com.automq.rocketmq.metadata.DefaultStoreMetadataService;
import com.automq.rocketmq.metadata.api.ProxyMetadataService;
import com.automq.rocketmq.metadata.api.StoreMetadataService;
import com.automq.rocketmq.metadata.s3.DefaultS3MetadataService;
import com.automq.rocketmq.metadata.api.S3MetadataService;
import com.automq.rocketmq.proxy.config.ProxyConfiguration;
import com.automq.rocketmq.proxy.grpc.GrpcProtocolServer;
import com.automq.rocketmq.proxy.processor.ExtendMessagingProcessor;
import com.automq.rocketmq.proxy.remoting.RemotingProtocolServer;
import com.automq.rocketmq.proxy.service.DeadLetterService;
import com.automq.rocketmq.proxy.service.DefaultServiceManager;
import com.automq.rocketmq.store.DataStoreFacade;
import com.automq.rocketmq.store.MessageStoreBuilder;
import com.automq.rocketmq.store.MessageStoreImpl;
import com.automq.rocketmq.store.api.MessageStore;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.thread.ThreadPoolMonitor;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.proxy.service.ServiceManager;

public class BrokerController implements Lifecycle {
    private final BrokerConfig brokerConfig;
    private final ServiceManager serviceManager;
    private final GrpcProtocolServer grpcServer;
    private final RemotingProtocolServer remotingServer;
    private final MetadataStore metadataStore;
    private final MessageStore messageStore;
    private final StoreMetadataService storeMetadataService;
    private final ProxyMetadataService proxyMetadataService;
    private final ExtendMessagingProcessor messagingProcessor;
    private final MetricsExporter metricsExporter;
    private final DeadLetterService dlqService;

    public BrokerController(BrokerConfig brokerConfig) throws Exception {
        this.brokerConfig = brokerConfig;

        // Init the proxy configuration.
        ProxyConfiguration.intConfig(brokerConfig.proxy());

        metadataStore = MetadataStoreBuilder.build(brokerConfig);

        proxyMetadataService = new DefaultProxyMetadataService(metadataStore);
        S3MetadataService s3MetadataService = new DefaultS3MetadataService(metadataStore.config(),
            metadataStore.sessionFactory(), metadataStore.asyncExecutor());
        storeMetadataService = new DefaultStoreMetadataService(metadataStore, s3MetadataService);

        dlqService = new DeadLetterService(brokerConfig, proxyMetadataService);

        MessageStoreImpl messageStore = MessageStoreBuilder.build(brokerConfig.store(), brokerConfig.s3Stream(), storeMetadataService, dlqService);
        this.messageStore = messageStore;

        DataStore dataStore = new DataStoreFacade(messageStore.getS3ObjectOperator(), messageStore.getTopicQueueManager());
        metadataStore.setDataStore(dataStore);


        serviceManager = new DefaultServiceManager(brokerConfig, proxyMetadataService, dlqService, messageStore);
        messagingProcessor = ExtendMessagingProcessor.createForS3RocketMQ(serviceManager);

        // TODO: Split controller to a separate port
        ControllerServiceImpl controllerService = MetadataStoreBuilder.build(metadataStore);
        grpcServer = new GrpcProtocolServer(brokerConfig.proxy(), messagingProcessor, controllerService);
        remotingServer = new RemotingProtocolServer(messagingProcessor);

        metricsExporter = new MetricsExporter(brokerConfig, messageStore, messagingProcessor);
    }

    @Override
    public void start() throws Exception {
        // Start the node registrar first, so that the node is registered before the proxy starts.
        metadataStore.start();

        messageStore.start();
        messagingProcessor.start();
        grpcServer.start();
        remotingServer.start();
        metadataStore.registerCurrentNode(brokerConfig.name(), brokerConfig.advertiseAddress(), brokerConfig.instanceId());
        metricsExporter.start();

        startThreadPoolMonitor();
    }

    @Override
    public void shutdown() throws Exception {
        grpcServer.shutdown();
        remotingServer.shutdown();
        messagingProcessor.shutdown();
        messageStore.shutdown();
        metadataStore.close();
        metricsExporter.shutdown();

        // Shutdown the thread pool monitor.
        ThreadPoolMonitor.shutdown();
    }

    private void startThreadPoolMonitor() {
        ThreadPoolMonitor.config(
            LoggerFactory.getLogger(LoggerName.PROXY_LOGGER_NAME),
            LoggerFactory.getLogger(LoggerName.PROXY_WATER_MARK_LOGGER_NAME),
            brokerConfig.proxy().enablePrintJstack(), brokerConfig.proxy().printJstackInMillis(),
            brokerConfig.proxy().printThreadPoolStatusInMillis());
        ThreadPoolMonitor.init();
    }
}
