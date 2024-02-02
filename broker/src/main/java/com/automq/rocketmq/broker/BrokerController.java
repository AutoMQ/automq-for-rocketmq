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

package com.automq.rocketmq.broker;

import com.automq.rocketmq.common.api.DataStore;
import com.automq.rocketmq.common.config.BrokerConfig;
import com.automq.rocketmq.common.config.ProfilerConfig;
import com.automq.rocketmq.common.util.Lifecycle;
import com.automq.rocketmq.controller.MetadataStore;
import com.automq.rocketmq.controller.server.ControllerServiceImpl;
import com.automq.rocketmq.controller.server.MetadataStoreBuilder;
import com.automq.rocketmq.metadata.DefaultProxyMetadataService;
import com.automq.rocketmq.metadata.DefaultStoreMetadataService;
import com.automq.rocketmq.metadata.api.ProxyMetadataService;
import com.automq.rocketmq.metadata.api.StoreMetadataService;
import com.automq.rocketmq.metadata.service.DefaultS3MetadataService;
import com.automq.rocketmq.metadata.service.S3MetadataService;
import com.automq.rocketmq.proxy.config.ProxyConfiguration;
import com.automq.rocketmq.proxy.grpc.GrpcProtocolServer;
import com.automq.rocketmq.proxy.grpc.ProxyServiceImpl;
import com.automq.rocketmq.proxy.grpc.client.GrpcProxyClient;
import com.automq.rocketmq.proxy.processor.ExtendMessagingProcessor;
import com.automq.rocketmq.proxy.remoting.RemotingProtocolServer;
import com.automq.rocketmq.proxy.service.DeadLetterService;
import com.automq.rocketmq.proxy.service.DefaultServiceManager;
import com.automq.rocketmq.proxy.service.ExtendMessageService;
import com.automq.rocketmq.proxy.service.LockService;
import com.automq.rocketmq.proxy.service.MessageServiceImpl;
import com.automq.rocketmq.proxy.service.SuspendRequestService;
import com.automq.rocketmq.store.DataStoreFacade;
import com.automq.rocketmq.store.MessageStoreBuilder;
import com.automq.rocketmq.store.MessageStoreImpl;
import com.automq.rocketmq.store.api.MessageStore;
import io.pyroscope.http.Format;
import io.pyroscope.javaagent.EventType;
import io.pyroscope.javaagent.PyroscopeAgent;
import io.pyroscope.javaagent.config.Config;
import io.pyroscope.labels.Pyroscope;
import java.util.Map;
import org.apache.rocketmq.broker.client.ConsumerManager;
import org.apache.rocketmq.broker.client.ProducerManager;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.thread.ThreadPoolMonitor;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.proxy.service.ServiceManager;
import org.apache.rocketmq.proxy.service.message.MessageService;

public class BrokerController implements Lifecycle {
    private static final Logger LOGGER = LoggerFactory.getLogger(MetricsExporter.class);

    private final BrokerConfig brokerConfig;
    private final ServiceManager serviceManager;
    private final GrpcProtocolServer grpcServer;
    private final RemotingProtocolServer remotingServer;
    private final MetadataStore metadataStore;
    private final MessageStore messageStore;
    private final StoreMetadataService storeMetadataService;
    private final ProxyMetadataService proxyMetadataService;
    private final ExtendMessagingProcessor messagingProcessor;
    private final TelemetryExporter telemetryExporter;
    private final MetricsExporter metricsExporter;
    private final DeadLetterService dlqService;
    private final MessageService messageService;
    private final ExtendMessageService extendMessageService;

    private final S3MetadataService s3MetadataService;

    public BrokerController(BrokerConfig brokerConfig) throws Exception {
        this.brokerConfig = brokerConfig;

        // Init the proxy configuration.
        ProxyConfiguration.intConfig(brokerConfig.proxy());

        metadataStore = MetadataStoreBuilder.build(brokerConfig);

        proxyMetadataService = new DefaultProxyMetadataService(metadataStore);
        s3MetadataService = new DefaultS3MetadataService(metadataStore.config(),
            metadataStore.sessionFactory(), metadataStore.asyncExecutor());
        storeMetadataService = new DefaultStoreMetadataService(metadataStore, s3MetadataService);

        GrpcProxyClient relayClient = new GrpcProxyClient(brokerConfig);
        dlqService = new DeadLetterService(brokerConfig, proxyMetadataService, relayClient);

        MessageStoreImpl messageStore = MessageStoreBuilder.build(brokerConfig.store(), brokerConfig.s3Stream(), storeMetadataService, dlqService);
        SuspendRequestService suspendRequestService = SuspendRequestService.getInstance();
        messageStore.registerMessageArriveListener((source, topic, queueId, offset, tag) -> suspendRequestService.notifyMessageArrival(topic.getName(), queueId, tag));
        this.messageStore = messageStore;

        DataStore dataStore = new DataStoreFacade(messageStore.streamStore(), messageStore.s3ObjectOperator(), messageStore.topicQueueManager());
        metadataStore.setDataStore(dataStore);
        dlqService.init(messageStore);

        LockService lockService = new LockService(brokerConfig.proxy());

        ProducerManager producerManager = new ProducerManager();
        MessageServiceImpl messageServiceImpl = new MessageServiceImpl(brokerConfig, messageStore, proxyMetadataService, lockService, dlqService, producerManager, relayClient);
        this.messageService = messageServiceImpl;
        this.extendMessageService = messageServiceImpl;
        ConsumerManager consumerManager = new ConsumerManager(new DefaultServiceManager.ConsumerIdsChangeListenerImpl(), brokerConfig.proxy().channelExpiredTimeout());
        serviceManager = new DefaultServiceManager(brokerConfig, proxyMetadataService, dlqService, messageService, messageStore, producerManager, consumerManager);

        messagingProcessor = ExtendMessagingProcessor.createForS3RocketMQ(serviceManager, brokerConfig.proxy());

        // Init the metrics exporter before accept requests.
        telemetryExporter = new TelemetryExporter(brokerConfig);
        metricsExporter = new MetricsExporter(brokerConfig, telemetryExporter, messageStore, messagingProcessor, metadataStore, s3MetadataService);

        // Init the profiler agent.
        ProfilerConfig profilerConfig = brokerConfig.profiler();
        if (profilerConfig.enabled()) {
            Pyroscope.setStaticLabels(Map.of("broker", brokerConfig.name()));
            PyroscopeAgent.start(
                new Config.Builder()
                    .setApplicationName("automq-for-rocketmq")
                    .setProfilingEvent(EventType.ITIMER)
                    .setFormat(Format.JFR)
                    .setServerAddress(profilerConfig.serverAddress())
                    .build()
            );
        }

        // TODO: Split controller to a separate port
        ControllerServiceImpl controllerService = MetadataStoreBuilder.build(metadataStore);
        ProxyServiceImpl proxyService = new ProxyServiceImpl(messageStore, extendMessageService, producerManager, consumerManager);
        grpcServer = new GrpcProtocolServer(brokerConfig.proxy(), messagingProcessor, controllerService, proxyService);
        remotingServer = new RemotingProtocolServer(messagingProcessor);
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
        s3MetadataService.start();

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
        telemetryExporter.close();

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
