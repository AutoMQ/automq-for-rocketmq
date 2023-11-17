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
import com.automq.rocketmq.common.config.ProfilerConfig;
import com.automq.rocketmq.common.config.TraceConfig;
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
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.AttributesBuilder;
import io.opentelemetry.exporter.otlp.trace.OtlpGrpcSpanExporter;
import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.SpanProcessor;
import io.opentelemetry.sdk.trace.export.BatchSpanProcessor;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import io.pyroscope.http.Format;
import io.pyroscope.javaagent.EventType;
import io.pyroscope.javaagent.PyroscopeAgent;
import io.pyroscope.javaagent.config.Config;
import io.pyroscope.labels.Pyroscope;
import java.io.InputStream;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.thread.ThreadPoolMonitor;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.proxy.service.ServiceManager;
import org.apache.rocketmq.proxy.service.message.MessageService;

import static io.opentelemetry.semconv.ResourceAttributes.SERVICE_INSTANCE_ID;
import static io.opentelemetry.semconv.ResourceAttributes.SERVICE_NAME;
import static io.opentelemetry.semconv.ResourceAttributes.SERVICE_VERSION;

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

        dlqService = new DeadLetterService(brokerConfig, proxyMetadataService);

        MessageStoreImpl messageStore = MessageStoreBuilder.build(brokerConfig.store(), brokerConfig.s3Stream(), storeMetadataService, dlqService);
        SuspendRequestService suspendRequestService = SuspendRequestService.getInstance();
        messageStore.registerMessageArriveListener((source, topic, queueId, offset, tag) -> suspendRequestService.notifyMessageArrival(topic.getName(), queueId, tag));
        this.messageStore = messageStore;

        DataStore dataStore = new DataStoreFacade(messageStore.streamStore(), messageStore.s3ObjectOperator(), messageStore.topicQueueManager());
        metadataStore.setDataStore(dataStore);

        LockService lockService = new LockService(brokerConfig.proxy());
        MessageServiceImpl messageServiceImpl = new MessageServiceImpl(brokerConfig.proxy(), messageStore, proxyMetadataService, lockService, dlqService);
        this.messageService = messageServiceImpl;
        this.extendMessageService = messageServiceImpl;
        serviceManager = new DefaultServiceManager(brokerConfig, proxyMetadataService, dlqService, messageService, messageStore);

        messagingProcessor = ExtendMessagingProcessor.createForS3RocketMQ(serviceManager, brokerConfig.proxy());

        // Build resource.
        Properties gitProperties;
        try {
            ClassLoader classLoader = getClass().getClassLoader();
            InputStream inputStream = classLoader.getResourceAsStream("git.properties");
            gitProperties = new Properties();
            gitProperties.load(inputStream);
        } catch (Exception e) {
            LOGGER.warn("read project version failed", e);
            throw new RuntimeException(e);
        }

        AttributesBuilder builder = Attributes.builder();
        builder.put(SERVICE_NAME, brokerConfig.name());
        builder.put(SERVICE_VERSION, (String) gitProperties.get("git.build.version"));
        builder.put("git.hash", (String) gitProperties.get("git.commit.id.describe"));
        builder.put(SERVICE_INSTANCE_ID, brokerConfig.instanceId());

        Resource resource = Resource.create(builder.build());

        // Build trace provider.
        TraceConfig traceConfig = brokerConfig.trace();
        SdkTracerProvider sdkTracerProvider = null;
        if (traceConfig.enabled()) {
            OtlpGrpcSpanExporter spanExporter = OtlpGrpcSpanExporter.builder()
                .setEndpoint(traceConfig.grpcExporterTarget())
                .setTimeout(traceConfig.grpcExporterTimeOutInMills(), TimeUnit.MILLISECONDS)
                .build();

            SpanProcessor spanProcessor;
            if (traceConfig.batchSize() == 0) {
                spanProcessor = SimpleSpanProcessor.create(spanExporter);
            } else {
                spanProcessor = BatchSpanProcessor.builder(spanExporter)
                    .setExporterTimeout(traceConfig.grpcExporterTimeOutInMills(), TimeUnit.MILLISECONDS)
                    .setScheduleDelay(traceConfig.periodicExporterIntervalInMills(), TimeUnit.MILLISECONDS)
                    .setMaxExportBatchSize(traceConfig.batchSize())
                    .setMaxQueueSize(traceConfig.maxCachedSize())
                    .build();
            }

            sdkTracerProvider = SdkTracerProvider.builder()
                .addSpanProcessor(spanProcessor)
                .setResource(resource)
                .build();
        }

        // Init the metrics exporter before accept requests.
        metricsExporter = new MetricsExporter(brokerConfig, messageStore, messagingProcessor, resource,
            sdkTracerProvider, metadataStore, s3MetadataService);

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
        ProxyServiceImpl proxyService = new ProxyServiceImpl(extendMessageService);
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
