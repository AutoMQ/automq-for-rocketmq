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

import com.automq.rocketmq.common.config.BrokerConfig;
import com.automq.rocketmq.common.config.MetricsConfig;
import com.automq.rocketmq.common.util.Lifecycle;
import com.automq.rocketmq.controller.MetadataStore;
import com.automq.rocketmq.controller.server.TopicMetricsManager;
import com.automq.rocketmq.metadata.service.S3MetadataService;
import com.automq.rocketmq.proxy.metrics.ProxyMetricsManager;
import com.automq.rocketmq.proxy.processor.ExtendMessagingProcessor;
import com.automq.rocketmq.store.MessageStoreImpl;
import com.automq.rocketmq.store.metrics.StoreMetricsManager;
import com.automq.rocketmq.store.metrics.StreamMetricsManager;
import com.google.common.base.Splitter;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.AttributesBuilder;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.instrumentation.oshi.SystemMetrics;
import io.opentelemetry.instrumentation.runtimemetrics.java17.RuntimeMetrics;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.metrics.InstrumentSelector;
import io.opentelemetry.sdk.metrics.SdkMeterProviderBuilder;
import io.opentelemetry.sdk.metrics.View;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

import static com.automq.rocketmq.broker.MetricsConstant.AGGREGATION_CUMULATIVE;
import static com.automq.rocketmq.broker.MetricsConstant.AGGREGATION_DELTA;
import static com.automq.rocketmq.broker.MetricsConstant.LABEL_AGGREGATION;
import static com.automq.rocketmq.broker.MetricsConstant.LABEL_INSTANCE_ID;
import static com.automq.rocketmq.broker.MetricsConstant.LABEL_NODE_NAME;

public class MetricsExporter implements Lifecycle {
    private static final Logger LOGGER = LoggerFactory.getLogger(MetricsExporter.class);
    private static final Map<String, String> LABEL_MAP = new HashMap<>();
    private volatile boolean started = false;
    private final BrokerConfig brokerConfig;
    private final MetricsConfig metricsConfig;
    private final TelemetryExporter telemetryExporter;
    private Meter brokerMeter;
    private RuntimeMetrics runtimeMetrics;

    private final ProxyMetricsManager proxyMetricsManager;
    private final StoreMetricsManager storeMetricsManager;
    private final StreamMetricsManager streamMetricsManager;

    private final TopicMetricsManager topicMetricsManager;

    public static Supplier<AttributesBuilder> attributesBuilderSupplier = Attributes::builder;

    public MetricsExporter(BrokerConfig brokerConfig, TelemetryExporter telemetryExporter,
        MessageStoreImpl messageStore, ExtendMessagingProcessor messagingProcessor, MetadataStore metadataStore,
        S3MetadataService s3MetadataService) {
        this.brokerConfig = brokerConfig;
        this.metricsConfig = brokerConfig.metrics();
        this.telemetryExporter = telemetryExporter;
        this.proxyMetricsManager = new ProxyMetricsManager(messagingProcessor);
        this.storeMetricsManager = new StoreMetricsManager(metricsConfig, messageStore);
        this.streamMetricsManager = new StreamMetricsManager();
        this.topicMetricsManager = new TopicMetricsManager(metadataStore, s3MetadataService);
        init();
    }

    public static AttributesBuilder newAttributesBuilder() {
        AttributesBuilder attributesBuilder;
        if (attributesBuilderSupplier == null) {
            attributesBuilderSupplier = Attributes::builder;
        }
        attributesBuilder = attributesBuilderSupplier.get();
        LABEL_MAP.forEach(attributesBuilder::put);
        return attributesBuilder;
    }

    private void init() {
        if (!telemetryExporter.isMetricsEnabled()) {
            LOGGER.info("MetricsExporter is disabled");
            return;
        }

        String labels = metricsConfig.labels();
        if (StringUtils.isNotBlank(labels)) {
            List<String> kvPairs = Splitter.on(',').omitEmptyStrings().splitToList(labels);
            for (String item : kvPairs) {
                String[] split = item.split(":");
                if (split.length != 2) {
                    LOGGER.warn("metricsLabel is not valid: {}", labels);
                    continue;
                }
                LABEL_MAP.put(split[0], split[1]);
            }
        }
        if (metricsConfig.exportInDelta()) {
            LABEL_MAP.put(LABEL_AGGREGATION, AGGREGATION_DELTA);
        } else {
            LABEL_MAP.put(LABEL_AGGREGATION, AGGREGATION_CUMULATIVE);
        }

        LABEL_MAP.put(LABEL_NODE_NAME, brokerConfig.name());
        LABEL_MAP.put(LABEL_INSTANCE_ID, brokerConfig.instanceId());

        Optional<OpenTelemetrySdk> optional = telemetryExporter.openTelemetrySdk();
        if (optional.isEmpty()) {
            LOGGER.warn("OpenTelemetrySdk is not initialized");
            return;
        }

        OpenTelemetrySdk openTelemetrySdk = optional.get();
        brokerMeter = openTelemetrySdk.getMeter("automq-for-rocketmq");

        // JVM metrics
        if (metricsConfig.exportJVMMetrics()) {
            runtimeMetrics = RuntimeMetrics.builder(openTelemetrySdk)
                .enableAllFeatures()
                .enableExperimentalJmxTelemetry()
                .build();
        }

        // System metrics
        if (metricsConfig.exportSystemMetrics()) {
            SystemMetrics.registerObservers(openTelemetrySdk);
        }

        initAttributesBuilder();
        initStaticMetrics();
    }

    private void initAttributesBuilder() {
        streamMetricsManager.initAttributesBuilder(MetricsExporter::newAttributesBuilder);
        storeMetricsManager.initAttributesBuilder(MetricsExporter::newAttributesBuilder);
        proxyMetricsManager.initAttributesBuilder(MetricsExporter::newAttributesBuilder);
        topicMetricsManager.initAttributesBuilder(MetricsExporter::newAttributesBuilder);
    }

    private void initStaticMetrics() {
        streamMetricsManager.initStaticMetrics(brokerMeter);
        storeMetricsManager.initStaticMetrics(brokerMeter);
        proxyMetricsManager.initStaticMetrics(brokerMeter);
        topicMetricsManager.initStaticMetrics(brokerMeter);
    }

    private void initDynamicMetrics() {
        streamMetricsManager.initDynamicMetrics(brokerMeter);
        storeMetricsManager.initDynamicMetrics(brokerMeter);
        proxyMetricsManager.initDynamicMetrics(brokerMeter);
        topicMetricsManager.initDynamicMetrics(brokerMeter);
        storeMetricsManager.start();
    }

    @Override
    public void start() {
        if (telemetryExporter.isMetricsEnabled()) {
            initDynamicMetrics();
            this.started = true;
        }
    }

    protected static void registerMetricsView(SdkMeterProviderBuilder providerBuilder) {
        for (Pair<InstrumentSelector, View> selectorViewPair : ProxyMetricsManager.getMetricsView()) {
            providerBuilder.registerView(selectorViewPair.getLeft(), selectorViewPair.getRight());
        }

        for (Pair<InstrumentSelector, View> selectorViewPair : StoreMetricsManager.getMetricsView()) {
            providerBuilder.registerView(selectorViewPair.getLeft(), selectorViewPair.getRight());
        }

        for (Pair<InstrumentSelector, View> selectorViewPair : StreamMetricsManager.getMetricsView()) {
            providerBuilder.registerView(selectorViewPair.getLeft(), selectorViewPair.getRight());
        }
    }

    @Override
    public void shutdown() {
        if (!started) {
            return;
        }
        if (runtimeMetrics != null) {
            runtimeMetrics.close();
        }
        storeMetricsManager.shutdown();
    }
}

