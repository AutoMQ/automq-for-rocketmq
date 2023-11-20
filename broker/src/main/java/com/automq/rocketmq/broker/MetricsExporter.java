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
import com.automq.stream.s3.metrics.S3StreamMetricsRegistry;
import com.google.common.base.Splitter;
import io.opentelemetry.api.baggage.propagation.W3CBaggagePropagator;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.AttributesBuilder;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.propagation.ContextPropagators;
import io.opentelemetry.context.propagation.TextMapPropagator;
import io.opentelemetry.exporter.logging.LoggingMetricExporter;
import io.opentelemetry.exporter.otlp.metrics.OtlpGrpcMetricExporter;
import io.opentelemetry.exporter.otlp.metrics.OtlpGrpcMetricExporterBuilder;
import io.opentelemetry.exporter.prometheus.PrometheusHttpServer;
import io.opentelemetry.instrumentation.oshi.SystemMetrics;
import io.opentelemetry.instrumentation.runtimemetrics.java17.RuntimeMetrics;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.OpenTelemetrySdkBuilder;
import io.opentelemetry.sdk.metrics.InstrumentSelector;
import io.opentelemetry.sdk.metrics.InstrumentType;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.SdkMeterProviderBuilder;
import io.opentelemetry.sdk.metrics.View;
import io.opentelemetry.sdk.metrics.data.AggregationTemporality;
import io.opentelemetry.sdk.metrics.export.PeriodicMetricReader;
import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.rocketmq.common.metrics.MetricsExporterType;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.slf4j.bridge.SLF4JBridgeHandler;

import static com.automq.rocketmq.broker.MetricsConstant.AGGREGATION_CUMULATIVE;
import static com.automq.rocketmq.broker.MetricsConstant.AGGREGATION_DELTA;
import static com.automq.rocketmq.broker.MetricsConstant.LABEL_AGGREGATION;
import static com.automq.rocketmq.broker.MetricsConstant.LABEL_INSTANCE_ID;
import static com.automq.rocketmq.broker.MetricsConstant.LABEL_NODE_NAME;

public class MetricsExporter implements Lifecycle {
    private static final Logger LOGGER = LoggerFactory.getLogger(MetricsExporter.class);
    private volatile boolean started = false;
    private final BrokerConfig brokerConfig;
    private final MetricsConfig metricsConfig;
    private final static Map<String, String> LABEL_MAP = new HashMap<>();
    private OtlpGrpcMetricExporter metricExporter;
    private PeriodicMetricReader periodicMetricReader;
    private PrometheusHttpServer prometheusHttpServer;
    private LoggingMetricExporter loggingMetricExporter;
    private Meter brokerMeter;
    private OpenTelemetrySdk openTelemetrySdk;
    private RuntimeMetrics runtimeMetrics;

    private final ProxyMetricsManager proxyMetricsManager;
    private final StoreMetricsManager storeMetricsManager;
    private final StreamMetricsManager streamMetricsManager;

    private final TopicMetricsManager topicMetricsManager;

    public static Supplier<AttributesBuilder> attributesBuilderSupplier = Attributes::builder;

    public MetricsExporter(BrokerConfig brokerConfig, MessageStoreImpl messageStore,
        ExtendMessagingProcessor messagingProcessor, Resource resource, SdkTracerProvider tracerProvider,
        MetadataStore metadataStore, S3MetadataService s3MetadataService) {
        this.brokerConfig = brokerConfig;
        this.metricsConfig = brokerConfig.metrics();
        this.proxyMetricsManager = new ProxyMetricsManager(messagingProcessor);
        this.storeMetricsManager = new StoreMetricsManager(metricsConfig, messageStore);
        this.streamMetricsManager = new StreamMetricsManager();
        this.topicMetricsManager = new TopicMetricsManager(metadataStore, s3MetadataService);
        init(resource, tracerProvider);
        S3StreamMetricsRegistry.setMetricsGroup(this.streamMetricsManager);
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

    private boolean checkConfig() {
        if (metricsConfig == null) {
            return false;
        }
        MetricsExporterType exporterType = MetricsExporterType.valueOf(metricsConfig.exporterType());
        if (!exporterType.isEnable()) {
            return false;
        }

        return switch (exporterType) {
            case OTLP_GRPC -> StringUtils.isNotBlank(metricsConfig.grpcExporterTarget());
            case PROM, LOG -> true;
            default -> false;
        };
    }

    private void init(Resource resource, SdkTracerProvider tracerProvider) {
        MetricsExporterType metricsExporterType = MetricsExporterType.valueOf(metricsConfig.exporterType());
        if (metricsExporterType == MetricsExporterType.DISABLE) {
            return;
        }

        if (!checkConfig()) {
            LOGGER.error("check metrics config failed, will not export metrics");
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

        SdkMeterProviderBuilder providerBuilder = SdkMeterProvider.builder()
            .setResource(resource);

        if (metricsExporterType == MetricsExporterType.OTLP_GRPC) {
            String endpoint = metricsConfig.grpcExporterTarget();
            if (!endpoint.startsWith("http")) {
                endpoint = "https://" + endpoint;
            }
            OtlpGrpcMetricExporterBuilder metricExporterBuilder = OtlpGrpcMetricExporter.builder()
                .setEndpoint(endpoint)
                .setTimeout(metricsConfig.grpcExporterTimeOutInMills(), TimeUnit.MILLISECONDS)
                .setAggregationTemporalitySelector(type -> {
                    if (metricsConfig.exportInDelta() &&
                        (type == InstrumentType.COUNTER || type == InstrumentType.OBSERVABLE_COUNTER || type == InstrumentType.HISTOGRAM)) {
                        return AggregationTemporality.DELTA;
                    }
                    return AggregationTemporality.CUMULATIVE;
                });

            String headers = metricsConfig.grpcExporterHeader();
            if (StringUtils.isNotBlank(headers)) {
                Map<String, String> headerMap = new HashMap<>();
                List<String> kvPairs = Splitter.on(',').omitEmptyStrings().splitToList(headers);
                for (String item : kvPairs) {
                    String[] split = item.split(":");
                    if (split.length != 2) {
                        LOGGER.warn("metricsGrpcExporterHeader is not valid: {}", headers);
                        continue;
                    }
                    headerMap.put(split[0], split[1]);
                }
                headerMap.forEach(metricExporterBuilder::addHeader);
            }

            metricExporter = metricExporterBuilder.build();

            periodicMetricReader = PeriodicMetricReader.builder(metricExporter)
                .setInterval(metricsConfig.periodicExporterIntervalInMills(), TimeUnit.MILLISECONDS)
                .build();

            providerBuilder.registerMetricReader(periodicMetricReader);
        } else if (metricsExporterType == MetricsExporterType.PROM) {
            String promExporterHost = metricsConfig.promExporterHost();
            if (StringUtils.isBlank(promExporterHost)) {
                throw new IllegalArgumentException("Config item promExporterHost is blank");
            }
            prometheusHttpServer = PrometheusHttpServer.builder()
                .setHost(promExporterHost)
                .setPort(metricsConfig.promExporterPort())
                .build();
            providerBuilder.registerMetricReader(prometheusHttpServer);
        } else if (metricsExporterType == MetricsExporterType.LOG) {
            SLF4JBridgeHandler.removeHandlersForRootLogger();
            SLF4JBridgeHandler.install();
            loggingMetricExporter = LoggingMetricExporter.create(metricsConfig.exportInDelta() ? AggregationTemporality.DELTA : AggregationTemporality.CUMULATIVE);
            java.util.logging.Logger.getLogger(LoggingMetricExporter.class.getName()).setLevel(java.util.logging.Level.FINEST);
            periodicMetricReader = PeriodicMetricReader.builder(loggingMetricExporter)
                .setInterval(metricsConfig.periodicExporterIntervalInMills(), TimeUnit.MILLISECONDS)
                .build();
            providerBuilder.registerMetricReader(periodicMetricReader);
        }

        registerMetricsView(providerBuilder);

        OpenTelemetrySdkBuilder telemetrySdkBuilder = OpenTelemetrySdk.builder();

        if (tracerProvider != null) {
            telemetrySdkBuilder.setTracerProvider(tracerProvider);
        }

        openTelemetrySdk = telemetrySdkBuilder
            .setPropagators(ContextPropagators.create(TextMapPropagator.composite(W3CTraceContextPropagator.getInstance(), W3CBaggagePropagator.getInstance())))
            .setMeterProvider(providerBuilder.build())
            .buildAndRegisterGlobal();

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
        MetricsExporterType metricsExporterType = MetricsExporterType.valueOf(metricsConfig.exporterType());
        if (metricsExporterType != MetricsExporterType.DISABLE) {
            initDynamicMetrics();
        }
        this.started = true;
    }

    private void registerMetricsView(SdkMeterProviderBuilder providerBuilder) {
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
        MetricsExporterType exporterType = MetricsExporterType.valueOf(metricsConfig.exporterType());
        if (exporterType == MetricsExporterType.OTLP_GRPC) {
            periodicMetricReader.forceFlush();
            periodicMetricReader.shutdown();
            metricExporter.shutdown();
        } else if (exporterType == MetricsExporterType.PROM) {
            prometheusHttpServer.forceFlush();
            prometheusHttpServer.shutdown();
        } else if (exporterType == MetricsExporterType.LOG) {
            periodicMetricReader.forceFlush();
            periodicMetricReader.shutdown();
            loggingMetricExporter.shutdown();
        }
        storeMetricsManager.shutdown();
        runtimeMetrics.close();
        openTelemetrySdk.shutdown();
    }
}

