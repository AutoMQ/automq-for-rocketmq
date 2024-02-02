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
import com.automq.rocketmq.common.config.TraceConfig;
import com.google.common.base.Splitter;
import io.opentelemetry.api.baggage.propagation.W3CBaggagePropagator;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.AttributesBuilder;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.propagation.ContextPropagators;
import io.opentelemetry.context.propagation.TextMapPropagator;
import io.opentelemetry.exporter.logging.LoggingMetricExporter;
import io.opentelemetry.exporter.otlp.metrics.OtlpGrpcMetricExporter;
import io.opentelemetry.exporter.otlp.metrics.OtlpGrpcMetricExporterBuilder;
import io.opentelemetry.exporter.otlp.trace.OtlpGrpcSpanExporter;
import io.opentelemetry.exporter.prometheus.PrometheusHttpServer;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.OpenTelemetrySdkBuilder;
import io.opentelemetry.sdk.metrics.InstrumentType;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.SdkMeterProviderBuilder;
import io.opentelemetry.sdk.metrics.data.AggregationTemporality;
import io.opentelemetry.sdk.metrics.export.PeriodicMetricReader;
import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.SpanProcessor;
import io.opentelemetry.sdk.trace.export.BatchSpanProcessor;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import java.io.Closeable;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.metrics.MetricsExporterType;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.slf4j.bridge.SLF4JBridgeHandler;

import static io.opentelemetry.semconv.ResourceAttributes.SERVICE_INSTANCE_ID;
import static io.opentelemetry.semconv.ResourceAttributes.SERVICE_NAME;
import static io.opentelemetry.semconv.ResourceAttributes.SERVICE_VERSION;

public class TelemetryExporter implements Closeable {
    private static final Logger LOGGER = LoggerFactory.getLogger(TelemetryExporter.class);

    private final BrokerConfig brokerConfig;
    private final MetricsConfig metricsConfig;
    private final TraceConfig traceConfig;

    private Resource resource;
    private OpenTelemetrySdk openTelemetrySdk;

    // Metrics
    MetricsExporterType metricsExporterType;

    private OtlpGrpcMetricExporter metricExporter;
    private PeriodicMetricReader periodicMetricReader;
    private PrometheusHttpServer prometheusHttpServer;
    private LoggingMetricExporter loggingMetricExporter;

    // Trace
    private SdkTracerProvider tracerProvider;

    public TelemetryExporter(BrokerConfig brokerConfig) {
        this.brokerConfig = brokerConfig;
        this.metricsConfig = brokerConfig.metrics();
        this.traceConfig = brokerConfig.trace();
        init();
    }

    private boolean isTelemetryEnabled() {
        try {
            metricsExporterType = MetricsExporterType.valueOf(metricsConfig.exporterType());
        } catch (Exception e) {
            LOGGER.warn("invalid metrics exporter type: {}", metricsConfig.exporterType());
            metricsExporterType = MetricsExporterType.DISABLE;
        }

        return metricsExporterType.isEnable() || traceConfig.enabled();
    }

    public boolean isMetricsEnabled() {
        return metricsExporterType.isEnable();
    }

    public boolean isTraceEnabled() {
        return traceConfig.enabled();
    }

    public Optional<OpenTelemetrySdk> openTelemetrySdk() {
        return Optional.ofNullable(openTelemetrySdk);
    }

    public void init() {
        if (!isTelemetryEnabled()) {
            return;
        }

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

        resource = Resource.create(builder.build());

        OpenTelemetrySdkBuilder telemetrySdkBuilder = OpenTelemetrySdk.builder();

        buildMeterProvider().ifPresent(telemetrySdkBuilder::setMeterProvider);
        buildTracerProvider().ifPresent(tracerProvider -> {
            telemetrySdkBuilder.setTracerProvider(tracerProvider);
            telemetrySdkBuilder.setPropagators(ContextPropagators.create(TextMapPropagator.composite(W3CTraceContextPropagator.getInstance(), W3CBaggagePropagator.getInstance())));
        });

        openTelemetrySdk = telemetrySdkBuilder.buildAndRegisterGlobal();
    }

    private boolean checkMetricsConfig() {
        return switch (metricsExporterType) {
            case OTLP_GRPC -> StringUtils.isNotBlank(metricsConfig.grpcExporterTarget());
            case PROM, LOG -> true;
            default -> false;
        };
    }

    private Optional<SdkMeterProvider> buildMeterProvider() {
        if (!checkMetricsConfig()) {
            LOGGER.error("check metrics config failed, will not export metrics");
            return Optional.empty();
        }

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

        MetricsExporter.registerMetricsView(providerBuilder);

        return Optional.of(providerBuilder.build());
    }

    private Optional<SdkTracerProvider> buildTracerProvider() {
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

            tracerProvider = SdkTracerProvider.builder()
                .addSpanProcessor(spanProcessor)
                .setResource(resource)
                .build();
            return Optional.of(tracerProvider);
        }
        return Optional.empty();
    }

    @Override
    public void close() {
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

        if (tracerProvider != null) {
            tracerProvider.shutdown();
        }

        if (openTelemetrySdk != null) {
            openTelemetrySdk.shutdown();
        }
    }
}
