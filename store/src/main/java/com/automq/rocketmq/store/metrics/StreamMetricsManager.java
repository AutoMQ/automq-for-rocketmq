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

package com.automq.rocketmq.store.metrics;

import com.automq.rocketmq.common.MetricsManager;
import com.automq.stream.s3.metrics.MetricsConfig;
import com.automq.stream.s3.metrics.MetricsLevel;
import com.automq.stream.s3.metrics.S3StreamMetricsManager;
import io.opentelemetry.api.common.AttributesBuilder;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.sdk.metrics.Aggregation;
import io.opentelemetry.sdk.metrics.InstrumentSelector;
import io.opentelemetry.sdk.metrics.InstrumentType;
import io.opentelemetry.sdk.metrics.View;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;
import org.apache.commons.lang3.tuple.Pair;

public class StreamMetricsManager implements MetricsManager {
    private static final String STREAM_METRICS_PREFIX = "rocketmq_stream_";
    @Override
    public void initAttributesBuilder(Supplier<AttributesBuilder> attributesBuilderSupplier) {
        S3StreamMetricsManager.configure(new MetricsConfig(MetricsLevel.INFO, attributesBuilderSupplier.get().build()));
    }

    @Override
    public void initStaticMetrics(Meter meter) {
        S3StreamMetricsManager.initMetrics(meter, STREAM_METRICS_PREFIX);
    }

    @Override
    public void initDynamicMetrics(Meter meter) {
    }

    public static List<Pair<InstrumentSelector, View>> getMetricsView() {
        ArrayList<Pair<InstrumentSelector, View>> metricsViewList = new ArrayList<>();

        List<Double> operationCostTimeBuckets = Arrays.asList(
            (double) Duration.ofNanos(100).toNanos(),
            (double) Duration.ofNanos(1000).toNanos(),
            (double) Duration.ofNanos(10_000).toNanos(),
            (double) Duration.ofNanos(100_000).toNanos(),
            (double) Duration.ofMillis(1).toNanos(),
            (double) Duration.ofMillis(2).toNanos(),
            (double) Duration.ofMillis(3).toNanos(),
            (double) Duration.ofMillis(5).toNanos(),
            (double) Duration.ofMillis(7).toNanos(),
            (double) Duration.ofMillis(10).toNanos(),
            (double) Duration.ofMillis(15).toNanos(),
            (double) Duration.ofMillis(30).toNanos(),
            (double) Duration.ofMillis(50).toNanos(),
            (double) Duration.ofMillis(100).toNanos(),
            (double) Duration.ofSeconds(1).toNanos(),
            (double) Duration.ofSeconds(2).toNanos(),
            (double) Duration.ofSeconds(3).toNanos()
        );
        InstrumentSelector selector = InstrumentSelector.builder()
            .setType(InstrumentType.HISTOGRAM)
            .setName(StoreMetricsConstant.HISTOGRAM_STREAM_OPERATION_TIME)
            .build();
        View view = View.builder()
            .setAggregation(Aggregation.explicitBucketHistogram(operationCostTimeBuckets))
            .build();
        metricsViewList.add(Pair.of(selector, view));

        return metricsViewList;
    }
}
