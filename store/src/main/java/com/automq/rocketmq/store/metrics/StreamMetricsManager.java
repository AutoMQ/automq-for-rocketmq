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

package com.automq.rocketmq.store.metrics;

import com.automq.rocketmq.common.MetricsManager;
import com.automq.stream.s3.metrics.Counter;
import com.automq.stream.s3.metrics.Gauge;
import com.automq.stream.s3.metrics.Histogram;
import com.automq.stream.s3.metrics.NoopS3StreamMetricsGroup;
import com.automq.stream.s3.metrics.S3StreamMetricsGroup;
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
import java.util.Map;
import java.util.function.Supplier;
import org.apache.commons.lang3.tuple.Pair;

public class StreamMetricsManager implements MetricsManager, S3StreamMetricsGroup {

    private static Supplier<AttributesBuilder> attributesBuilderSupplier;
    private static Meter meter;
    private static NoopS3StreamMetricsGroup noopS3StreamMetricsGroup = new NoopS3StreamMetricsGroup();

    @Override
    public void initAttributesBuilder(Supplier<AttributesBuilder> attributesBuilderSupplier) {
        StreamMetricsManager.attributesBuilderSupplier = attributesBuilderSupplier;
    }

    @Override
    public void initStaticMetrics(Meter meter) {
        StreamMetricsManager.meter = meter;
    }

    @Override
    public void initDynamicMetrics(Meter meter) {
        StreamMetricsManager.meter = meter;
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

    @Override
    public Counter newCounter(String name, Map<String, String> tags) {
        if (meter != null && attributesBuilderSupplier != null) {
            return new StreamMetricsCounter(name, tags, meter, attributesBuilderSupplier);
        }
        return noopS3StreamMetricsGroup.newCounter(name, tags);
    }

    @Override
    public Histogram newHistogram(String name, Map<String, String> tags) {
        if (meter != null && attributesBuilderSupplier != null) {
            return new StreamMetricsHistogram(name, tags, meter, attributesBuilderSupplier);
        }
        return noopS3StreamMetricsGroup.newHistogram(name, tags);
    }

    @Override
    public void newGauge(String name, Map<String, String> tags, Gauge gauge) {
        if (meter != null && attributesBuilderSupplier != null) {
            new StreamMetricsGauge(name, tags, meter, attributesBuilderSupplier, gauge);
            return;
        }
        noopS3StreamMetricsGroup.newGauge(name, tags, gauge);
    }
}
