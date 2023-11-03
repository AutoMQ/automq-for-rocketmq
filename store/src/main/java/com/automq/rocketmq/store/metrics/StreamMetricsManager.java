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
import java.util.Map;
import java.util.function.Supplier;

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
