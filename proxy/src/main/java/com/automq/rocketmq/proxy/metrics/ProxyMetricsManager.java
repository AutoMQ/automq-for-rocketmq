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

package com.automq.rocketmq.proxy.metrics;

import com.google.common.collect.Lists;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.AttributesBuilder;
import io.opentelemetry.api.metrics.LongHistogram;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.sdk.metrics.Aggregation;
import io.opentelemetry.sdk.metrics.InstrumentSelector;
import io.opentelemetry.sdk.metrics.InstrumentType;
import io.opentelemetry.sdk.metrics.View;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;
import org.apache.rocketmq.common.Pair;
import org.apache.rocketmq.common.metrics.NopLongHistogram;

public class ProxyMetricsManager {
    public static final String LABEL_PROTOCOL_TYPE = "protocol_type";
    public static final String LABEL_ACTION = "action";
    public static final String LABEL_RESULT = "result";

    public static final String HISTOGRAM_RPC_LATENCY = "rocketmq_rpc_latency";
    private static LongHistogram rpcLatency = new NopLongHistogram();

    private static Supplier<AttributesBuilder> attributesBuilderSupplier;

    public static AttributesBuilder newAttributesBuilder() {
        if (attributesBuilderSupplier == null) {
            return Attributes.builder();
        }
        return attributesBuilderSupplier.get();
    }

    public static void initMetrics(Meter meter, Supplier<AttributesBuilder> attributesBuilderSupplier) {
        ProxyMetricsManager.attributesBuilderSupplier = attributesBuilderSupplier;
        rpcLatency = meter.histogramBuilder(HISTOGRAM_RPC_LATENCY)
            .setDescription("Rpc latency")
            .setUnit("milliseconds")
            .ofLongs()
            .build();
    }

    public static List<Pair<InstrumentSelector, View>> getMetricsView() {
        List<Double> rpcCostTimeBuckets = Arrays.asList(
            (double) Duration.ofMillis(1).toMillis(),
            (double) Duration.ofMillis(3).toMillis(),
            (double) Duration.ofMillis(5).toMillis(),
            (double) Duration.ofMillis(7).toMillis(),
            (double) Duration.ofMillis(10).toMillis(),
            (double) Duration.ofMillis(100).toMillis(),
            (double) Duration.ofSeconds(1).toMillis(),
            (double) Duration.ofSeconds(2).toMillis(),
            (double) Duration.ofSeconds(3).toMillis()
        );
        InstrumentSelector selector = InstrumentSelector.builder()
            .setType(InstrumentType.HISTOGRAM)
            .setName(HISTOGRAM_RPC_LATENCY)
            .build();
        View view = View.builder()
            .setAggregation(Aggregation.explicitBucketHistogram(rpcCostTimeBuckets))
            .build();
        return Lists.newArrayList(new Pair<>(selector, view));
    }

    public static void recordRpcLatency(String protocolType, String action, String result, long costTimeMillis) {
        AttributesBuilder attributesBuilder = newAttributesBuilder()
            .put(LABEL_PROTOCOL_TYPE, protocolType)
            .put(LABEL_ACTION, action)
            .put(LABEL_RESULT, result);
        rpcLatency.record(costTimeMillis, attributesBuilder.build());
    }
}
