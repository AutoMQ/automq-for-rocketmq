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

import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.AttributesBuilder;
import io.opentelemetry.api.metrics.Meter;
import java.util.Map;
import java.util.function.Supplier;

public class BaseStreamMetrics {
    private static final String STREAM_METRICS_PREFIX = "rocketmq_stream_";

    protected final Meter meter;
    protected final Supplier<AttributesBuilder> attributesBuilderSupplier;
    protected final Map<String, String> tags;
    protected final String metricsName;

    protected BaseStreamMetrics(String name, Map<String, String> tags,
        Meter meter, Supplier<AttributesBuilder> attributesBuilderSupplier) {
        this.metricsName = metricsName(name);
        this.tags = tags;
        this.meter = meter;
        this.attributesBuilderSupplier = attributesBuilderSupplier;
    }


    protected AttributesBuilder newAttributesBuilder() {
        AttributesBuilder builder;
        if (attributesBuilderSupplier == null) {
            builder = Attributes.builder();
        } else {
            builder = attributesBuilderSupplier.get();
        }
        tags.forEach(builder::put);
        return builder;
    }

    protected String metricsName(String name) {
        name = name.toLowerCase();
        return STREAM_METRICS_PREFIX + "_" + name;
    }
}
