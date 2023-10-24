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

import com.automq.stream.s3.metrics.Counter;
import io.opentelemetry.api.common.AttributesBuilder;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.Meter;
import java.util.Map;
import java.util.function.Supplier;

public class StreamMetricsCounter extends BaseStreamMetrics implements Counter {
    private final LongCounter counter;

    public StreamMetricsCounter(String type, String name, Map<String, String> tags,
        Meter meter, Supplier<AttributesBuilder> attributesBuilderSupplier) {
        super(type, name, tags, meter, attributesBuilderSupplier);
        this.counter = this.meter.counterBuilder(this.metricsName)
            .build();
    }

    @Override
    public void inc() {
        inc(1);
    }

    @Override
    public void inc(long n) {
        counter.add(n, newAttributesBuilder().build());
    }

    @Override
    public long count() {
        throw new UnsupportedOperationException();
    }
}
