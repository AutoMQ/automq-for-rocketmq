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

package com.automq.rocketmq.common;

import io.opentelemetry.api.common.AttributesBuilder;
import io.opentelemetry.api.metrics.Meter;
import java.util.function.Supplier;

public interface MetricsManager {
    void initAttributesBuilder(Supplier<AttributesBuilder> attributesBuilderSupplier);
    void initStaticMetrics(Meter meter);
    void initDynamicMetrics(Meter meter);
}
